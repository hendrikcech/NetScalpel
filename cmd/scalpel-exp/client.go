package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
	"github.com/hendrikcech/netscalpel/internal/hostinfo"
	"github.com/hendrikcech/netscalpel/pkg"
)

type Client struct {
	IP        string
	Port      uint
	Results   string
	Rounds    uint
	Procedure string
	Params    experiment.ParamMap

	LogLevel slog.Level
	// Permanent log file supplied by the caller. Needs to be closed by the caller.
	Logfile *os.File

	round        uint
	experimentID string

	// Per-round log file
	slogFile *os.File
}

// prepareOverrides resolves the registered procedure and returns the
// parameter overrides of every invocation in execution order: a
// PerDirection procedure without a direction override runs once for DL and
// once for UL per round, every other procedure once per round.
//
// All invocation overrides are validated here, before the first RPC dial or
// result directory creation; Client.Run goes through this method, so direct
// Go callers cannot bypass the checks.
func (c *Client) prepareOverrides() (procedures.Procedure, []experiment.ParamMap, error) {
	proc, ok := procedures.Lookup(c.Procedure)
	if !ok {
		return procedures.Procedure{}, nil, fmt.Errorf("unknown procedure %q", c.Procedure)
	}

	paramsSet := []experiment.ParamMap{c.Params}
	if proc.Mode == procedures.PerDirection {
		if _, ok := c.Params["direction"]; !ok {
			// Direction wasn't specified: execute procedure twice, once on UL once on DL
			paramsSet = nil
			for _, direction := range []pkg.Direction{pkg.DL, pkg.UL} {
				params := maps.Clone(c.Params)
				if params == nil {
					params = make(experiment.ParamMap)
				}
				params["direction"] = direction.String()
				paramsSet = append(paramsSet, params)
			}
		}
	}

	for i, params := range paramsSet {
		if _, err := procedures.PrepareParams(proc, params); err != nil {
			return procedures.Procedure{}, nil, fmt.Errorf("invocation %v/%v: %w", i+1, len(paramsSet), err)
		}
	}
	return proc, paramsSet, nil
}

func (c *Client) Run(ctx context.Context) error {
	c.experimentID = pkg.GenID("experiment")
	if err := c.setupSlog(ctx, ""); err != nil {
		return err
	}

	defer func() {
		// Make sure to close a potentially open log file (on error)
		if c.slogFile != nil {
			c.slogFile.Close()
		}
	}()

	proc, paramsSet, err := c.prepareOverrides()
	if err != nil {
		slog.ErrorContext(ctx, "Procedure setup failed", "procedure", c.Procedure, "error", err)
		return err
	}

	for c.round = range c.Rounds {
		for _, overrides := range paramsSet {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			rpcClient, err := dialRpcClient(ctx, c.IP, c.Port)
			if err != nil {
				slog.ErrorContext(ctx, "Failed dialing RPC server", "error", err)
				return err
			}
			if ctx.Err() != nil {
				rpcClient.Close()
				return ctx.Err()
			}
			e := experiment.NewExecutor(ctx, c.IP, rpcClient)
			resultPath, scheduleErr := c.scheduleProcedureInvocation(ctx, e, time.Now(), proc, overrides)
			var finishErr error
			if resultPath != "" {
				finishErr = c.finishProcedureInvocation(ctx, e, e.RpcClient, resultPath)
			}
			e.RpcClient.Close()
			if err := errors.Join(scheduleErr, finishErr); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *Client) finishProcedureInvocation(ctx context.Context, e *experiment.Executor, rpcClient *rpc.Client, resultPath string) (resultErr error) {
	resultErr = c.setupSlog(ctx, resultPath)
	if e.Manifest == nil {
		e.Manifest = &experiment.Manifest{SchemaVersion: 1, Status: "running"}
	}
	if resultErr != nil {
		e.AddError("client", "log_setup", resultErr)
	}
	remoteAfterSafe := true
	defer func() {
		e.Finalize()
		if e.Manifest != nil {
			after := hostinfo.Load()
			e.Manifest.Client.After = &after
			if ctx.Err() == nil && remoteAfterSafe {
				if remote, err := pkg.FetchMetadata(ctx, rpcClient); err == nil {
					e.Manifest.Server.After = &remote.Load
				} else {
					e.Manifest.Warnings = append(e.Manifest.Warnings, "server AFTER metadata unavailable: "+err.Error())
				}
			} else {
				e.Manifest.Warnings = append(e.Manifest.Warnings, "server AFTER metadata skipped: cancellation or remote completion unconfirmed")
			}
		}
		if err := e.WriteInfo(resultPath); err != nil {
			slog.ErrorContext(ctx, "Failed writing final manifest", "error", err)
			e.AddError("client", "manifest_final", err)
			if e.Manifest.Status != "cancelled" {
				e.Manifest.Status = "failed"
			}
			resultErr = errors.Join(resultErr, fmt.Errorf("final manifest: %w", err))
		}
	}()

	if e.Manifest != nil && !e.Started {
		if len(e.Manifest.Errors) > 0 {
			resultErr = errors.Join(resultErr, errors.New("invocation setup failed"))
		}
		return
	}
	if err := e.G.Wait(); err != nil {
		resultErr = errors.Join(resultErr, err)
		e.AddError("client", "run", err)
		slog.ErrorContext(ctx, fmt.Sprintf("[Round %v/%v] client.Run failed", c.round+1, c.Rounds), "error", err)
	}

	// On user abort the results are incomplete; don't block on gathering.
	// Tell the server to cancel the still-running tests of this invocation so it
	// releases its sockets and goroutines right away.
	if ctx.Err() != nil {
		resultErr = errors.Join(resultErr, ctx.Err())
		slog.InfoContext(ctx, "Procedure invocation was cancelled, aborting server-side tests and skipping result gathering")
		if err := e.AbortServerTests(); err != nil {
			e.AddError("server", "abort", err)
			slog.WarnContext(ctx, "Failed aborting server-side tests", "error", err.Error())
		}
		return
	}

	slog.InfoContext(ctx, "Gathering results ...")
	start := time.Now()
	if err := e.GatherResults(); err != nil {
		resultErr = errors.Join(resultErr, err)
		remoteAfterSafe = false
		e.AddError("client", "gather", err)
		slog.ErrorContext(ctx, "Failed gathering results", "duration", time.Since(start).Seconds(), "error", err.Error())
	} else {
		slog.InfoContext(ctx, fmt.Sprintf("Gathered results in %.2fs", time.Since(start).Seconds()))
	}

	slog.DebugContext(ctx, "Fetching and writing server log ...")
	if err := c.writeServerLog(ctx, resultPath, rpcClient); err != nil {
		resultErr = errors.Join(resultErr, err)
		e.AddError("server", "server_log", err)
		slog.ErrorContext(ctx, "Failed fetching and writing server log", "error", err.Error())
	}
	return
}

func (c *Client) scheduleProcedureInvocation(ctx context.Context, e *experiment.Executor, ts time.Time, proc procedures.Procedure, overrides experiment.ParamMap) (string, error) {
	e.DeferStart = true
	local := pkg.LocalMetadata()
	remote, metadataErr := pkg.FetchMetadata(ctx, e.RpcClient)
	// A metadata timeout closes the channel to release blocked writes. Metadata
	// is optional: reconnect without retrying metadata before measurements start.
	if errors.Is(metadataErr, context.DeadlineExceeded) && ctx.Err() == nil {
		fresh, err := dialRpcClient(ctx, c.IP, c.Port)
		if err != nil {
			return "", fmt.Errorf("reconnect after metadata timeout: %w", err)
		}
		e.RpcClient = fresh
	}
	// Prepare per invocation so DL, UL, and later rounds never share
	// mutable parameter values. Errors were ruled out before dialing.
	params, err := procedures.PrepareParams(proc, overrides)
	if err != nil {
		slog.ErrorContext(ctx, "Failed preparing procedure parameters", "error", err)
		return "", err
	}

	// Optional metadata may take time (especially on an older peer). Do not
	// schedule against the now-stale timestamp supplied before collection.
	if now := time.Now(); now.After(ts) {
		ts = now
	}
	ri := experiment.NextRI(ts)
	name := "_" + c.Procedure
	if direction, ok := params["direction"]; ok {
		name += "_" + strings.ToLower(direction.(string))
	}
	slog.InfoContext(ctx, fmt.Sprintf("[Round %v/%v] Schedule %s in %.2fs", c.round+1, c.Rounds,
		name, time.Until(ri).Seconds()), "start", ri, "params", params)
	resultPath, err := mkResultPath(c.Results, ri, name)
	if err != nil {
		slog.ErrorContext(ctx, "mkResultPath", "error", err.Error())
		return "", err
	}
	if c.experimentID == "" {
		c.experimentID = pkg.GenID("experiment")
	}
	e.Manifest = &experiment.Manifest{
		SchemaVersion: 1, ExperimentID: c.experimentID, InvocationID: pkg.GenID("invocation"),
		Round: c.round + 1, Procedure: proc.Name, Parameters: experiment.ManifestParameters(params), ReferenceTime: ri,
		ScheduledStart: ri, Status: "scheduled",
		Client:   experiment.Endpoint{Role: "client", Metadata: &local},
		Server:   experiment.Endpoint{Role: "server", Metadata: remote},
		Warnings: []string{}, Errors: []experiment.ExecutionError{},
	}
	if metadataErr != nil {
		e.Manifest.Warnings = append(e.Manifest.Warnings, "server metadata unavailable (older server may not support it): "+metadataErr.Error())
	}
	if remote != nil && remote.Build.Version != local.Build.Version {
		e.Manifest.Warnings = append(e.Manifest.Warnings, "client/server version mismatch: "+local.Build.Version+" / "+remote.Build.Version)
	}
	if err := proc.Run(e, ri, resultPath, params); err != nil {
		e.AddError("client", "schedule", err)
		slog.Error("Procedure errored", "error", err)
		return resultPath, err
	}
	if err := e.WriteInfo(resultPath); err != nil {
		e.AddError("client", "manifest", err)
		slog.Error("Initial manifest failed; measurements not launched", "error", err)
		return resultPath, err
	}
	now := time.Now().UTC()
	e.Manifest.ExecutionStart = &now
	e.Manifest.Status = "running"
	e.Start()
	return resultPath, nil
}

func (c *Client) setupSlog(ctx context.Context, resultPath string) (resultErr error) {
	if c.slogFile != nil {
		c.slogFile.Close()
	}

	if resultPath != "" {
		path := filepath.Join(resultPath, "scalpel_client.log")
		var err error
		c.slogFile, err = os.Create(path)
		if err != nil {
			resultErr = err
			slog.ErrorContext(ctx, "Failed opening slogfile", "path", path, "error", err)
		}
	}

	pkg.SetupSlogMulti(c.LogLevel, false, c.slogFile, c.Logfile)
	return resultErr
}

func (c *Client) WriteServerLog(path string, rpcClient *rpc.Client) error {
	return c.writeServerLog(context.Background(), path, rpcClient)
}

func (c *Client) writeServerLog(ctx context.Context, path string, rpcClient *rpc.Client) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var result pkg.RequestSlogReply
	err := pkg.BoundedCall(ctx, rpcClient, "Server.RequestSlog", pkg.RequestSlogArgs{}, &result)
	if err != nil {
		return fmt.Errorf("Call Server.RequestSlog failed: %w", err)
	}

	if result.Log == "" {
		return nil
	}

	logPath := filepath.Join(path, "scalpel_server.log")
	if err := os.WriteFile(logPath, []byte(result.Log), 0644); err != nil {
		return fmt.Errorf("Failed writing to %v: %w", logPath, err)
	}

	return nil
}

func mkResultPath(base string, ts time.Time, suffix string) (string, error) {
	resultPath := filepath.Join(base, ts.Format("20060102T150405")+suffix)
	if err := os.MkdirAll(resultPath, os.ModePerm); err != nil {
		return "", fmt.Errorf("Failed to create the result directory %v: %w", resultPath, err)
	}
	return resultPath, nil
}

func dialRpcClient(ctx context.Context, ip string, port uint) (*rpc.Client, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		return nil, fmt.Errorf("rpc dial %s:%v failed: %w", ip, port, err)
	}
	return rpc.NewClient(conn), nil
}
