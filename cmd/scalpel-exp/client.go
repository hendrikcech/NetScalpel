package main

import (
	"context"
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

	round uint

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

func (c *Client) Run(ctx context.Context) {
	c.setupSlog(ctx, "")

	defer func() {
		// Make sure to close a potentially open log file (on error)
		if c.slogFile != nil {
			c.slogFile.Close()
		}
	}()

	proc, paramsSet, err := c.prepareOverrides()
	if err != nil {
		slog.ErrorContext(ctx, "Procedure setup failed", "procedure", c.Procedure, "error", err)
		os.Exit(1)
	}

	for c.round = range c.Rounds {
		rpcClient, err := dialRpcClient(ctx, c.IP, c.Port)
		if err != nil {
			slog.ErrorContext(ctx, "Failed dialing RPC server", "error", err)
			return
		}
		if ctx.Err() != nil {
			rpcClient.Close()
			return
		}

		for _, overrides := range paramsSet {
			e := experiment.NewExecutor(ctx, c.IP, rpcClient)
			resultPath := c.scheduleProcedureInvocation(ctx, e, time.Now(), proc, overrides)
			c.finishProcedureInvocation(ctx, e, rpcClient, resultPath)
		}

		rpcClient.Close()
	}
}

func (c *Client) finishProcedureInvocation(ctx context.Context, e *experiment.Executor, rpcClient *rpc.Client, resultPath string) {
	c.setupSlog(ctx, resultPath)

	if err := e.G.Wait(); err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("[Round %v/%v] client.Run failed", c.round+1, c.Rounds), "error", err)
	}

	// On user abort the results are incomplete; don't block on gathering.
	// Tell the server to cancel the still-running tests of this invocation so it
	// releases its sockets and goroutines right away.
	if ctx.Err() != nil {
		slog.InfoContext(ctx, "Procedure invocation was cancelled, aborting server-side tests and skipping result gathering")
		if err := e.AbortServerTests(); err != nil {
			slog.WarnContext(ctx, "Failed aborting server-side tests", "error", err.Error())
		}
		return
	}

	slog.InfoContext(ctx, "Gathering results ...")
	start := time.Now()
	if err := e.GatherResults(); err != nil {
		slog.ErrorContext(ctx, "Failed gathering results", "duration", time.Since(start).Seconds(), "error", err.Error())
	} else {
		slog.InfoContext(ctx, fmt.Sprintf("Gathered results in %.2fs", time.Since(start).Seconds()))
	}

	// TODO: add information about pacing and timestamping support
	if err := e.WriteInfo(resultPath); err != nil {
		slog.ErrorContext(ctx, "Failed writing info", "error", err.Error())
	}

	slog.DebugContext(ctx, "Fetching and writing server log ...")
	if err := c.WriteServerLog(resultPath, rpcClient); err != nil {
		slog.ErrorContext(ctx, "Failed fetching and writing server log", "error", err.Error())
	}
}

func (c *Client) scheduleProcedureInvocation(ctx context.Context, e *experiment.Executor, ts time.Time, proc procedures.Procedure, overrides experiment.ParamMap) string {
	// Prepare per invocation so DL, UL, and later rounds never share
	// mutable parameter values. Errors were ruled out before dialing.
	params, err := procedures.PrepareParams(proc, overrides)
	if err != nil {
		slog.ErrorContext(ctx, "Failed preparing procedure parameters", "error", err)
		os.Exit(1)
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
		os.Exit(1)
	}
	if err := proc.Run(e, ri, resultPath, params); err != nil {
		slog.Error("Procedure errored", "error", err)
		os.Exit(1)
	}
	return resultPath
}

func (c *Client) setupSlog(ctx context.Context, resultPath string) {
	if c.slogFile != nil {
		c.slogFile.Close()
	}

	if resultPath != "" {
		path := filepath.Join(resultPath, "scalpel_client.log")
		var err error
		c.slogFile, err = os.Create(path)
		if err != nil {
			slog.ErrorContext(ctx, "Failed opening slogfile", "path", path, "error", err)
		}
	}

	pkg.SetupSlogMulti(c.LogLevel, false, c.slogFile, c.Logfile)
}

func (c *Client) WriteServerLog(path string, rpcClient *rpc.Client) error {
	var result pkg.RequestSlogReply
	if err := rpcClient.Call("Server.RequestSlog", pkg.RequestSlogArgs{}, &result); err != nil {
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
