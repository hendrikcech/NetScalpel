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

// StartMode selects the anchor a procedure schedule is built against.
type StartMode string

const (
	// StartModeStarlink aligns procedures to Starlink reconfiguration intervals.
	StartModeStarlink StartMode = "starlink"
	// StartModeImmediate keeps the schedule intact but starts it as soon as
	// setup allows, with no RI alignment and no exact start-time guarantee.
	StartModeImmediate StartMode = "immediate"
)

// ParseStartMode accepts either value case-insensitively and returns its
// canonical lower-case form.
func ParseStartMode(s string) (StartMode, error) {
	switch strings.ToLower(s) {
	case string(StartModeStarlink):
		return StartModeStarlink, nil
	case string(StartModeImmediate):
		return StartModeImmediate, nil
	default:
		return "", fmt.Errorf("unknown start mode %q (want starlink or immediate)", s)
	}
}

// UnmarshalText lets the CLI parse --start-mode case-insensitively.
func (m *StartMode) UnmarshalText(text []byte) error {
	mode, err := ParseStartMode(string(text))
	if err != nil {
		return err
	}
	*m = mode
	return nil
}

type Client struct {
	IP        string
	Port      uint
	Results   string
	Rounds    uint
	StartMode StartMode
	Procedure string
	Params    experiment.ParamMap

	TimestampConfig pkg.TimestampConfig

	LogLevel slog.Level
	// Permanent log file supplied by the caller. Needs to be closed by the caller.
	Logfile *os.File

	batchInvocation *BatchInvocationMetadata

	round        uint
	experimentID string

	// Per-round log file
	slogFile *os.File
}

func (c *Client) normalizeStartMode() error {
	if c.StartMode == "" {
		c.StartMode = StartModeStarlink
	}
	mode, err := ParseStartMode(string(c.StartMode))
	if err != nil {
		return err
	}
	c.StartMode = mode
	return nil
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
	if err := c.normalizeStartMode(); err != nil {
		return procedures.Procedure{}, nil, err
	}
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
	config, err := c.TimestampConfig.Normalize()
	if err != nil {
		return fmt.Errorf("invalid timestamp config: %w", err)
	}
	c.TimestampConfig = config
	if c.Rounds == 0 {
		return fmt.Errorf("rounds must be greater than zero")
	}
	proc, paramsSet, err := c.prepareOverrides()
	if err != nil {
		slog.ErrorContext(ctx, "Procedure setup failed", "procedure", c.Procedure, "error", err)
		return err
	}

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
			e.TimestampConfig = &c.TimestampConfig
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

func (c *Client) finishProcedureInvocation(ctx context.Context, e *experiment.Executor, rpcClient *rpc.Client, resultPath string) error {
	return c.finishProcedureInvocationClassified(ctx, e, rpcClient, resultPath).err
}

type invocationFinish struct {
	err             error
	fatal           bool
	failed          bool
	remoteConfirmed bool
	cancelled       bool
}

func (c *Client) finishProcedureInvocationClassified(ctx context.Context, e *experiment.Executor, rpcClient *rpc.Client, resultPath string) (out invocationFinish) {
	out.err = c.setupSlog(ctx, resultPath)
	if e.Manifest == nil {
		e.Manifest = &experiment.Manifest{SchemaVersion: 1, Status: "running"}
	}
	if out.err != nil {
		out.fatal = true
		out.failed = true
		e.AddError("client", "log_setup", out.err)
	}
	defer func() {
		e.Finalize()
		if e.Manifest != nil {
			after := hostinfo.Load()
			e.Manifest.Client.After = &after
			if ctx.Err() == nil && out.remoteConfirmed {
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
			out.fatal = true
			out.failed = true
			if e.Manifest.Status != "cancelled" {
				e.Manifest.Status = "failed"
			}
			out.err = errors.Join(out.err, fmt.Errorf("final manifest: %w", err))
		}
	}()

	if e.Manifest != nil && !e.Started {
		if len(e.Manifest.Errors) > 0 {
			out.err = errors.Join(out.err, errors.New("invocation setup failed"))
			out.failed = true
		}
		out.remoteConfirmed = true
		out.cancelled = ctx.Err() != nil
		return
	}
	waitErr := e.Wait()
	if waitErr != nil {
		out.err = errors.Join(out.err, waitErr)
		if hasIndependentInvocationError(waitErr, ctx.Err()) {
			out.failed = true
		}
		e.AddError("client", "run", waitErr)
		slog.ErrorContext(ctx, fmt.Sprintf("[Round %v/%v] client.Run failed", c.round+1, c.Rounds), "error", waitErr)
	}

	if ctx.Err() != nil {
		out.err = errors.Join(out.err, ctx.Err())
		out.cancelled = true
		slog.InfoContext(ctx, "Procedure invocation was cancelled, aborting server-side tests and skipping result gathering")
		if err := c.abortServerTests(e); err != nil {
			e.AddError("server", "abort", err)
			slog.WarnContext(ctx, "Failed aborting server-side tests", "error", err.Error())
		}
		return
	}
	if waitErr != nil {
		// Abort only requests cancellation. Gathering the terminal results below
		// confirms whether remote work actually stopped.
		if err := c.abortServerTests(e); err != nil {
			out.err = errors.Join(out.err, fmt.Errorf("abort server tests: %w", err))
			e.AddError("server", "abort", err)
		}
	}

	slog.InfoContext(ctx, "Gathering results ...")
	start := time.Now()
	gatherErr := e.GatherResults()
	if gatherErr != nil {
		out.err = errors.Join(out.err, gatherErr)
		if hasIndependentInvocationError(gatherErr, ctx.Err()) {
			out.failed = true
		}
		e.AddError("client", "gather", gatherErr)
		slog.ErrorContext(ctx, "Failed gathering results", "duration", time.Since(start).Seconds(), "error", gatherErr.Error())
	} else {
		slog.InfoContext(ctx, fmt.Sprintf("Gathered results in %.2fs", time.Since(start).Seconds()))
	}
	out.remoteConfirmed = e.ResultsConfirmed()
	if !out.remoteConfirmed {
		out.fatal = true
		if err := c.abortServerTests(e); err != nil {
			out.err = errors.Join(out.err, fmt.Errorf("abort unconfirmed server tests: %w", err))
			e.AddError("server", "abort", err)
		}
	}
	var persistenceErr *pkg.ResultPersistenceError
	if errors.As(gatherErr, &persistenceErr) {
		out.fatal = true
	}
	if ctx.Err() != nil {
		out.cancelled = true
		out.err = errors.Join(out.err, ctx.Err())
		if err := c.abortServerTests(e); err != nil {
			e.AddError("server", "abort", err)
			out.err = errors.Join(out.err, fmt.Errorf("abort server tests after cancellation: %w", err))
		}
		return
	}

	slog.DebugContext(ctx, "Fetching and writing server log ...")
	if err := c.writeServerLog(ctx, resultPath, rpcClient); err != nil {
		out.err = errors.Join(out.err, err)
		var persistenceErr *pkg.ResultPersistenceError
		if errors.As(err, &persistenceErr) {
			out.fatal = true
		}
		out.failed = out.failed || hasIndependentInvocationError(err, ctx.Err())
		out.cancelled = ctx.Err() != nil
		e.AddError("server", "server_log", err)
		slog.ErrorContext(ctx, "Failed fetching and writing server log", "error", err.Error())
	}
	return out
}

func hasIndependentInvocationError(err, cancellation error) bool {
	if err == nil {
		return false
	}
	if cancellation == nil {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range joined.Unwrap() {
			if hasIndependentInvocationError(cause, cancellation) {
				return true
			}
		}
		return false
	}
	if cause := errors.Unwrap(err); cause != nil {
		return hasIndependentInvocationError(cause, cancellation)
	}
	return !errors.Is(err, cancellation)
}

func (c *Client) abortServerTests(e *experiment.Executor) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	abortClient, err := dialRpcClient(ctx, c.IP, c.Port)
	if err != nil {
		return err
	}
	defer abortClient.Close()
	original := e.RpcClient
	e.RpcClient = abortClient
	err = e.AbortServerTests()
	e.RpcClient = original
	return err
}

func (c *Client) scheduleProcedureInvocation(ctx context.Context, e *experiment.Executor, ts time.Time, proc procedures.Procedure, overrides experiment.ParamMap) (resultPath string, resultErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			resultErr = &FatalInvocationError{Err: fmt.Errorf("procedure %s schedule panicked: %v", proc.Name, recovered)}
		}
	}()
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
	name := "_" + proc.Name
	if direction, ok := params["direction"]; ok {
		name += "_" + strings.ToLower(direction.(string))
	}
	resultTS := ri
	if c.StartMode == StartModeImmediate {
		// The RI anchor shapes the planned window; the schedule is rebased below.
		resultTS = ts
	} else {
		slog.InfoContext(ctx, fmt.Sprintf("[Round %v/%v] Schedule %s in %.2fs", c.round+1, c.Rounds,
			name, time.Until(ri).Seconds()), "start", ri, "params", params)
	}
	resultPath, err = mkResultPath(c.Results, resultTS, name)
	if err != nil {
		slog.ErrorContext(ctx, "mkResultPath", "error", err.Error())
		return "", &FatalInvocationError{Err: err}
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
	if c.batchInvocation != nil {
		e.Manifest.BatchID = c.batchInvocation.BatchID
		e.Manifest.BatchEntryID = c.batchInvocation.EntryID
		e.Manifest.BatchAncestry = append([]string(nil), c.batchInvocation.Ancestry...)
		e.Manifest.BatchPosition = c.batchInvocation.ExecutionPosition
		e.Manifest.BatchRound = c.batchInvocation.Round
		e.Manifest.BatchInvoke = c.batchInvocation.InvocationPosition
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
	if c.StartMode == StartModeImmediate {
		// A setup allowance, not a readiness guarantee: missed starts still fail.
		start := time.Now().Add(2 * time.Second)
		if err := e.Rebase(start); err != nil {
			e.AddError("client", "schedule", err)
			slog.Error("Failed rebasing immediate schedule", "error", err)
			return resultPath, err
		}
		e.Manifest.ReferenceTime = start
		slog.InfoContext(ctx, "Rebased immediate schedule", "start", start, "params", params)
	}
	if err := e.WriteInfo(resultPath); err != nil {
		e.AddError("client", "manifest", err)
		slog.Error("Initial manifest failed; measurements not launched", "error", err)
		return resultPath, &FatalInvocationError{Err: err}
	}
	now := time.Now().UTC()
	e.Manifest.ExecutionStart = &now
	e.Manifest.Status = "running"
	e.Start()
	return resultPath, nil
}

func (c *Client) setupSlog(ctx context.Context, resultPath string) (resultErr error) {
	if c.slogFile != nil {
		resultErr = errors.Join(resultErr, c.slogFile.Close())
		c.slogFile = nil
	}

	if resultPath != "" {
		path := filepath.Join(resultPath, "scalpel_client.log")
		var err error
		c.slogFile, err = os.Create(path)
		if err != nil {
			resultErr = errors.Join(resultErr, err)
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
		return &pkg.ResultPersistenceError{Err: fmt.Errorf("Failed writing to %v: %w", logPath, err)}
	}

	return nil
}

func mkResultPath(base string, ts time.Time, suffix string) (string, error) {
	if err := os.MkdirAll(base, os.ModePerm); err != nil {
		return "", fmt.Errorf("Failed to create the results directory %v: %w", base, err)
	}
	name := ts.Format("20060102T150405") + suffix
	for collision := uint(0); ; collision++ {
		candidate := name
		if collision > 0 {
			candidate += fmt.Sprintf("_%d", collision)
		}
		resultPath := filepath.Join(base, candidate)
		if err := os.Mkdir(resultPath, os.ModePerm); err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return "", fmt.Errorf("Failed to create the result directory %v: %w", resultPath, err)
		}
		return resultPath, nil
	}
}

func dialRpcClient(ctx context.Context, ip string, port uint) (*rpc.Client, error) {
	dialer := net.Dialer{Timeout: 10 * time.Second}
	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		return nil, fmt.Errorf("rpc dial %s:%v failed: %w", ip, port, err)
	}
	return rpc.NewClient(conn), nil
}
