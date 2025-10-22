package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

type Client struct {
	IP        string
	Port      uint
	Results   string
	Rounds    uint
	Procedure string
	Params    map[string]any

	// Permanent log file supplied by the caller. Needs to be closed by the caller.
	Logfile *os.File

	round uint

	// Per-round log file
	slogFile *os.File
}

func (c *Client) Run(ctx context.Context) {
	c.setupSlog(ctx, "")

	defer func() {
		// Make sure to close a potentially open log file (on error)
		if c.slogFile != nil {
			c.slogFile.Close()
		}
	}()

	paramsSet := []map[string]any{c.Params}
	var procFn ProcedureFunc
	if fn, ok := proceduresUlDl[c.Procedure]; ok {
		procFn = fn
		if _, ok := c.Params["direction"]; !ok {
			// Direction wasn't specified: execute procedure twice, once on UL once on DL
			paramsDL := maps.Clone(c.Params)
			paramsDL["direction"] = pkg.DL.String()
			paramsUL := maps.Clone(c.Params)
			paramsUL["direction"] = pkg.UL.String()
			paramsSet = []map[string]any{paramsDL, paramsUL}
		}
	} else if fn, ok := proceduresBidir[c.Procedure]; ok {
		procFn = fn
	} else {
		slog.ErrorContext(ctx, "Unknown -procedure", "procedure", c.Procedure)
		os.Exit(1)
	}

	for c.round = range c.Rounds {
		rpcClient, err := dialRpcClient(ctx, c.IP, c.Port)
		if err != nil {
			return
		}
		defer rpcClient.Close()
		if ctx.Err() != nil {
			return
		}

		for _, params := range paramsSet {
			e := NewExecutor(ctx, c.IP, rpcClient)
			resultPath := c.executeProcedure(ctx, e, time.Now(), procFn, params)
			c.runRound(ctx, e, rpcClient, resultPath)
		}

		rpcClient.Close()
	}
}

func (c *Client) runRound(ctx context.Context, e *Executor, rpcClient *rpc.Client, resultPath string) {
	c.setupSlog(ctx, resultPath)

	if err := e.G.Wait(); err != nil {
		slog.ErrorContext(ctx, fmt.Sprintf("[Round %v/%v] client.Run failed", c.round+1, c.Rounds), "error", err)
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

func (c *Client) executeProcedure(ctx context.Context, e *Executor, ts time.Time, fn ProcedureFunc, params ParamMap) string {
	ri := nextRi(ts)
	name := "_" + c.Procedure
	if direction, ok := params["direction"]; ok {
		name += "_" + strings.ToLower(direction.(string))
	}
	slog.InfoContext(ctx, fmt.Sprintf("[Round %v/%v] Schedule %s in %.2fs", c.round+1, c.Rounds,
		name, ri.Sub(time.Now()).Seconds()), "start", ri, "params", params)
	resultPath, err := mkResultPath(c.Results, ri, name)
	if err != nil {
		slog.ErrorContext(ctx, "mkResultPath", "error", err.Error())
		os.Exit(1)
	}
	if err := fn(e, ri, resultPath, params); err != nil {
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
		path := filepath.Join(resultPath, "stltrace_client.log")
		var err error
		c.slogFile, err = os.Create(path)
		if err != nil {
			slog.ErrorContext(ctx, "Failed opening slogfile", "path", path, "error", err)
		}
	}

	pkg.SetupSlogMulti(false, c.slogFile, c.Logfile)
}

func (c *Client) WriteServerLog(path string, rpcClient *rpc.Client) error {
	var result pkg.RequestSlogReply
	if err := rpcClient.Call("Server.RequestSlog", pkg.RequestSlogArgs{}, &result); err != nil {
		return fmt.Errorf("Call Server.RequestSlog failed: %v", err.Error())
	}

	if result.Log == "" {
		return nil
	}

	logPath := filepath.Join(path, "stltrace_server.log")
	if err := os.WriteFile(logPath, []byte(result.Log), 0644); err != nil {
		return fmt.Errorf("Failed writing to %v: %v", logPath, err.Error())
	}

	return nil
}

type ProcedureFunc func(*Executor, time.Time, string, ParamMap) error

func nextRi(ts time.Time) time.Time {
	base := ts.Round(time.Second)
	var riSec int
	for _, riSec = range []int{12, 27, 42, 57, 57 + 15} {
		if riSec > base.Second() {
			break
		}
	}
	minute := riSec / 60
	second := riSec % 60
	nextRi := time.Date(base.Year(), base.Month(), base.Day(), base.Hour(), base.Minute(), second, 0, base.Location())
	nextRi = nextRi.Add(time.Duration(minute) * time.Minute)
	return nextRi
}

func mkResultPath(base string, ts time.Time, suffix string) (string, error) {
	resultPath := filepath.Join(base, ts.Format("20060102T150405")+suffix)
	if err := os.MkdirAll(resultPath, os.ModePerm); err != nil {
		return "", fmt.Errorf("Failed to create the result directory %v: %v", resultPath, err)
	}
	return resultPath, nil
}

func dialRpcClient(ctx context.Context, ip string, port uint) (*rpc.Client, error) {
	clientC := make(chan *rpc.Client, 1)
	go func() {
		client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
		if err != nil {
			fmt.Printf("rpc.Dial failed: %v\n", err.Error())
			os.Exit(1)
		}
		clientC <- client
	}()

	select {
	case <-time.After(10 * time.Second):
		fmt.Printf("rpc.Dial timed out: %s:%v\n", ip, port)
		os.Exit(1)
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case client := <-clientC:
		return client, nil
	}
}
