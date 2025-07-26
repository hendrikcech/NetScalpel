package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

func main() {
	clientCmd := flag.NewFlagSet("client", flag.ExitOnError)
	clientIP := clientCmd.String("ip", "", "server ip")
	clientPort := clientCmd.Uint("port", 8500, "server port")
	clientResults := clientCmd.String("results", "results", "path to results folder")
	clientRounds := clientCmd.Uint("rounds", 1, "number of measurement rounds to run; 0 = infinite")
	clientProcedure := clientCmd.String("procedure", "", "test procedure")
	clientParams := clientCmd.String("params", "", "semicolon-separated key=value pairs passed to procedure")
	clientProfile := clientCmd.String("profile", "", "write pprof to file")
	clientLog := clientCmd.String("log", "", "write all log output to this file")

	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverIP := serverCmd.String("ip", "0.0.0.0", "ip")
	serverPort := serverCmd.Uint("port", 8500, "port")
	serverProfile := serverCmd.String("profile", "", "write pprof to file")
	serverLog := serverCmd.String("log", "", "write all log output to this file")

	if len(os.Args) < 2 {
		fmt.Println("expected 'client' or 'server' subcommands")
		os.Exit(1)
	}

	pkg.RegisterGob()

	pkg.SetupSlogBasic(slog.LevelInfo)

	ctx, ctxCancel := context.WithCancel(context.Background())

	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGINT, syscall.SIGTERM)

	go dumpOnSig()

	switch os.Args[1] {
	case "client":
		clientCmd.Parse(os.Args[2:])
		if *clientIP == "" {
			fmt.Println("expected -ip")
			os.Exit(1)
		}

		if *clientPort == 0 {
			fmt.Println("expected -port")
			os.Exit(1)
		}

		if *clientRounds == 0 {
			*clientRounds = math.MaxUint
		}

		createProfile(*clientProfile)
		defer pprof.StopCPUProfile()

		params, err := parseParams(*clientParams)
		if err != nil {
			slog.ErrorContext(ctx, "Failed parsing params", "error", err)
			os.Exit(1)
		}

		go func() {
			sig := <-signalC
			slog.InfoContext(ctx, "Cancelling ctx on signal", "signal", sig)
			ctxCancel()
		}()

		var logfile *os.File
		if *clientLog != "" {
			var err error
			if logfile, err = os.Create(*clientLog); err != nil {
				slog.ErrorContext(ctx, "Failed opening client logfile", "path", *clientLog, "error", err)
				os.Exit(1)
			}
			defer logfile.Close()
		}

		client := Client{
			IP:        *clientIP,
			Port:      *clientPort,
			Results:   *clientResults,
			Rounds:    *clientRounds,
			Procedure: *clientProcedure,
			Params:    params,
			Logfile:   logfile,
		}
		client.Run(ctx)

	case "server":
		serverCmd.Parse(os.Args[2:])

		var logfile *os.File
		if *serverLog != "" {
			var err error
			if logfile, err = os.Create(*serverLog); err != nil {
				slog.ErrorContext(ctx, "Failed opening server logfile", "path", *serverLog, "error", err)
				os.Exit(1)
			}
			defer logfile.Close()
		}
		slogCh := pkg.SetupSlogMulti(true, logfile)

		createProfile(*serverProfile)
		defer pprof.StopCPUProfile()

		s := pkg.RunServer(ctx, *serverIP, *serverPort, slogCh)

		sig := <-signalC
		slog.InfoContext(ctx, "Stopping server", "signal", sig)
		ctxCancel()
		s.Stop()

	default:
		fmt.Println("expected 'client' or 'server' subcommands")
		os.Exit(1)
	}
}

func createProfile(profile string) {
	if profile == "" {
		return
	}
	f, err := os.Create(profile)
	if err != nil {
		slog.Error("Create profile", "error", err)
		os.Exit(1)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		slog.Error("StartCPUProfile", "error", err.Error())
		os.Exit(1)
	}
	slog.Info("Writing pprof profile", "path", profile)
}

type ProcedureFunc func(time.Time, string, ParamMap) error

type ParamMap map[string]any

func (p ParamMap) Direction() (pkg.Direction, error) {
	value, ok := p["direction"]
	if !ok {
		return 999, fmt.Errorf("Direction parameter not present")
	}
	directionStr, ok := value.(string)
	if !ok {
		return 999, fmt.Errorf("Direction parameter must be string")
	}
	direction, err := pkg.ParseDirection(directionStr)
	if err != nil {
		return 999, err
	}
	return direction, nil
}

func (p ParamMap) Uints(key string) ([]uint, error) {
	value, ok := p[key]
	if !ok {
		return nil, fmt.Errorf("Parameter '%v' not present", key)
	}
	listStr, ok := value.([]string)
	if !ok {
		// Only a single element
		listStr = []string{value.(string)}
	}
	list := make([]uint, len(listStr))
	for i := range listStr {
		var err error
		parsed, err := strconv.ParseUint(listStr[i], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Parameter %v: failed parsing '%s' as uint", key, listStr[i])
		}
		list[i] = uint(parsed)
	}
	return list, nil
}

func (p ParamMap) Uint(key string) (uint, error) {
	value, ok := p[key]
	if !ok {
		return 0, fmt.Errorf("Parameter '%v' not present", key)
	}
	parsed, err := strconv.ParseUint(value.(string), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("Parameter '%v': failed parsing '%s' as uint", key, value)
	}
	return uint(parsed), nil
}

// Parses semicolon-separated key=value pairs
// If value contains a comma, the value is parsed as a list
// Example:
// direction=ul;durations=100,200
func parseParams(paramStr string) (ParamMap, error) {
	params := make(map[string]any)
	if paramStr == "" {
		return params, nil
	}
	for _, kv := range strings.Split(paramStr, ";") {
		parts := strings.Split(kv, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("Invalid key-value pair: %v", kv)
		}
		key := parts[0]
		value := parts[1]
		valueParts := strings.Split(value, ",")
		if len(valueParts) == 1 {
			params[key] = value
		} else {
			params[key] = valueParts
		}
	}
	return params, nil
}

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

	for c.round = range c.Rounds {
		rpcClient, err := dialRpcClient(ctx, c.IP, c.Port)
		if err != nil {
			return
		}
		defer rpcClient.Close()
		if ctx.Err() != nil {
			return
		}
		c.runRound(ctx, rpcClient)
		rpcClient.Close()
	}
}

func (c *Client) runRound(ctx context.Context, rpcClient *rpc.Client) {
	e := NewExecutor(ctx, c.IP, rpcClient)

	// Called once with direction DL and once with direction UL per round
	// (if param direction is not specified)
	proceduresUlDl := map[string]ProcedureFunc{
		"burst":            e.BurstRi,
		"prograte":         e.ProgressiveRate,
		"cddf":             e.CoolDownDifferentFlow,
		"cdsf":             e.CoolDownSameFlow,
		"multiflow":        e.MultiFlow,
		"mouseeleph":       e.MouseElephantFlows,
		"progdurmultirate": e.ProgressiveDurationMultiRate,
		"simplequic":       e.QUIC,
		"progdurquic":      e.ProgressiveDurationQUIC,
	}

	// Only called once per round
	proceduresBidir := map[string]ProcedureFunc{
		"trace": e.TraceRi,
		"owd":   e.MeasOWD,
	}

	now := time.Now()
	resultPath := ""
	if fn, ok := proceduresUlDl[c.Procedure]; ok {
		if _, ok := c.Params["direction"]; ok {
			resultPath = c.executeProcedure(ctx, now, fn, c.Params)
		} else {
			params := maps.Clone(c.Params)
			params["direction"] = pkg.DL.String()
			resultPath = c.executeProcedure(ctx, now, fn, params)
			params["direction"] = pkg.UL.String()
			resultPath = c.executeProcedure(ctx, now.Add(15*time.Second), fn, params)
		}
	} else if fn, ok := proceduresBidir[c.Procedure]; ok {
		resultPath = c.executeProcedure(ctx, now, fn, c.Params)
	} else {
		slog.ErrorContext(ctx, "Unknown -procedure", "procedure", c.Procedure)
		os.Exit(1)
	}

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

func (c *Client) executeProcedure(ctx context.Context, ts time.Time, fn ProcedureFunc, params ParamMap) string {
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
	if err := fn(ri, resultPath, params); err != nil {
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

func dumpOnSig() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	buf := make([]byte, 1<<20)
	for {
		<-sigs
		stacklen := runtime.Stack(buf, true)
		fmt.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
	}
}
