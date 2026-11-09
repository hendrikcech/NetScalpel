package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alecthomas/kong"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
	"github.com/hendrikcech/netscalpel/internal/buildinfo"
	"github.com/hendrikcech/netscalpel/pkg"
)

func main() {
	kongctx := kong.Parse(&cli, kong.Vars{"version": buildinfo.String()}, kong.UsageOnError())
	if kongctx.Command() == "client" {
		if err := validateClientFlags(kongctx.Args); err != nil {
			_ = kongctx.PrintUsage(false)
			kongctx.Fatalf("%v", err)
		}
	}
	var timestampConfig pkg.TimestampConfig
	if kongctx.Command() == "client" || kongctx.Command() == "server" {
		var err error
		timestampConfig, err = normalizedTimestampConfig()
		if err != nil {
			_ = kongctx.PrintUsage(false)
			kongctx.Fatalf("invalid timestamp config: %v", err)
		}
	}

	pkg.RegisterGob()

	pkg.SetupSlogBasic(slog.Level(cli.LogLevel))

	ctx, ctxCancel := context.WithCancel(context.Background())

	signalC := make(chan os.Signal, 2)
	signal.Notify(signalC, syscall.SIGINT, syscall.SIGTERM)

	// A second signal force-exits, in case graceful shutdown hangs
	forceExitOnSignal := func() {
		sig := <-signalC
		slog.Warn("Forcing exit on second signal", "signal", sig)
		os.Exit(130)
	}

	var logfile *os.File
	if cli.Log != "" {
		var err error
		if logfile, err = os.Create(cli.Log); err != nil {
			slog.ErrorContext(ctx, "Failed opening logfile", "path", cli.Log, "error", err)
			os.Exit(1)
		}
		defer logfile.Close()
	}

	switch kongctx.Command() {
	case "client":
		if flagWasSet(kongctx.Args, "batch") {
			client := &Client{
				IP: cli.IP, Port: cli.Port, Results: cli.Client.Results,
				StartMode: cli.Client.StartMode,
				LogLevel:  slog.Level(cli.LogLevel), Logfile: logfile,
				TimestampConfig: timestampConfig,
			}
			go func() {
				sig := <-signalC
				slog.InfoContext(ctx, "Cancelling ctx on signal", "signal", sig)
				ctxCancel()
				forceExitOnSignal()
			}()
			if err := RunBatchFile(ctx, cli.Client.Batch, cli.Client.Results, client); err != nil {
				slog.ErrorContext(ctx, "Batch failed", "error", err)
				os.Exit(1)
			}
			return
		}
		params, err := parseParams(cli.Client.Params)
		if err != nil {
			slog.ErrorContext(ctx, "Failed parsing params", "error", err)
			os.Exit(1)
		}

		go func() {
			sig := <-signalC
			slog.InfoContext(ctx, "Cancelling ctx on signal", "signal", sig)
			ctxCancel()
			forceExitOnSignal()
		}()

		client := Client{
			IP:              cli.IP,
			Port:            cli.Port,
			Results:         cli.Client.Results,
			Rounds:          cli.Client.Rounds,
			StartMode:       cli.Client.StartMode,
			Procedure:       cli.Client.Procedure,
			Params:          params,
			LogLevel:        slog.Level(cli.LogLevel),
			Logfile:         logfile,
			TimestampConfig: timestampConfig,
		}
		if err := client.Run(ctx); err != nil {
			slog.ErrorContext(ctx, "Experiment failed", "error", err)
			os.Exit(1)
		}

	case "server":
		slogCh := pkg.SetupSlogMulti(slog.Level(cli.LogLevel), true, logfile)

		s := pkg.RunServer(ctx, cli.IP, cli.Port, slogCh, timestampConfig)

		sig := <-signalC
		slog.InfoContext(ctx, "Stopping server", "signal", sig)
		ctxCancel()
		go forceExitOnSignal()
		s.Stop()

	case "procedures", "procedures <name>":
		// Runs without a server or a valid measurement IP: the output comes
		// from registration metadata only.
		printProcedures(cli.Procedures.Name)

	default:
		panic(kongctx.Command())
	}
}

func validateClientFlags(args []string) error {
	var missing []string
	if cli.IP == "" || cli.IP == "0.0.0.0" {
		missing = append(missing, "--ip")
	}
	batchSet := flagWasSet(args, "batch")
	if batchSet && flagWasSet(args, "rounds") {
		return fmt.Errorf("--batch and --rounds can't be used together")
	}
	if batchSet && cli.Client.Batch == "" {
		return fmt.Errorf("--batch requires a non-empty file path")
	}
	if !batchSet && cli.Client.Procedure == "" {
		missing = append(missing, "--procedure")
	}
	const hint = "Run 'scalpel-exp procedures' to list supported procedures."
	if len(missing) > 0 {
		return fmt.Errorf("missing required client flags: %s. %s", strings.Join(missing, ", "), hint)
	}
	if cli.Port == 0 {
		return fmt.Errorf("--port must be greater than zero")
	}
	if batchSet {
		return nil
	}
	if cli.Client.Rounds == 0 {
		return fmt.Errorf("--rounds must be greater than zero")
	}
	if _, ok := procedures.Lookup(cli.Client.Procedure); !ok {
		return fmt.Errorf("unknown procedure %q. %s", cli.Client.Procedure, hint)
	}
	return nil
}

func flagWasSet(args []string, name string) bool {
	long := "--" + name
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return false
}
