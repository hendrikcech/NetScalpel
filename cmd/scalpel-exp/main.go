package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alecthomas/kong"

	"github.com/hendrikcech/netscalpel/pkg"
)

func main() {
	kongctx := kong.Parse(&cli)
	if kongctx.Error != nil {
		fmt.Printf("kong error: %v\n", kongctx.Error.Error())
		os.Exit(1)
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
		if cli.IP == "0.0.0.0" || cli.IP == "" {
			fmt.Println("Expected server --ip")
			os.Exit(1)
		}

		if cli.Port == 0 {
			fmt.Println("expected --port")
			os.Exit(1)
		}

		if cli.Client.Rounds == 0 {
			cli.Client.Rounds = math.MaxUint
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
			IP:        cli.IP,
			Port:      cli.Port,
			Results:   cli.Client.Results,
			Rounds:    cli.Client.Rounds,
			Procedure: cli.Client.Procedure,
			Params:    params,
			LogLevel:  slog.Level(cli.LogLevel),
			Logfile:   logfile,
		}
		client.Run(ctx)

	case "server":
		slogCh := pkg.SetupSlogMulti(slog.Level(cli.LogLevel), true, logfile)

		s := pkg.RunServer(ctx, cli.IP, cli.Port, slogCh)

		sig := <-signalC
		slog.InfoContext(ctx, "Stopping server", "signal", sig)
		ctxCancel()
		go forceExitOnSignal()
		s.Stop()

	case "procedures":
		var b strings.Builder

		b.WriteString("UL/DL Procedures:\n")
		for k := range proceduresUlDl {
			b.WriteString("* ")
			b.WriteString(k)
			b.WriteString("\n")
		}

		b.WriteString("Bidir Procedures:\n")
		for k := range proceduresBidir {
			b.WriteString("* ")
			b.WriteString(k)
			b.WriteString("\n")
		}

		fmt.Print(b.String())

	default:
		panic(kongctx.Command())
	}
}
