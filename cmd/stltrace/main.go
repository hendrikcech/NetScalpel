package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"

	"github.com/alecthomas/kong"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

func main() {
	kongctx := kong.Parse(&cli)
	if kongctx.Error != nil {
		fmt.Printf("kong error: %v\n", kongctx.Error.Error())
		os.Exit(1)
	}

	pkg.RegisterGob()

	pkg.SetupSlogBasic(slog.LevelInfo)

	ctx, ctxCancel := context.WithCancel(context.Background())

	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGINT, syscall.SIGTERM)

	// go dumpOnSig()

	var logfile *os.File
	if cli.Log != "" {
		var err error
		if logfile, err = os.Create(cli.Log); err != nil {
			slog.ErrorContext(ctx, "Failed opening logfile", "path", cli.Log, "error", err)
			os.Exit(1)
		}
		defer logfile.Close()
	}

	// createProfile(cli.Profile)
	// defer pprof.StopCPUProfile()

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
		}()

		client := Client{
			IP:        cli.IP,
			Port:      cli.Port,
			Results:   cli.Client.Results,
			Rounds:    cli.Client.Rounds,
			Procedure: cli.Client.Procedure,
			Params:    params,
			Logfile:   logfile,
		}
		client.Run(ctx)

	case "server":
		slogCh := pkg.SetupSlogMulti(true, logfile)

		s := pkg.RunServer(ctx, cli.IP, cli.Port, slogCh)

		sig := <-signalC
		slog.InfoContext(ctx, "Stopping server", "signal", sig)
		ctxCancel()
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
