package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strings"
	"syscall"

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
		fmt.Println("expected 'client', 'server', or 'procedures' subcommand")
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
