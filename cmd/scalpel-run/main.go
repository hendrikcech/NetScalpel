package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/google/uuid"

	"github.com/hendrikcech/netscalpel/internal/buildinfo"
	"github.com/hendrikcech/netscalpel/pkg"
)

var cli struct {
	Version     kong.VersionFlag `help:"Print running build version and provenance."`
	IP          string           `help:"Server IP." default:"0.0.0.0"`
	Port        uint             `help:"Server port." default:"8500"`
	Direction   string           `help:"Direction: UL or DL." default:"ul" enum:"dl,ul,DL,UL"`
	Log         string           `help:"Write debug log to this file."`
	LogLevel    int              `help:"Log level (-4: Debug, 0: Info, 4: Warn, 8: Error)" default:"0"`
	Out         string           `help:"CSV file to write results to (default: stdout)."`
	Info        string           `help:"Client-only UDP timestamp report path (no report unless supplied)."`
	TimestampTx string           `help:"Preferred local TX timestamp source: hardware (hw), software (sw), or app." default:"software"`
	TimestampRx string           `help:"Preferred local RX timestamp source: hardware (hw), software (sw), or app." default:"software"`

	UDPBurst struct {
		Num     uint `help:"Number of packets to send in burst." default:"10"`
		Pad     uint `help:"Number of bytes append to each packet." default:"0"`
		Timeout uint `help:"Server timeout in ms." default:"1000"`
	} `cmd:"" help:"Send a burst of UDP packets."`

	UDPRate struct {
		Rate     float64 `help:"Rate in Mbps." default:"1"`
		Duration uint    `help:"Duration in ms." default:"1000"`
	} `cmd:"" help:"Send UDP packets at a steady rate."`

	UDPPeriodic struct {
		Interval uint `help:"Time between each packet in ms." default:"100"`
		Duration uint `help:"Duration in ms." default:"1000"`
		Pad      uint `help:"Number of bytes append to each packet." default:"0"`
	} `cmd:"" help:"Send UDP packets at a constant interval."`

	TCP struct {
		Duration uint   `help:"Duration in ms." default:"4294967296"`
		Bytes    uint   `help:"Send a number of bytes (default: unlimited)." default:"0"`
		CCA      string `help:"Use a specific congestion controller algorithm." default:"cubic"`
	} `cmd:"" help:"Send TCP at a constant interval. Both duration and bytes can be specified."`

	ICMP struct {
		Interval uint `help:"Time between ICMP echo requests in ms." default:"100"`
		Duration uint `help:"Duration in ms." default:"1000"`
	} `cmd:"" help:"Send ICMP echo requests at a constant interval."`

	Server struct{} `cmd:"" help:"Run in server mode."`
}

func main() {
	kongctx := kong.Parse(&cli, kong.Vars{"version": buildinfo.String()})
	if kongctx.Error != nil {
		fmt.Fprintf(os.Stderr, "kong error: %v\n", kongctx.Error.Error())
		os.Exit(1)
	}
	if kongctx.Command() == "server" {
		if err := validateServerFlags(kongctx.Args); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid server flags: %v\n", err)
			os.Exit(1)
		}
	}
	infoPath := timestampInfoPath(kongctx.Command(), cli.Info)
	if isMeasurementCommand(kongctx.Command()) {
		if err := validateOutputPathCollisions(cli.Out, cli.Log, infoPath); err != nil {
			fmt.Fprintf(os.Stderr, "Invalid output paths: %v\n", err)
			os.Exit(1)
		}
	}
	config, configErr := timestampConfigFromCLI()
	if configErr != nil {
		fmt.Fprintf(os.Stderr, "Invalid timestamp config: %v\n", configErr)
		os.Exit(1)
	}
	pkg.RegisterGob()

	var slogFile *os.File
	if cli.Log != "" {
		var err error
		slogFile, err = os.Create(cli.Log)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed opening logfile %v: %v\n", cli.Log, err.Error())
			os.Exit(1)
		}
		defer slogFile.Close()
	}
	setupStandaloneLogging(slog.Level(cli.LogLevel), os.Stderr, slogFile)

	ctx, ctxCancel := context.WithCancel(context.Background())

	if kongctx.Command() == "server" {
		sigs := make(chan os.Signal, 2)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := pkg.RunServer(ctx, cli.IP, cli.Port, nil, config)

		sig := <-sigs
		fmt.Fprintf(os.Stderr, "Stopping server on %v\n", sig)
		ctxCancel()
		// A second signal force-exits, in case graceful shutdown hangs
		go func() {
			sig := <-sigs
			fmt.Fprintf(os.Stderr, "Forcing exit on second signal %v\n", sig)
			os.Exit(130)
		}()
		s.Stop()
		os.Exit(0)
	}

	if cli.IP == "0.0.0.0" {
		fmt.Fprintln(os.Stderr, "Argument --ip required")
		os.Exit(1)
	}

	direction, err := pkg.ParseDirection(cli.Direction)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	client := &pkg.SenderClient{
		IP:              cli.IP,
		Out:             cli.Out,
		Direction:       direction,
		ID:              uuid.New().String(),
		TimestampConfig: config,
	}

	switch kongctx.Command() {
	case "udp-burst":
		client.Sender = &pkg.BurstSender{Params: pkg.BurstParams{
			Timeout: time.Duration(cli.UDPBurst.Timeout) * time.Millisecond,
			Num:     cli.UDPBurst.Num,
			Pad:     cli.UDPBurst.Pad,
		}}
	case "udp-rate":
		client.Sender = &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
			Pps:         uint(cli.UDPRate.Rate * 1e6 / 8 / 1400),
			Interval:    time.Duration(1) * time.Millisecond,
			Duration:    time.Duration(cli.UDPRate.Duration) * time.Millisecond,
			PayloadSize: 1400,
		}}}
	case "udp-periodic":
		client.Sender = &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Duration(cli.UDPPeriodic.Interval) * time.Millisecond,
			Duration: time.Duration(cli.UDPPeriodic.Duration) * time.Millisecond,
			Pad:      cli.UDPPeriodic.Pad,
		}}
	case "tcp":
		cca, err := pkg.ParseTCPCCA(cli.TCP.CCA)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
		if cli.TCP.Duration == 4294967296 && cli.TCP.Bytes == 4294967296 {
			fmt.Fprintln(os.Stderr, "Either --duration or --bytes need to be set.")
			os.Exit(1)
		}
		client.Sender = &pkg.TCPSender{Params: pkg.TCPSenderParams{
			Duration_: time.Duration(cli.TCP.Duration) * time.Millisecond,
			Bytes:     cli.TCP.Bytes,
			CCA:       cca,
		}}
	case "icmp":
		client.Sender = &pkg.ICMPSender{Params: pkg.ICMPParams{
			Interval:  time.Duration(cli.ICMP.Interval) * time.Millisecond,
			Duration_: time.Duration(cli.ICMP.Duration) * time.Millisecond,
		}}
	default:
		panic(kongctx.Command())
	}
	rpcClient, err := dialRpcClient(cli.IP, cli.Port)
	if err != nil {
		runErr := fmt.Errorf("rpc.Dial failed: %w", err)
		if writeErr := writeTimestampInfo(infoPath, client, config, runErr); writeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("Info write error: %w", writeErr))
		}
		fmt.Fprintln(os.Stderr, runErr)
		os.Exit(1)
	}
	defer rpcClient.Close()
	runErr := runSenderAndGather(ctx, rpcClient, client)
	if err := writeTimestampInfo(infoPath, client, config, runErr); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("Info write error: %w", err))
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, client.Summary())
}

func setupStandaloneLogging(level slog.Level, console io.Writer, logFile *os.File) {
	if logFile != nil {
		pkg.SetupSlogMultiTo(level, false, console, logFile)
		return
	}
	pkg.SetupSlogBasicTo(level, console)
}

func isMeasurementCommand(command string) bool {
	switch command {
	case "udp-burst", "udp-rate", "udp-periodic", "tcp", "icmp":
		return true
	default:
		return false
	}
}

func isUDPMeasurementCommand(command string) bool {
	switch command {
	case "udp-burst", "udp-rate", "udp-periodic":
		return true
	default:
		return false
	}
}

func timestampInfoPath(command, info string) string {
	if !isUDPMeasurementCommand(command) {
		return ""
	}
	return info
}

func validateServerFlags(args []string) error {
	if flagWasSet(args, "info") {
		return fmt.Errorf("--info is client-only and cannot be used with the server command")
	}
	return nil
}

func timestampConfigFromCLI() (pkg.TimestampConfig, error) {
	return pkg.ParseTimestampConfig(cli.TimestampTx, cli.TimestampRx)
}

func flagWasSet(args []string, name string) bool {
	long := "--" + name
	for _, arg := range args {
		if arg == long || len(arg) > len(long) && arg[:len(long)+1] == long+"=" {
			return true
		}
	}
	return false
}

func dialRpcClient(ip string, port uint) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		return nil, err
	}
	return client, nil
}
