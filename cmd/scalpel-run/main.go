package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/google/uuid"

	"github.com/hendrikcech/netscalpel/pkg"
)

var cli struct {
	IP        string `help:"Server IP." default:"0.0.0.0"`
	Port      uint   `help:"Server port." default:"8500"`
	Direction string `help:"Direction: UL or DL." default:"ul" enum:"dl,ul,DL,UL"`
	Log       string `help:"Write debug log to this file."`
	LogLevel  int    `help:"Log level (-4: Debug, 0: Info, 4: Warn, 8: Error)" default:"0"`
	Out       string `help:"CSV file to write results to (default: stdout)."`

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
	kongctx := kong.Parse(&cli)
	if kongctx.Error != nil {
		fmt.Printf("kong error: %v\n", kongctx.Error.Error())
		os.Exit(1)
	}

	pkg.RegisterGob()

	if cli.Log != "" {
		slogFile, err := os.Create(cli.Log)
		if err != nil {
			fmt.Printf("Failed opening logfile %v: %v\n", cli.Log, err.Error())
			os.Exit(1)
		}
		defer slogFile.Close()
		pkg.SetupSlogMulti(slog.Level(cli.LogLevel), false, slogFile)
	} else {
		pkg.SetupSlogBasic(slog.Level(cli.LogLevel))
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	if kongctx.Command() == "server" {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := pkg.RunServer(ctx, cli.IP, cli.Port, nil)

		sig := <-sigs
		fmt.Printf("Stopping server on %v\n", sig)
		ctxCancel()
		s.Stop()
		os.Exit(0)
	}

	if cli.IP == "0.0.0.0" {
		fmt.Println("Argument --ip required")
		os.Exit(1)
	}

	direction, err := pkg.ParseDirection(cli.Direction)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	client := &pkg.SenderClient{
		IP:        cli.IP,
		Out:       cli.Out,
		Direction: direction,
		ID:        uuid.New().String(),
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
			fmt.Println(err.Error())
			os.Exit(1)
		}
		if cli.TCP.Duration == 4294967296 && cli.TCP.Bytes == 4294967296 {
			fmt.Println("Either --duration or --bytes need to be set.")
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

	rpcClient := dialRpcClient(cli.IP, cli.Port)
	defer rpcClient.Close()
	if err := client.Run(ctx, rpcClient); err != nil {
		fmt.Printf("Run error: %v\n", err)
		os.Exit(1)
	}
	if err := client.Gather(ctx, rpcClient); err != nil {
		fmt.Printf("Gather error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(client.Summary())
}

func dialRpcClient(ip string, port uint) *rpc.Client {
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		fmt.Printf("rpc.Dial failed: %v\n", err.Error())
		os.Exit(1)
	}
	return client
}
