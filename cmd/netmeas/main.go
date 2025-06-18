package main

import (
	"context"
	"flag"
	"fmt"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

func main() {
	burstCmd := flag.NewFlagSet("burst", flag.ExitOnError)
	burstIP := burstCmd.String("ip", "", "ip")
	burstPort := burstCmd.Uint("port", 8500, "port")
	burstNum := burstCmd.Uint("num", 10, "num")
	burstPad := burstCmd.Uint("pad", 0, "pad")
	burstTimeout := burstCmd.Uint("timeout", 1000, "server timeout in milliseconds")
	burstOut := burstCmd.String("o", "", "write csv to logfile (default stdout)")
	burstDirection := burstCmd.String("direction", "ul", "Send direction: 'ul' (client to server)")

	rateCmd := flag.NewFlagSet("rate", flag.ExitOnError)
	rateIP := rateCmd.String("ip", "", "ip")
	ratePort := rateCmd.Uint("port", 8500, "port")
	rateRate := rateCmd.Float64("rate", 0, "rate in Mbps")
	rateDuration := rateCmd.Float64("duration", 5, "duration in seconds")
	rateOut := rateCmd.String("o", "", "write csv to logfile (default stdout)")
	rateDirection := rateCmd.String("direction", "ul", "Send direction: 'ul' (client to server)")

	periodicCmd := flag.NewFlagSet("periodic", flag.ExitOnError)
	periodicIP := periodicCmd.String("ip", "", "ip")
	periodicPort := periodicCmd.Uint("port", 8500, "port")
	periodicPad := periodicCmd.Uint("pad", 0, "pad")
	periodicInterval := periodicCmd.Uint("interval", 1000, "interval in milliseconds")
	periodicDuration := periodicCmd.Float64("duration", 5, "duration in seconds")
	periodicOut := periodicCmd.String("o", "", "write csv to logfile (default stdout)")
	periodicDirection := periodicCmd.String("direction", "ul", "Send direction: 'ul' (client to server)")

	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverIP := serverCmd.String("ip", "0.0.0.0", "ip")
	serverPort := serverCmd.Uint("port", 8500, "port")

	failMessage := "expected 'burst', 'rate', 'periodic', or 'server' subcommand"

	if len(os.Args) < 2 {
		fmt.Println(failMessage)
		os.Exit(1)
	}

	pkg.RegisterGob()

	var client pkg.Client
	var rpcClient *rpc.Client
	ctx := context.Background()

	switch os.Args[1] {
	case "burst":
		burstCmd.Parse(os.Args[2:])
		if *burstIP == "" {
			fmt.Println("expected -ip")
			os.Exit(1)
		}
		if *burstPort == 0 {
			fmt.Println("expected -port")
			os.Exit(1)
		}
		direction, err := pkg.ParseDirection(*burstDirection)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		rpcClient = dialRpcClient(*burstIP, *burstPort)

		client = &pkg.SenderClient{
			IP:        *burstIP,
			Out:       *burstOut,
			Direction: direction,
			ID:        uuid.New().String(),

			Sender: &pkg.BurstSender{Params: pkg.BurstParams{
				Timeout: time.Duration(*burstTimeout) * time.Millisecond,
				Num:     *burstNum,
				Pad:     *burstPad,
			}},
		}
	case "rate":
		rateCmd.Parse(os.Args[2:])
		if *rateIP == "" {
			fmt.Println("expected -ip")
			os.Exit(1)
		}
		if *ratePort == 0 {
			fmt.Println("expected -port")
			os.Exit(1)
		}
		if *rateRate == 0 {
			fmt.Println("expected -rate")
			os.Exit(1)
		}
		direction, err := pkg.ParseDirection(*rateDirection)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		rpcClient = dialRpcClient(*rateIP, *ratePort)

		client = &pkg.SenderClient{
			IP:        *rateIP,
			Out:       *rateOut,
			Direction: direction,
			ID:        uuid.New().String(),

			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         uint(*rateRate * 1e6 / 8 / 1400),
				Interval:    time.Duration(1) * time.Millisecond,
				Duration:    time.Duration(*rateDuration) * time.Second,
				PayloadSize: 1400,
			}}},
		}

	case "periodic":
		periodicCmd.Parse(os.Args[2:])
		if *periodicIP == "" {
			fmt.Println("expected -ip")
			os.Exit(1)
		}
		if *periodicPort == 0 {
			fmt.Println("expected -port")
			os.Exit(1)
		}
		direction, err := pkg.ParseDirection(*periodicDirection)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}

		rpcClient = dialRpcClient(*periodicIP, *periodicPort)

		client = &pkg.SenderClient{
			IP:        *periodicIP,
			Out:       *periodicOut,
			Direction: direction,
			ID:        uuid.New().String(),

			Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
				Interval: time.Duration(*periodicInterval) * time.Millisecond,
				Duration: time.Duration(*periodicDuration) * time.Second,
				Pad:      *periodicPad,
			}},
		}
	case "server":
		serverCmd.Parse(os.Args[2:])

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := pkg.RunServer(ctx, *serverIP, *serverPort)

		sig := <-sigs
		fmt.Printf("Stopping server on %v\n", sig)
		s.Stop()
		os.Exit(0)
	default:
		fmt.Println(failMessage)
		os.Exit(1)
	}

	defer rpcClient.Close()
	if err := client.Run(ctx, rpcClient); err != nil {
		println(err)
		os.Exit(1)
	}
	if err := client.Gather(ctx, rpcClient); err != nil {
		println(err)
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
