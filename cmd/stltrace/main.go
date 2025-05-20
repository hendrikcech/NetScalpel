package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

func main() {
	clientCmd := flag.NewFlagSet("client", flag.ExitOnError)
	clientIp := clientCmd.String("ip", "", "server ip")
	clientPort := clientCmd.Uint("port", 0, "server port")
	clientResults := clientCmd.String("results", "results", "path to results folder")
	clientRounds := clientCmd.Uint("rounds", 0, "number of measurement rounds to run; 0 = infinite")
	clientProcedure := clientCmd.String("procedure", "trace", "test procedure")

	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	serverIp := serverCmd.String("ip", "0.0.0.0", "ip")
	serverPort := serverCmd.Uint("port", 8500, "port")

	if len(os.Args) < 2 {
		fmt.Println("expected 'client' or 'server' subcommands")
		os.Exit(1)
	}

	pkg.RegisterGob()

	go dumpOnSig()

	switch os.Args[1] {
	case "client":
		clientCmd.Parse(os.Args[2:])
		if *clientIp == "" {
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

		if *clientProcedure != "trace" && *clientProcedure != "burst" {
			fmt.Printf("Unknown -procedure %v\n", *clientProcedure)
			os.Exit(1)
		}

		rpcClient := dialRpcClient(*clientIp, *clientPort)

		for i := range *clientRounds {
			var clients []pkg.Client

			g := new(errgroup.Group)
			now := time.Now()
			resultPath := ""

			if *clientProcedure == "trace" {
				ri := nextRi(now)
				resultPath = mkResultPath(*clientResults, ri)
				log.Printf("[Round %v] Schedule trace in %.2f s at %v", i+1, ri.Sub(time.Now()).Seconds(), ri)
				clients = append(clients, traceRi(ri, rpcClient, resultPath, g, *clientIp, *clientPort)...)

				ri = nextRi(now.Add(15 * time.Second))
				resultPath = mkResultPath(*clientResults, ri)
				log.Printf("[Round %v] Schedule trace in %.2f s at %v", i+1, ri.Sub(time.Now()).Seconds(), ri)
				clients = append(clients, traceRi(ri, rpcClient, resultPath, g, *clientIp, *clientPort)...)
			} else {
				ri := nextRi(now)
				resultPath = mkResultPath(*clientResults, ri)
				log.Printf("[Round %v] Schedule burst in %.2f s at %v", i+1, ri.Sub(time.Now()).Seconds(), ri)
				clients = append(clients, burstRi(ri, rpcClient, resultPath, g, *clientIp, *clientPort)...)
			}

			if err := g.Wait(); err != nil {
				log.Fatalf("%v", err.Error())
			}

			log.Printf("Gathering results...")
			start := time.Now()
			if err := gatherResults(rpcClient, clients); err != nil {
				log.Fatalf("Failed gathering results: %v", err.Error())
			}
			duration := time.Now().Sub(start)
			log.Printf("Gathered results in %.2f seconds", duration.Seconds())

			if err := writeInfo(resultPath, clients); err != nil {
				log.Printf("Failed writing info: %v", err.Error())
			}
		}

	case "server":
		serverCmd.Parse(os.Args[2:])

		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		s := pkg.RunServer(*serverIp, *serverPort)

		sig := <-sigs
		fmt.Printf("Stopping server on %v\n", sig)
		s.Stop()

	default:
		fmt.Println("expected 'client' or 'server' subcommands")
		os.Exit(1)
	}
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

func mkResultPath(base string, ts time.Time) string {
	resultPath := filepath.Join(base, ts.Format("20060102T150405"))
	if err := os.MkdirAll(resultPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create the result directory %v: %v", resultPath, err)
	}
	return resultPath
}

func traceRi(ts time.Time, rpcClient *rpc.Client, resultPath string, g *errgroup.Group, ip string, port uint) []pkg.Client {
	var clients []pkg.Client

	owdStart := ts.Add(7 * time.Second)
	owdUl := pkg.SenderClient{
		Ip:      ip,
		Port:    port,
		Out:     filepath.Join(resultPath, "owd_ul.csv"),
		Reverse: false,
		StartAt: owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	}
	clients = append(clients, &owdUl)
	g.Go(func() error { return owdUl.Run(rpcClient) })
	// owdUl.Run(rpcClient)

	owdDl := pkg.SenderClient{
		Ip:      ip,
		Port:    port,
		Out:     filepath.Join(resultPath, "owd_dl.csv"),
		Reverse: true,
		StartAt: owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	}
	clients = append(clients, &owdDl)
	g.Go(func() error { return owdDl.Run(rpcClient) })

	rateStart := ts.Add(11 * time.Second)
	rateUl := pkg.SenderClient{
		Ip:      ip,
		Port:    port,
		Out:     filepath.Join(resultPath, "rate_ul.csv"),
		Reverse: false,
		StartAt: rateStart,
		Sender: &pkg.RateSender{Params: pkg.RateParams{
			RateMbps:    70,
			Interval:    time.Duration(10) * time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}},
	}
	clients = append(clients, &rateUl)
	g.Go(func() error { return rateUl.Run(rpcClient) })

	rateDl := pkg.SenderClient{
		Ip:      ip,
		Port:    port,
		Out:     filepath.Join(resultPath, "rate_dl.csv"),
		Reverse: true,
		StartAt: rateStart,
		Sender: &pkg.RateSender{Params: pkg.RateParams{
			RateMbps:    700,
			Interval:    time.Duration(10) * time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}},
	}
	clients = append(clients, &rateDl)
	g.Go(func() error { return rateDl.Run(rpcClient) })

	for _, local := range []bool{true, false} {
		var name string
		if local {
			name = "local"
		} else {
			name = "remote"
		}
		client := pkg.CommandClient{
			Params: pkg.TcpdumpParams{
				Name_:    fmt.Sprintf("tcpdump_%v", name),
				Timeout_: 16 * time.Second,
				Filter:   "udp",
			},
			Local:    local,
			StartAt:  owdStart.Add(-500 * time.Millisecond),
			LocalDir: resultPath,
		}
		clients = append(clients, &client)
		g.Go(func() error {
			return client.Run(rpcClient)
		})
	}

	return clients
}

func dialRpcClient(ip string, port uint) *rpc.Client {
	client, err := rpc.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		fmt.Printf("rpc.Dial failed: %v\n", err.Error())
		os.Exit(1)
	}
	return client
}

func dumpOnSig() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGQUIT)
	buf := make([]byte, 1<<20)
	for {
		<-sigs
		stacklen := runtime.Stack(buf, true)
		log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf[:stacklen])
	}
}

func gatherResults(rpcClient *rpc.Client, clients []pkg.Client) error {
	g := new(errgroup.Group)
	for _, client := range clients {
		g.Go(func() error {
			return client.Gather(rpcClient)
		})
	}
	return g.Wait()
}

func writeInfo(path string, clients []pkg.Client) error {
	infoPath := filepath.Join(path, "info.txt")
	f, err := os.Create(infoPath)
	if err != nil {
		return fmt.Errorf("Failed creating %v: %v", infoPath, err.Error())
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	for _, client := range clients {
		w.WriteString(client.Summary())
		w.WriteString("\n")
	}
	return nil
}

func burstRi(ts time.Time, rpcClient *rpc.Client, resultPath string, g *errgroup.Group, ip string, port uint) []pkg.Client {
	var clients []pkg.Client

	smallTimeout := 500 * time.Millisecond
	largeTimeout := 1000 * time.Millisecond
	start := ts.Add(1 * time.Second)

	direction := "ul"
	nums := []uint{1, 10, 50, 100, 150, 200, 250, 300, 400, 550, 700, 850, 1000, 2000}

	// Execute the bursts in random order
	for _, idx := range rand.Perm(len(nums) + 1) {
		timeout := time.Duration(0)
		var client pkg.Client

		// Special case: execute rate test
		if idx == len(nums) {
			duration := 2 * time.Second
			timeout = duration + largeTimeout
			client = &pkg.SenderClient{
				Ip:      ip,
				Port:    port,
				Out:     filepath.Join(resultPath, fmt.Sprintf("rate_%v.csv", direction)),
				Reverse: false,
				StartAt: start,
				Sender: &pkg.RateSender{Params: pkg.RateParams{
					RateMbps:    70,
					Interval:    time.Duration(10) * time.Millisecond,
					Duration:    duration,
					PayloadSize: 1400,
				}},
			}
		} else {
			num := nums[idx]
			if num <= 500 {
				timeout = smallTimeout
			} else if num > 500 {
				timeout = largeTimeout
			}
			client = &pkg.SenderClient{
				Ip:      ip,
				Port:    port,
				Out:     filepath.Join(resultPath, fmt.Sprintf("burst_%v_%04d.csv", direction, num)),
				Reverse: false,
				StartAt: start,
				Sender: &pkg.BurstSender{Params: pkg.BurstParams{
					Timeout: timeout,
					Num:     num,
					Pad:     1400,
				}},
			}
		}

		clients = append(clients, client)
		g.Go(func() error { return client.Run(rpcClient) })
		start = start.Add(timeout)
	}

	for _, local := range []bool{true, false} {
		var name string
		if local {
			name = "local"
		} else {
			name = "remote"
		}
		client := pkg.CommandClient{
			Params: pkg.TcpdumpParams{
				Name_:    fmt.Sprintf("tcpdump_%v", name),
				Timeout_: 15 * time.Second,
				Filter:   "udp",
			},
			Local:    local,
			StartAt:  ts,
			LocalDir: resultPath,
		}
		clients = append(clients, &client)
		g.Go(func() error {
			return client.Run(rpcClient)
		})
	}

	// g.Go(func() error {
	// 	// Leave a gap between sending the last burst and requesting the results
	// 	time.Sleep(time.Until(start.Add(len(nums) * timeout)))
	// 	return nil
	// })

	return clients
}
