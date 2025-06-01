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
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

func main() {
	clientCmd := flag.NewFlagSet("client", flag.ExitOnError)
	clientIp := clientCmd.String("ip", "", "server ip")
	clientPort := clientCmd.Uint("port", 8500, "server port")
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

		for i := range *clientRounds {
			e := NewExecutor(*clientIp, *clientPort)
			now := time.Now()
			resultPath := ""

			switch *clientProcedure {
			case "trace":
				ri := nextRi(now)
				resultPath = mkResultPath(*clientResults, ri, "_trace")
				log.Printf("[Round %v] Schedule trace in %.2f s at %v", i+1, ri.Sub(time.Now()).Seconds(), ri)
				e.TraceRi(ri, resultPath)

				ri = nextRi(now.Add(15 * time.Second))
				resultPath = mkResultPath(*clientResults, ri, "_trace")
				log.Printf("[Round %v] Schedule trace in %.2f s at %v", i+1, ri.Sub(time.Now()).Seconds(), ri)
				e.TraceRi(ri, resultPath)
			case "burst":
				ri := nextRi(now)
				resultPath = mkResultPath(*clientResults, ri, "_burst")
				direction := DL
				log.Printf("[Round %v] Schedule %s burst in %.2f s at %v", i+1, direction, ri.Sub(time.Now()).Seconds(), ri)
				e.BurstRi(ri, resultPath, direction)
			case "cooldown":
				ri := nextRi(now)
				resultPath = mkResultPath(*clientResults, ri, "_cooldown_ul")
				direction := UL
				log.Printf("[Round %v] Schedule %s %s in %.2f s at %v", i+1, direction, *clientProcedure, ri.Sub(time.Now()).Seconds(), ri)
				e.CoolDown(ri, resultPath, direction)

				ri = nextRi(now.Add(15 * time.Second))
				resultPath = mkResultPath(*clientResults, ri, "_cooldown_dl")
				direction = DL
				log.Printf("[Round %v] Schedule %s %s in %.2f s at %v", i+1, direction, *clientProcedure, ri.Sub(time.Now()).Seconds(), ri)
				e.CoolDown(ri, resultPath, direction)
			default:
				log.Fatalf("Unknown -procedure '%v'", *clientProcedure)
			}

			if err := e.G.Wait(); err != nil {
				log.Fatalf("%v", err.Error())
			}

			log.Printf("Gathering results...")
			start := time.Now()
			if err := e.GatherResults(); err != nil {
				log.Fatalf("Failed gathering results: %v", err.Error())
			}
			duration := time.Now().Sub(start)
			log.Printf("Gathered results in %.2f seconds", duration.Seconds())

			if err := e.WriteInfo(resultPath); err != nil {
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

func mkResultPath(base string, ts time.Time, suffix string) string {
	resultPath := filepath.Join(base, ts.Format("20060102T150405")+suffix)
	if err := os.MkdirAll(resultPath, os.ModePerm); err != nil {
		log.Fatalf("Failed to create the result directory %v: %v", resultPath, err)
	}
	return resultPath
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

type Direction int

const (
	UL Direction = iota
	DL
)

func (d Direction) Reverse() bool {
	switch d {
	case UL:
		return false
	case DL:
		return true
	default:
		panic("Unknown Direction enum type")
	}
}

func (d Direction) String() string {
	switch d {
	case UL:
		return "UL"
	case DL:
		return "DL"
	default:
		panic("Unknown Direction enum type")
	}
}

func (d Direction) StringLower() string {
	return strings.ToLower(d.String())
}

type Executor struct {
	Ip        string
	Port      uint
	RpcClient *rpc.Client
	G         *errgroup.Group
	Clients   []pkg.Client
}

func NewExecutor(ip string, port uint) Executor {
	return Executor{
		Ip:        ip,
		Port:      port,
		RpcClient: dialRpcClient(ip, port),
		G:         new(errgroup.Group),
		Clients:   make([]pkg.Client, 0),
	}
}

func (e *Executor) RunClient(client pkg.Client) {
	e.Clients = append(e.Clients, client)
	e.G.Go(func() error { return client.Run(e.RpcClient) })
}

func (e *Executor) GatherResults() error {
	g := new(errgroup.Group)
	for _, client := range e.Clients {
		g.Go(func() error {
			return client.Gather(e.RpcClient)
		})
	}
	return g.Wait()
}

func (e *Executor) WriteInfo(path string) error {
	infoPath := filepath.Join(path, "info.txt")
	f, err := os.Create(infoPath)
	if err != nil {
		return fmt.Errorf("Failed creating %v: %v", infoPath, err.Error())
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	for _, client := range e.Clients {
		w.WriteString(client.Summary())
		w.WriteString("\n")
	}
	return nil
}

func (e *Executor) TraceRi(ts time.Time, resultPath string) []pkg.Client {
	var clients []pkg.Client

	owdStart := ts.Add(7 * time.Second)
	e.RunClient(&pkg.SenderClient{
		Ip:      e.Ip,
		Port:    e.Port,
		Out:     filepath.Join(resultPath, "owd_ul.csv"),
		Reverse: false,
		StartAt: owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	})

	e.RunClient(&pkg.SenderClient{
		Ip:      e.Ip,
		Port:    e.Port,
		Out:     filepath.Join(resultPath, "owd_dl.csv"),
		Reverse: true,
		StartAt: owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	})

	rateStart := ts.Add(11 * time.Second)
	e.RunClient(&pkg.SenderClient{
		Ip:      e.Ip,
		Port:    e.Port,
		Out:     filepath.Join(resultPath, "rate_ul.csv"),
		Reverse: false,
		StartAt: rateStart,
		Sender: &pkg.RateSender{Params: pkg.RateParams{
			Pps:         70 * 1e6 / 8 / 1400,
			Interval:    time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}},
	})

	e.RunClient(&pkg.SenderClient{
		Ip:      e.Ip,
		Port:    e.Port,
		Out:     filepath.Join(resultPath, "rate_dl.csv"),
		Reverse: true,
		StartAt: rateStart,
		Sender: &pkg.RateSender{Params: pkg.RateParams{
			Pps:         700 * 1e6 / 8 / 1400,
			Interval:    time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}},
	})

	for _, local := range []bool{true, false} {
		var name string
		if local {
			name = "local"
		} else {
			name = "remote"
		}
		e.RunClient(&pkg.CommandClient{
			Params: pkg.TcpdumpParams{
				Name_:    fmt.Sprintf("tcpdump_%v", name),
				Timeout_: 16 * time.Second,
				Filter:   "udp",
			},
			Local:    local,
			StartAt:  owdStart.Add(-500 * time.Millisecond),
			LocalDir: resultPath,
		})
	}

	return clients
}

func (e *Executor) BurstRi(ts time.Time, resultPath string, direction Direction) []pkg.Client {
	var clients []pkg.Client

	smallTimeout := 500 * time.Millisecond
	largeTimeout := 1000 * time.Millisecond
	start := ts.Add(1 * time.Second)

	// nums := []uint{1, 10, 50, 100, 150, 200, 250, 300, 400, 550, 700, 850, 1000, 2000}
	nums := []uint{1, 10, 20, 30, 40, 50, 100, 150, 200, 250, 300, 400, 500, 1000, 2000}

	// Execute the bursts in random order
	for _, idx := range rand.Perm(len(nums) + 1) {
		timeout := time.Duration(0)
		// Special case: execute rate test
		if idx == len(nums) {
			duration := 2 * time.Second
			timeout = duration + largeTimeout
			var pps uint
			if direction == UL {
				pps = 70 * 1e6 / 8 / 1400
			} else {
				pps = 700 * 1e6 / 8 / 1400
			}
			e.RunClient(&pkg.SenderClient{
				Ip:      e.Ip,
				Port:    e.Port,
				Out:     filepath.Join(resultPath, fmt.Sprintf("rate_%v.csv", strings.ToLower(direction.String()))),
				Reverse: direction.Reverse(),
				StartAt: start,
				Sender: &pkg.RateSender{Params: pkg.RateParams{
					Pps:         pps,
					Interval:    time.Millisecond,
					Duration:    duration,
					PayloadSize: 1400,
				}},
			})
		} else {
			num := nums[idx]
			if num <= 500 {
				timeout = smallTimeout
			} else if num > 500 {
				timeout = largeTimeout
			}
			e.RunClient(&pkg.SenderClient{
				Ip:      e.Ip,
				Port:    e.Port,
				Out:     filepath.Join(resultPath, fmt.Sprintf("burst_%v_%04d.csv", strings.ToLower(direction.String()), num)),
				Reverse: direction.Reverse(),
				StartAt: start,
				Sender: &pkg.BurstSender{Params: pkg.BurstParams{
					Timeout: timeout,
					Num:     num,
					Pad:     1400,
				}},
			})
		}

		start = start.Add(timeout)
	}

	for _, local := range []bool{true, false} {
		var name string
		if local {
			name = "local"
		} else {
			name = "remote"
		}
		e.RunClient(&pkg.CommandClient{
			Params: pkg.TcpdumpParams{
				Name_:    fmt.Sprintf("tcpdump_%v", name),
				Timeout_: 15 * time.Second,
				Filter:   "udp",
			},
			Local:    local,
			StartAt:  ts,
			LocalDir: resultPath,
		})
	}

	// e.G.Go(func() error {
	// 	// Leave a gap between sending the last burst and requesting the results
	// 	time.Sleep(time.Until(start.Add(len(nums) * timeout)))
	// 	return nil
	// })

	return clients
}

func (e *Executor) CoolDown(ts time.Time, resultPath string, direction Direction) []pkg.Client {
	var clients []pkg.Client

	start := ts.Add(1 * time.Second)
	duration := time.Duration(800) * time.Millisecond
	deadline := nextRi(ts).Add(-time.Second)

	var pps uint
	if direction == UL {
		pps = 70 * 1e6 / 8 / 1400
	} else {
		pps = 700 * 1e6 / 8 / 1400
	}

	e.RunClient(&pkg.SenderClient{
		Ip:      e.Ip,
		Port:    e.Port,
		Out:     filepath.Join(resultPath, fmt.Sprintf("rate_%v_init.csv", direction.StringLower())),
		Reverse: direction.Reverse(),
		StartAt: start,
		Sender: &pkg.RateSender{Params: pkg.RateParams{
			Pps:         pps,
			Interval:    time.Millisecond,
			Duration:    duration,
			PayloadSize: 1400,
		}},
	})
	start = start.Add(duration)

	// coolDowns := []int{0, 10, 50, 100, 500, 1000, 4000}
	// for _, idx := range rand.Perm(len(coolDowns)) {
	var coolDowns []int
	for {
		// coolDownMs := coolDowns[idx]
		// coolDownMs := 130 + rand.Intn(3000)

		x := rand.ExpFloat64() / 1.5
		y := 2 / math.Pi * math.Atan(x)
		minCoolDownMs := 200
		maxCoolDownMs := min(deadline.Sub(start.Add(duration)).Milliseconds(), 3000-int64(minCoolDownMs))
		coolDownMs := minCoolDownMs + int(y*float64(maxCoolDownMs))

		// Add cool down before starting rate test
		nextStart := start.Add(time.Duration(coolDownMs) * time.Millisecond)
		if start.Add(duration).After(deadline) {
			// Would take too long
			break
		}
		start = nextStart
		coolDowns = append(coolDowns, coolDownMs)

		e.RunClient(&pkg.SenderClient{
			Ip:      e.Ip,
			Port:    e.Port,
			Out:     filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d.csv", direction.StringLower(), coolDownMs)),
			Reverse: direction.Reverse(),
			StartAt: start,
			Sender: &pkg.RateSender{Params: pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}},
		})

		// Start next test after this one was run
		start = start.Add(duration)
	}

	log.Printf("Scheduled with cooldowns: %v", coolDowns)

	for _, local := range []bool{true, false} {
		var name string
		if local {
			name = "local"
		} else {
			name = "remote"
		}
		e.RunClient(&pkg.CommandClient{
			Params: pkg.TcpdumpParams{
				Name_:    fmt.Sprintf("tcpdump_%v", name),
				Timeout_: start.Sub(ts) + time.Second,
				Filter:   "udp",
			},
			Local:    local,
			StartAt:  ts,
			LocalDir: resultPath,
		})
	}

	if start.After(nextRi(ts).Add(time.Second)) {
		panic("Test takes longer than one RI")
	}

	// e.G.Go(func() error {
	// 	// Leave a gap between sending the last burst and requesting the results
	// 	time.Sleep(time.Until(start.Add(len(nums) * timeout)))
	// 	return nil
	// })

	return clients
}
