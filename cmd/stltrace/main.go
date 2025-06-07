package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"maps"
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
	clientParams := clientCmd.String("params", "", "comma-separated key=value pairs passed to procedure")

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

		params, err := parseParams(*clientParams)
		if err != nil {
			log.Fatalf("Failed parsing params: %v", err.Error())
		}

		client := Client{
			Ip:        *clientIp,
			Port:      *clientPort,
			Results:   *clientResults,
			Rounds:    *clientRounds,
			Procedure: *clientProcedure,
			Params:    params,
		}
		client.Run()

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

type ProcedureFunc func(time.Time, string, ParamMap)

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

// Parses comma-separated key=value pairs
// If value contains whitespace, value is parsed as a list
func parseParams(paramStr string) (ParamMap, error) {
	params := make(map[string]any)
	if paramStr == "" {
		return params, nil
	}
	for _, kv := range strings.Split(paramStr, ",") {
		parts := strings.Split(kv, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("Invalid key-value pair: %v", kv)
		}
		key := parts[0]
		value := parts[1]
		valueParts := strings.Split(value, " ")
		if len(valueParts) == 1 {
			params[key] = value
		} else {
			params[key] = valueParts
		}
	}
	return params, nil
}

type Client struct {
	Ip        string
	Port      uint
	Results   string
	Rounds    uint
	Procedure string
	Params    map[string]any

	round uint
}

func (c *Client) Run() {
	for i := range c.Rounds {
		e := NewExecutor(c.Ip, c.Port)
		now := time.Now()
		resultPath := ""
		c.round = i

		proceduresUlDl := map[string]ProcedureFunc{
			"burst":    e.BurstRi,
			"prograte": e.ProgressiveRate,
			"cooldown": e.CoolDown,
			"cdsf":     e.CoolDownSameFlow,
		}

		if fn, ok := proceduresUlDl[c.Procedure]; ok {
			if _, ok := c.Params["direction"]; ok {
				c.executeProcedure(now, fn, c.Params)
			} else {
				params := maps.Clone(c.Params)
				params["direction"] = pkg.UL.String()
				c.executeProcedure(now, fn, params)
				params["direction"] = pkg.DL.String()
				c.executeProcedure(now.Add(15*time.Second), fn, params)
			}
		} else if c.Procedure == "trace" {
			c.executeProcedure(now, fn, c.Params)
		} else {
			log.Fatalf("Unknown -procedure '%v'", c.Procedure)
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
}

func (c *Client) executeProcedure(ts time.Time, fn ProcedureFunc, params ParamMap) {
	ri := nextRi(ts)
	log.Printf("[Round %v] Schedule %s %s in %.2fs at %v", c.round, params["direction"], c.Procedure, ri.Sub(time.Now()).Seconds(), ri)
	name := "_" + c.Procedure
	if direction, ok := params["direction"]; ok {
		name += "_" + strings.ToLower(direction.(string))
	}
	resultPath := mkResultPath(c.Results, ri, name)
	fn(ri, resultPath, params)
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

func (e *Executor) TraceRi(ts time.Time, resultPath string, params ParamMap) {
	owdStart := ts.Add(7 * time.Second)
	e.RunClient(&pkg.SenderClient{
		Ip:        e.Ip,
		Port:      e.Port,
		Out:       filepath.Join(resultPath, "owd_ul.csv"),
		Direction: pkg.UL,
		StartAt:   owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	})

	e.RunClient(&pkg.SenderClient{
		Ip:        e.Ip,
		Port:      e.Port,
		Out:       filepath.Join(resultPath, "owd_dl.csv"),
		Direction: pkg.DL,
		StartAt:   owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	})

	rateStart := ts.Add(11 * time.Second)
	e.RunClient(&pkg.SenderClient{
		Ip:        e.Ip,
		Port:      e.Port,
		Out:       filepath.Join(resultPath, "rate_ul.csv"),
		Direction: pkg.UL,
		StartAt:   rateStart,
		Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
			Pps:         70 * 1e6 / 8 / 1400,
			Interval:    time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}}},
	})

	e.RunClient(&pkg.SenderClient{
		Ip:        e.Ip,
		Port:      e.Port,
		Out:       filepath.Join(resultPath, "rate_dl.csv"),
		Direction: pkg.DL,
		StartAt:   rateStart,
		Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
			Pps:         700 * 1e6 / 8 / 1400,
			Interval:    time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}}},
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
}

func (e *Executor) ProgressiveRate(ts time.Time, resultPath string, params ParamMap) {
	direction, err := params.Direction()
	if err != nil {
		log.Fatalf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	smallGap := 500 * time.Millisecond
	largeGap := 1500 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)

	// durationsMs := []int{100, 300, 500, 700, 900, 1400, 2000}
	durationsMs := []int{100, 200, 300, 350, 400, 450, 500}

	// Execute the bursts in random order
	for _, idx := range rand.Perm(len(durationsMs)) {
		durationMs := durationsMs[idx]
		duration := time.Duration(durationMs) * time.Millisecond
		var gap time.Duration
		if durationMs < 1000 {
			gap = smallGap
		} else {
			gap = largeGap
		}
		var pps uint
		if direction == pkg.UL {
			pps = 70 * 1e6 / 8 / 1400
		} else {
			pps = 700 * 1e6 / 8 / 1400
		}
		e.RunClient(&pkg.SenderClient{
			Ip:        e.Ip,
			Port:      e.Port,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d.csv", direction.StringLower(), durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(duration).Add(gap)
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
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
}

func (e *Executor) BurstRi(ts time.Time, resultPath string, params ParamMap) {
	direction, err := params.Direction()
	if err != nil {
		log.Fatalf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	// smallTimeout := 500 * time.Millisecond
	// largeTimeout := 2000 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)

	// nums := []uint{1, 10, 50, 100, 150, 200, 250, 300, 400, 550, 700, 850, 1000, 2000}
	// nums := []uint{1, 10, 20, 30, 40, 50, 100, 150, 200, 250, 300, 400, 500, 1000, 2000}
	var nums []uint
	var gaps []time.Duration
	if direction == pkg.UL {
		nums = []uint{100, 500, 750, 1000, 2000, 3000, 4000, 5000}
		gaps = []time.Duration{500 * time.Millisecond, 1500 * time.Millisecond, 2500 * time.Millisecond}
	} else {
		nums = []uint{100, 500, 750, 1000, 2000, 3000, 4000, 5000}
		gaps = []time.Duration{500 * time.Millisecond, 1000 * time.Millisecond, 1500 * time.Millisecond}
	}

	// Execute the bursts in random order
	for i, idx := range rand.Perm(len(nums) + 1) {
		var gap time.Duration
		// Special case: execute rate test
		if idx == len(nums) {
			duration := 2 * time.Second
			gap = duration + gaps[2]
			if start.Add(gap).After(deadline) {
				log.Printf("Only executing %v/%v %v tests", i, len(nums), direction)
				break
			}
			var pps uint
			if direction == pkg.UL {
				pps = 70 * 1e6 / 8 / 1400
			} else {
				pps = 700 * 1e6 / 8 / 1400
			}
			e.RunClient(&pkg.SenderClient{
				Ip:        e.Ip,
				Port:      e.Port,
				Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v.csv", direction.StringLower())),
				Direction: direction,
				StartAt:   start,
				Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
					Pps:         pps,
					Interval:    time.Millisecond,
					Duration:    duration,
					PayloadSize: 1400,
				}}},
			})
		} else {
			num := nums[idx]
			gap = gaps[0]
			if num <= 2000 {
				gap = gaps[1]
			} else {
				gap = gaps[2]
			}
			if start.Add(gap).After(deadline) {
				log.Printf("Only executing %v/%v %v tests", i, len(nums), direction)
				break
			}
			e.RunClient(&pkg.SenderClient{
				Ip:        e.Ip,
				Port:      e.Port,
				Out:       filepath.Join(resultPath, fmt.Sprintf("burst_%v_%04d.csv", direction.StringLower(), num)),
				Direction: direction,
				StartAt:   start,
				Sender: &pkg.BurstSender{Params: pkg.BurstParams{
					Timeout: 4 * time.Second,
					Num:     num,
					Pad:     1400,
				}},
			})
		}

		start = start.Add(gap)
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
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
}

func (e *Executor) CoolDown(ts time.Time, resultPath string, params ParamMap) {
	direction, err := params.Direction()
	if err != nil {
		log.Fatalf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	start := ts.Add(1 * time.Second)
	duration := time.Duration(800) * time.Millisecond
	deadline := nextRi(ts).Add(-time.Second)

	var pps uint
	if direction == pkg.UL {
		pps = 70 * 1e6 / 8 / 1400
	} else {
		pps = 700 * 1e6 / 8 / 1400
	}

	e.RunClient(&pkg.SenderClient{
		Ip:        e.Ip,
		Port:      e.Port,
		Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_init.csv", direction.StringLower())),
		Direction: direction,
		StartAt:   start,
		Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
			Pps:         pps,
			Interval:    time.Millisecond,
			Duration:    duration,
			PayloadSize: 1400,
		}}},
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
			Ip:        e.Ip,
			Port:      e.Port,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d.csv", direction.StringLower(), coolDownMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
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
}

func (e *Executor) CoolDownSameFlow(ts time.Time, resultPath string, params ParamMap) {
	direction, err := params.Direction()
	if err != nil {
		log.Fatalf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	start := ts.Add(1 * time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond
	deadline := nextRi(ts).Add(-time.Second)

	var pps uint
	if direction == pkg.UL {
		pps = 70 * 1e6 / 8 / 1400
	} else {
		pps = 700 * 1e6 / 8 / 1400
	}

	// coolDowns := []int{0, 10, 50, 100, 500, 1000, 4000}
	// for _, idx := range rand.Perm(len(coolDowns)) {
	var coolDowns []int64
	for {
		// coolDownMs := coolDowns[idx]
		// coolDownMs := 130 + rand.Intn(3000)

		// Exponential distribution: test small values more
		x := rand.ExpFloat64() / 2      // increase rate / lambda
		y := 2 / math.Pi * math.Atan(x) // map to interval [0, 1]
		minCoolDownMs := int64(50)
		maxCoolDownMs := min(deadline.Sub(start.Add(2*duration)).Milliseconds()+minCoolDownMs, 5000-int64(minCoolDownMs))
		coolDownMs := minCoolDownMs + int64(y*float64(maxCoolDownMs))
		coolDownDuration := time.Duration(coolDownMs) * time.Millisecond

		// Check if test still fits into the current RI
		nextStart := start.Add(2 * duration).Add(coolDownDuration)
		if nextStart.After(deadline) {
			break
		}
		start = nextStart
		coolDowns = append(coolDowns, coolDownMs)

		e.RunClient(&pkg.SenderClient{
			Ip:        e.Ip,
			Port:      e.Port,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d.csv", direction.StringLower(), coolDownMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{
				pkg.RateParams{
					Pps:         pps,
					Interval:    time.Millisecond,
					Duration:    duration,
					PayloadSize: 1400,
				},
				pkg.RateParams{
					Pps:         0,
					Interval:    time.Millisecond,
					Duration:    coolDownDuration,
					PayloadSize: 1400,
				},
				pkg.RateParams{
					Pps:         pps,
					Interval:    time.Millisecond,
					Duration:    duration,
					PayloadSize: 1400,
				},
			}},
		})

		// Start next test after this one was run
		start = start.Add(spacing)
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
}
