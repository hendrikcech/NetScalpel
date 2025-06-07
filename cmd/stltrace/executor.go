package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

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
	durationsMs := []uint{100, 200, 300, 350, 400, 450, 500}
	if _, ok := params["durations"]; ok {
		var err error
		if durationsMs, err = params.Uints("durations"); err != nil {
			log.Fatalf("%v", err.Error())
		}
	}

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
