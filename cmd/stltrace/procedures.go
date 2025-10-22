package main

import (
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/alistanis/cartesian"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

var proceduresUlDl = map[string]ProcedureFunc{
	"burst":            Burst,
	"prograte":         ProgressiveRate,
	"cddf":             CoolDownDifferentFlow,
	"cdsf":             CoolDownSameFlow,
	"multiflow":        MultiFlow,
	"switchflow":       SwitchFlow,
	"mouseeleph":       MouseElephantFlows,
	"progdurmultirate": ProgressiveDurationMultiRate,
	"simplequic":       QUIC,
	"progdurquic":      ProgressiveDurationQUIC,
	"durationtcp":      DurationTCP,
	"tcpri":            DurationTCPRI,
	"rateri":           RateRI,
	"owd":              MeasOWD,
	"rate":             MeasRate,
}

// Only called once per round
var proceduresBidir = map[string]ProcedureFunc{
	"trace":     TraceRi,
	"owdbidir":  MeasOWD,
	"ratebidir": MeasRate,
}

func PPS(direction pkg.Direction) uint {
	if direction == pkg.UL {
		return 70 * 1e6 / 8 / 1400
	} else if direction == pkg.DL {
		return 700 * 1e6 / 8 / 1400
	} else {
		panic("Unknown direction")
	}
}

// https://stackoverflow.com/a/39868255
// min inclusive, max exclusive
func makeRange(min, max uint) []uint {
	a := make([]uint, max-min)
	for i := range a {
		a[i] = min + uint(i)
	}
	return a
}

func TraceRi(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	owdStart := ts.Add(7 * time.Second)
	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
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
		IP:        e.IP,
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
		IP:        e.IP,
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
		IP:        e.IP,
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

	e.tcpdump(resultPath, owdStart.Add(-500*time.Millisecond), 16*time.Second)

	return nil
}

func ProgressiveRate(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
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
			slog.Error("parse duration", "error", err)
			os.Exit(1)
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
			IP:        e.IP,
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

	e.tcpdump(resultPath, ts, 15*time.Second)

	return nil
}

func ProgressiveDurationMultiRate(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	gap := 2000 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)

	// durationsMs := []int{100, 300, 500, 700, 900, 1400, 2000}
	// durationsMs := []uint{100, 300, 500, 700}
	durationsMs := []uint{1000, 1500, 2000}
	if _, ok := params["durations"]; ok {
		var err error
		if durationsMs, err = params.Uints("durations"); err != nil {
			slog.Error("parse duration", "error", err)
			os.Exit(1)
		}
	}

	var ratesMbps []uint
	if direction == pkg.UL {
		if _, ok := params["ratesUL"]; ok {
			var err error
			if ratesMbps, err = params.Uints("ratesUL"); err != nil {
				slog.Error("parse ratesUL", "error", err)
				os.Exit(1)
			}
		} else {
			// ratesMbps = []uint{70, 35, 10, 5, 1}
			// ratesMbps = []uint{50, 35, 25, 20}
			ratesMbps = []uint{70}
		}
	} else {
		if _, ok := params["ratesDL"]; ok {
			var err error
			if ratesMbps, err = params.Uints("ratesDL"); err != nil {
				slog.Error("parse ratesDL", "error", err)
				os.Exit(1)
			}
		} else {
			// ratesMbps = []uint{700, 350, 100, 5, 1}
			// ratesMbps = []uint{500, 250, 200}
			// ratesMbps = []uint{175, 150, 125}
			// ratesMbps = []uint{700, 600, 500}
			ratesMbps = []uint{700}
		}
	}

	for _, rateIdx := range rand.Perm(len(ratesMbps)) {
		rateMbps := ratesMbps[rateIdx]

		for _, idx := range rand.Perm(len(durationsMs)) {
			durationMs := durationsMs[idx]
			duration := time.Duration(durationMs) * time.Millisecond

			if start.Add(duration).Add(gap).After(deadline) {
				break
			}

			e.RunClient(&pkg.SenderClient{
				IP:        e.IP,
				Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%03d_%04d.csv", direction.StringLower(), rateMbps, durationMs)),
				Direction: direction,
				StartAt:   start,
				Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
					Pps:         rateMbps * 1e6 / 8 / 1400,
					Interval:    time.Millisecond,
					Duration:    duration,
					PayloadSize: 1400,
				}}},
			})

			start = start.Add(duration).Add(gap)
		}
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
	}

	e.tcpdump(resultPath, ts, 15*time.Second)

	return nil
}

// Schedules a rate test over a Starlink reconfiguration.
// Sends for 700 ms before the RI and for 700 ms after the RI.
func RateRI(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	durationMs := 1400
	duration := time.Duration(durationMs) * time.Millisecond
	start := ts.Add(-duration / 2)

	var rateMbps uint
	if direction == pkg.UL {
		rateMbps = 70
	} else {
		rateMbps = 700
	}

	e.RunClient(&pkg.SenderClient{
		IP: e.IP,
		Out: filepath.Join(resultPath, fmt.Sprintf("rateri_%v_%03d_%04d.csv",
			direction.StringLower(), rateMbps, durationMs)),
		Direction: direction,
		StartAt:   start,
		Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
			Pps:         rateMbps * 1e6 / 8 / 1400,
			Interval:    time.Millisecond,
			Duration:    duration,
			PayloadSize: 1400,
		}}},
	})

	e.tcpdump(resultPath, start.Add(-time.Second), 5*time.Second)

	return nil
}

func Burst(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)
	gap := 4000 * time.Millisecond

	pad := []uint{0, 700, 1400}
	var nums []uint
	if direction == pkg.UL {
		nums = []uint{1000, 3000, 6000, 10000}
	} else {
		nums = []uint{1000, 3000, 6000, 10000}
	}
	args := cartesian.Product(nums, pad)

	// Execute the bursts in random order
	for i, idx := range rand.Perm(len(args)) {
		num := args[idx][0]
		pad := args[idx][1]
		if start.Add(gap).After(deadline) {
			slog.Info(fmt.Sprintf("Only executing %v/%v %v tests", i, len(args), direction))
			break
		}
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("burst_%v_%04d_%04d.csv", direction.StringLower(), num, pad)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.BurstSender{Params: pkg.BurstParams{
				Timeout: 4 * time.Second,
				Num:     num,
				Pad:     pad,
			}},
		})

		start = start.Add(gap)
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
	}

	e.tcpdump(resultPath, ts, 15*time.Second)

	return nil
}

func CoolDownDifferentFlow(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
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
outer:
	for {
		// coolDownMs := coolDowns[idx]
		coolDownMs := int64(rand.Intn(400)) / 10 * 10 // Limit to multiples of 10
		coolDownDuration := time.Duration(coolDownMs) * time.Millisecond

		for _, v := range coolDowns {
			if coolDownMs == v {
				// Don't test the same coolDown twice in one round to make it easier for analysis scripts
				continue outer
			}
		}

		// Add cool down before starting rate test
		nextStart := start.Add(duration).Add(coolDownDuration).Add(duration)
		if start.After(deadline) {
			// Would take too long
			break
		}
		coolDowns = append(coolDowns, coolDownMs)

		// Prime the link
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_a.csv", direction.StringLower(), coolDownMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(duration).Add(coolDownDuration)

		// Test after a gap
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_b.csv", direction.StringLower(), coolDownMs)),
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

		if start != nextStart {
			panic("start != nextStart")
		}

		start = start.Add(spacing)
	}

	slog.Info(fmt.Sprintf("Scheduled cooldowns: %v", coolDowns))

	e.tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}

func CoolDownSameFlow(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
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

	// coolDowns := []int{0, 5, 10, 25, 50, 100, 500, 1000}
	// for _, idx := range rand.Perm(len(coolDowns)) {
	var coolDowns []int64
outer:
	for {
		// coolDownMs := coolDowns[idx]
		coolDownMs := int64(rand.Intn(600)) / 10 * 10 // Limit to multiples of 10

		for _, v := range coolDowns {
			if coolDownMs == v {
				// Don't test the same coolDown twice in one round to make it easier for analysis scripts
				continue outer
			}
		}

		// Exponential distribution: test small values more
		// x := rand.ExpFloat64() / 2      // increase rate / lambda
		// y := 2 / math.Pi * math.Atan(x) // map to interval [0, 1]
		// minCoolDownMs := int64(50)
		// maxCoolDownMs := min(deadline.Sub(start.Add(2*duration)).Milliseconds()+minCoolDownMs, 5000-int64(minCoolDownMs))
		// coolDownMs := minCoolDownMs + int64(y*float64(maxCoolDownMs))

		coolDownDuration := time.Duration(coolDownMs) * time.Millisecond

		// Check if test still fits into the current RI
		nextStart := start.Add(2 * duration).Add(coolDownDuration)
		if nextStart.After(deadline) {
			break
		}
		start = nextStart
		coolDowns = append(coolDowns, coolDownMs)

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
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

	slog.Info(fmt.Sprintf("Scheduled with cooldowns: %v", coolDowns))

	e.tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	if start.After(nextRi(ts).Add(time.Second)) {
		return fmt.Errorf("Test takes longer than one RI")
	}

	return nil
}

func MultiFlow(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond

	var pps uint
	if direction == pkg.UL {
		pps = 70 * 1e6 / 8 / 1400
	} else {
		pps = 700 * 1e6 / 8 / 1400
	}

	offsets := []time.Duration{
		0 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		300 * time.Millisecond,
		400 * time.Millisecond,
		500 * time.Millisecond,
		600 * time.Millisecond,
		700 * time.Millisecond,
	}

	for _, idx := range rand.Perm(len(offsets)) {
		offset := offsets[idx]

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_a.csv", direction.StringLower(), offset.Milliseconds())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(offset)

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_b.csv", direction.StringLower(), offset.Milliseconds())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration + (duration - offset), // all end after start + 2 * duration
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(duration).Add(spacing)

		// Check if another test still fits into the current RI
		if start.Add(2 * duration).Add(spacing).After(deadline) {
			break
		}
	}

	e.tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}

// Start with one UDP flow. Flow 1 stops after 800 ms and flow 2 starts at the
// same time. Observe if flow 2 has a ramp-up.
func SwitchFlow(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond

	var rates []uint
	if direction == pkg.UL {
		rates = []uint{20, 40, 70}
	} else {
		rates = []uint{200, 250, 300, 700}
	}

	for _, idx := range rand.Perm(len(rates)) {
		rate := rates[idx]
		pps := uint(rate*1e6) / 8 / 1400

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_a.csv", direction.StringLower(), rate)),
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

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_b.csv", direction.StringLower(), rate)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})
		start = start.Add(duration).Add(spacing)

		// Check if another test still fits into the current RI
		if start.Add(2 * duration).Add(spacing).After(deadline) {
			break
		}
	}

	e.tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}

func MouseElephantFlows(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond

	var elephMbps uint
	var miceMbps []uint
	if direction == pkg.UL {
		elephMbps = 70
		miceMbps = []uint{1, 5, 10, 15, 30, 70}
	} else {
		elephMbps = 700
		miceMbps = []uint{1, 5, 15, 30, 700}
	}

	offsets := []time.Duration{
		// -600 * time.Millisecond,
		// -400 * time.Millisecond,
		// -200 * time.Millisecond,
		0 * time.Millisecond,
		// 200 * time.Millisecond,
		// 400 * time.Millisecond,
		// 600 * time.Millisecond,
	}

outer:
	// for i := 0; ; i++ {
	// for _, idx := range rand.Perm(len(offsets)) {
	// offset := offsets[idx]
	for i, idx := range rand.Perm(len(miceMbps)) {
		mouseMbps := miceMbps[idx]

		offset := offsets[0]

		var startEleph, startMouse time.Time
		durationEleph := duration
		var durationMouse time.Duration
		if offset >= 0 {
			startEleph = start
			startMouse = start.Add(offset)
			// durationMouse = duration + (duration - offset) // End at start + 2 * duration
			durationMouse = duration
		} else {
			startEleph = start.Add(-offset)
			startMouse = start
			durationMouse = 2 * duration // End at start + 2 * duration
		}

		nameEleph := fmt.Sprintf("%v_%d_eleph_%03d.csv", direction.StringLower(), i, elephMbps)
		nameMouse := fmt.Sprintf("%v_%d_mouse_%03d.csv", direction.StringLower(), i, mouseMbps)

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, nameEleph),
			Direction: direction,
			StartAt:   startEleph,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         uint(elephMbps*1e6) / 8 / 1400,
				Interval:    time.Millisecond,
				Duration:    durationEleph,
				PayloadSize: 1400,
			}}},
		})

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, nameMouse),
			Direction: direction,
			StartAt:   startMouse,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         uint(mouseMbps*1e6) / 8 / 1400,
				Interval:    time.Millisecond,
				Duration:    durationMouse,
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(2 * duration).Add(spacing)

		// Check if another test still fits into the current RI
		if start.Add(2 * duration).Add(spacing).After(deadline) {
			break outer
		}
	}
	// }

	e.tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}

func QUIC(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	start := ts.Add(1 * time.Second)
	// deadline := nextRi(ts).Add(-time.Second)

	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, fmt.Sprintf("quic_%v_10M.csv", direction.StringLower())),
		Direction: direction,
		StartAt:   start,
		Sender: &pkg.QUICSender{Params: pkg.QUICParams{
			Duration_: 5 * time.Second,
			Bytes:     10 * 1e6,
		}},
	})

	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, fmt.Sprintf("quic_%v_unlim.csv", direction.StringLower())),
		Direction: direction,
		StartAt:   start.Add(7 * time.Second),
		Sender: &pkg.QUICSender{Params: pkg.QUICParams{
			Duration_: 5 * time.Second,
			Bytes:     1 << 32,
		}},
	})

	e.tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}

func ProgressiveDurationQUIC(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	gap := 1000 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)

	// durationsMs := []int{100, 300, 500, 700, 900, 1400, 2000}
	durationsMs := []uint{100, 400, 1000, 5000}
	if _, ok := params["durations"]; ok {
		var err error
		if durationsMs, err = params.Uints("durations"); err != nil {
			slog.Error("parse duration", "error", err)
			os.Exit(1)
		}
	}

	// Execute the tasks in random order
	for _, idx := range rand.Perm(len(durationsMs)) {
		durationMs := durationsMs[idx]
		duration := time.Duration(durationMs) * time.Millisecond

		nextStart := start.Add(duration).Add(gap)
		if nextStart.After(deadline) {
			// Would take too much time
			break
		}

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("quic_%v_%04d.csv", direction.StringLower(), durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.QUICSender{Params: pkg.QUICParams{
				Duration_: duration,
				Bytes:     1 << 32,
			}},
		})

		start = nextStart
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
	}

	e.tcpdump(resultPath, ts, 15*time.Second)

	return nil
}

func DurationTCP(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	gap := 1000 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := nextRi(ts).Add(-time.Second)

	// durationsMs := []int{100, 300, 500, 700, 900, 1400, 2000}
	// durationsMs := []uint{100, 400, 1000, 3000}
	durationsMs := []uint{3000}
	if _, ok := params["durations"]; ok {
		var err error
		if durationsMs, err = params.Uints("durations"); err != nil {
			slog.Error("parse duration", "error", err)
			os.Exit(1)
		}
	}

	// Tuple of CCA and enableHystart
	// ccas := []pkg.TCPCCA{pkg.CUBIC, pkg.CUBIC_NO_HYSTART, pkg.BBR1}
	ccas := pkg.TCPCCAS
	ccaIdxs := makeRange(0, uint(len(ccas)))

	args := cartesian.Product(durationsMs, ccaIdxs)

	for _, idx := range rand.Perm(len(args)) {
		durationMs := args[idx][0]
		duration := time.Duration(durationMs) * time.Millisecond
		cca := ccas[args[idx][1]]

		nextStart := start.Add(duration).Add(gap)
		if nextStart.After(deadline) {
			// Would take too much time
			break
		}

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("tcp_%v_%v_%04d.csv", direction.StringLower(), cca.String(), durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.TCPSender{Params: pkg.TCPSenderParams{
				Duration_: duration,
				Bytes:     0,
				CCA:       cca,
			}},
		})

		start = nextStart
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
	}

	e.tcpdump(resultPath, ts, 15*time.Second)

	return nil
}

func DurationTCPRI(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %v", err.Error())
	}

	durationMs := 6000
	duration := time.Duration(durationMs) * time.Millisecond
	start := ts.Add(-duration / 2)
	if time.Until(start) < 2*time.Second {
		start = start.Add(15 * time.Second)
	}

	maxIdx := len(pkg.TCPCCAS) + 1
	idx := rand.Intn(maxIdx)
	if idx == maxIdx-1 {
		// Perform UDP rate test
		var rateMbps uint
		if direction == pkg.UL {
			rateMbps = 70
		} else {
			rateMbps = 700
		}
		e.RunClient(&pkg.SenderClient{
			IP: e.IP,
			Out: filepath.Join(resultPath, fmt.Sprintf("udp_%v_%03d_%04d.csv",
				direction.StringLower(), rateMbps, durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         rateMbps * 1e6 / 8 / 1400,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})
	} else {
		cca := pkg.TCPCCAS[idx]
		e.RunClient(&pkg.SenderClient{
			IP: e.IP,
			Out: filepath.Join(resultPath, fmt.Sprintf("tcp_%v_%v_%04d.csv",
				direction.StringLower(), cca.String(), durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.TCPSender{Params: pkg.TCPSenderParams{
				Duration_: duration,
				Bytes:     0,
				CCA:       cca,
			}},
		})
	}

	e.tcpdump(resultPath, start.Add(-time.Second), duration+2*time.Second)

	return nil
}

// Bidirectional, if called without direction parameter
func MeasOWD(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	directions := []pkg.Direction{pkg.DL, pkg.UL}
	if _, ok := params["direction"]; ok {
		var err error
		direction, err := params.Direction()
		if err != nil {
			slog.Error("parse duration", "error", err)
			os.Exit(1)
		}
		directions = []pkg.Direction{direction}
	}

	start := ts.Add(5 * time.Second)

	duration := 60 * time.Second
	if _, ok := params["duration_ms"]; ok {
		value, err := params.Uint("duration_ms")
		if err != nil {
			return err
		}
		duration = time.Duration(value) * time.Millisecond
	}

	for _, direction := range directions {
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("owd_%v.csv", direction.StringLower())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
				Interval: 1 * time.Millisecond,
				Duration: duration,
				Pad:      0,
			}},
		})
	}

	// e.tcpdump(resultPath, ts, duration+time.Second)

	return nil
}

// Bidirectional, if called without direction parameter
func MeasRate(e *Executor, ts time.Time, resultPath string, params ParamMap) error {
	directions := []pkg.Direction{pkg.DL, pkg.UL}
	if _, ok := params["direction"]; ok {
		var err error
		direction, err := params.Direction()
		if err != nil {
			slog.Error("parse duration", "error", err)
			os.Exit(1)
		}
		directions = []pkg.Direction{direction}
	}

	start := ts.Add(5 * time.Second)

	duration := 60 * time.Second
	if _, ok := params["duration_ms"]; ok {
		value, err := params.Uint("duration_ms")
		if err != nil {
			return err
		}
		duration = time.Duration(value) * time.Millisecond
	}

	for _, direction := range directions {
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v.csv", direction.StringLower())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         PPS(direction),
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})
	}

	// Don't run tcpdump due to large pcaps
	// e.tcpdump(resultPath, ts, duration+time.Second)

	return nil
}
