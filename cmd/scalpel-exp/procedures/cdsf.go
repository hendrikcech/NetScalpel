package procedures

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func init() {
	Register(Procedure{
		Name: "cdsf",
		Description: "Single flow: rate phase, random zero-rate cooldown, second rate phase; " +
			"cooldowns are drawn in multiples of step up to max.",
		Mode: PerDirection,
		Run:  CoolDownSameFlow,
		Params: []ParamSpec{
			{
				Name:        "ratesUL",
				Description: "Rate in megabits per second for the uplink.",
				Default:     []uint{140},
			},
			{
				Name:        "ratesDL",
				Description: "Rate in megabits per second for the downlink.",
				Default:     []uint{500},
			},
			{
				Name:        "step",
				Description: "Cooldowns are multiples of this many milliseconds.",
				Default:     uint(50),
			},
			{
				Name:        "max",
				Description: "Upper bound of the random cooldown range in milliseconds.",
				Default:     uint(600),
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func CoolDownSameFlow(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	start := ts.Add(1 * time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond
	deadline := experiment.NextRI(ts).Add(-time.Second)

	ratesMbps, err := ratesFor(direction, params)
	if err != nil {
		return err
	}
	rateIdx := rng.Intn(len(ratesMbps))
	rateMbps := ratesMbps[rateIdx]
	pps := rateMbps * 1000000 / 8 / 1400

	// Limit cooldowns to multiples of step // tested with step=25
	step, err := params.Uint("step")
	if err != nil {
		return err
	}
	maxBreak, err := params.Uint("max")
	if err != nil {
		return err
	}

	// coolDowns := []int{0, 5, 10, 25, 50, 100, 500, 1000}
	// for _, idx := range rng.Perm(len(coolDowns)) {
	var coolDowns []int64 // Populated with cooldowns that will be tested in this round
outer:
	for {
		// coolDownMs := coolDowns[idx]
		coolDownMs := int64(rng.Intn(int(maxBreak))) / int64(step) * int64(step)

		for _, v := range coolDowns {
			if coolDownMs == v {
				// Don't test the same coolDown twice in one round to make it easier for analysis scripts
				continue outer
			}
		}

		// Exponential distribution: test small values more
		// x := rng.ExpFloat64() / 2      // increase rate / lambda
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
			IP: e.IP,
			Out: filepath.Join(resultPath, fmt.Sprintf("cdrate_%v_%03d_%04d.csv",
				direction.StringLower(), rateMbps, coolDownMs)),
			// earlier filename: fmt.Sprintf("rate_%v_%04d.csv", direction.StringLower(), coolDownMs)),
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

	slog.Info(fmt.Sprintf("Scheduled %s with cooldowns: %v", direction, coolDowns))

	e.Tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	if start.After(experiment.NextRI(ts).Add(time.Second)) {
		return fmt.Errorf("Test takes longer than one RI")
	}

	return nil
}
