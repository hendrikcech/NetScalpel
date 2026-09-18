package procedures

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func init() {
	Register(Procedure{
		Name: "rampupprobe",
		Description: "Short fixed-duration rate tests to observe the sender ramp-up at the " +
			"start of each rate test.",
		Mode: PerDirection,
		Run:  RampUpProbe,
		Params: []ParamSpec{
			{
				Name:        "durations",
				Description: "Rate test durations in milliseconds.",
				Default:     []uint{400},
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func RampUpProbe(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	smallGap := 500 * time.Millisecond
	largeGap := 1500 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)

	durationsMs, err := params.Uints("durations")
	if err != nil {
		return err
	}

	// Execute the bursts in random order
	for _, idx := range rng.Perm(len(durationsMs)) {
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

	e.Tcpdump(resultPath, ts, 15*time.Second)

	return nil
}
