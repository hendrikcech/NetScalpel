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
		Name:        "multidurrate",
		Description: "Rate tests over random duration and rate combinations, each with concurrent ICMP and UDP OWD probes.",
		Mode:        PerDirection,
		Run:         MultiDurationRate,
		Params: []ParamSpec{
			{
				Name:        "durations",
				Description: "Test durations in milliseconds.",
				Default:     []uint{1000, 4000},
			},
			{
				Name:        "ratesUL",
				Description: "Rates in megabits per second for the uplink.",
				Default:     []uint{140},
			},
			{
				Name:        "ratesDL",
				Description: "Rates in megabits per second for the downlink.",
				Default:     []uint{500},
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

// Previously named prograte / ProgressiveDurationMultiRate
func MultiDurationRate(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	gap := 2000 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)

	durationsMs, err := params.Uints("durations")
	if err != nil {
		return err
	}

	ratesMbps, err := ratesFor(direction, params)
	if err != nil {
		return err
	}

	for _, rateIdx := range rng.Perm(len(ratesMbps)) {
		rateMbps := ratesMbps[rateIdx]

		for _, idx := range rng.Perm(len(durationsMs)) {
			durationMs := durationsMs[idx]
			duration := time.Duration(durationMs) * time.Millisecond

			if start.Add(duration).Add(gap).After(deadline) {
				break
			}

			e.RunClient(&pkg.SenderClient{
				IP: e.IP,
				Out: filepath.Join(resultPath, fmt.Sprintf("rate_%v_%03d_%04d.csv",
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
			e.RunClient(&pkg.SenderClient{
				IP: e.IP,
				Out: filepath.Join(resultPath, fmt.Sprintf("owd-icmp_%v_%03d_%04d.csv",
					direction.StringLower(), rateMbps, durationMs)),
				Direction: direction,
				StartAt:   start,
				Sender: &pkg.ICMPSender{Params: pkg.ICMPParams{
					Interval:  time.Millisecond,
					Duration_: duration,
				}},
			})
			e.RunClient(&pkg.SenderClient{
				IP: e.IP,
				Out: filepath.Join(resultPath, fmt.Sprintf("owd-udp_%v_%03d_%04d.csv",
					direction.StringLower(), rateMbps, durationMs)),
				Direction: direction,
				StartAt:   start,
				Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
					Interval: time.Millisecond,
					Duration: duration,
					Pad:      0,
				}},
			})

			start = start.Add(duration).Add(gap)
		}
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
	}

	e.Tcpdump(resultPath, ts, 15*time.Second)

	return nil
}
