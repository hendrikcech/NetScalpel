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
		Name: "rateri",
		Description: "UDP rate test at a random rate spanning a reconfiguration instant, " +
			"with concurrent ICMP and UDP OWD probes.",
		Mode: PerDirection,
		Run:  RateRI,
		Params: []ParamSpec{
			{
				Name:        "ratesUL",
				Description: "Candidate rates in megabits per second for the uplink; one is picked at random per round.",
				Default:     []uint{10, 70},
			},
			{
				Name:        "ratesDL",
				Description: "Candidate rates in megabits per second for the downlink; one is picked at random per round.",
				Default:     []uint{50, 700},
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
			// Deliberately schedules around ts, spanning the reconfiguration.
			AllowBeforeStart: true,
		},
	})
}

// Schedules a rate test over a Starlink reconfiguration.
// Sends for 700 ms before the RI and for 700 ms after the RI.
func RateRI(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	durationMs := 3000
	duration := time.Duration(durationMs) * time.Millisecond
	start := ts.Add(-duration / 2)

	ratesMbps, err := ratesFor(direction, params)
	if err != nil {
		return err
	}

	rateMbps := ratesMbps[rng.Intn(len(ratesMbps))]

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

	e.Tcpdump(resultPath, start.Add(-time.Second), 5*time.Second)

	return nil
}
