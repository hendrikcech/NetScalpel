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
		Name: "rate",
		Description: "UDP rate test near link capacity (200 Mbps UL, 500 Mbps DL) with " +
			"concurrent ICMP and UDP OWD probes.",
		Mode:   PerDirection,
		Run:    MeasRate,
		Params: rateParams(),
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul", "duration_ms": "3000"},
		},
	})
	Register(Procedure{
		Name: "ratebidir",
		Description: "UDP rate test near link capacity with concurrent ICMP and UDP OWD probes; " +
			"one invocation covers both directions when direction is omitted.",
		Mode:              OncePerRound,
		SupportsDirection: true,
		Run:               MeasRate,
		Params:            rateParams(),
		ScheduleTest: &ScheduleTest{
			// Fixture lives next to the shared rate implementation.
			GoldenBase: "rate",
			Params:     experiment.ParamMap{"duration_ms": "3000"},
		},
	})
}

// rateParams is the shared parameter metadata of the two MeasRate registrations.
func rateParams() []ParamSpec {
	return []ParamSpec{
		{
			Name:        "duration_ms",
			Description: "Test duration in milliseconds.",
			Default:     uint(60000),
		},
	}
}

// Bidirectional, if called without direction parameter
func MeasRate(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	directions := []pkg.Direction{pkg.DL, pkg.UL}
	if _, ok := params["direction"]; ok {
		direction, err := params.Direction()
		if err != nil {
			return err
		}
		directions = []pkg.Direction{direction}
	}

	start := ts.Add(5 * time.Second)

	durationMs, err := params.Uint("duration_ms")
	if err != nil {
		return err
	}
	duration := time.Duration(durationMs) * time.Millisecond

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
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("owd-icmp_%v.csv", direction.StringLower())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.ICMPSender{Params: pkg.ICMPParams{
				Interval:  time.Millisecond,
				Duration_: duration,
			}},
		})
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("owd-udp_%v.csv", direction.StringLower())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
				Interval: time.Millisecond,
				Duration: duration,
				Pad:      0,
			}},
		})
	}

	// Don't run tcpdump due to large pcaps
	// e.Tcpdump(resultPath, ts, duration+time.Second)

	return nil
}
