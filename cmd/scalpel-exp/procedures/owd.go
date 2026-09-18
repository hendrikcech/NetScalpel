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
		Name:        "owd",
		Description: "Periodic UDP and ICMP one-way-delay probes.",
		Mode:        PerDirection,
		Run:         MeasOWD,
		Params:      owdParams(),
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul", "duration_ms": "3000"},
		},
	})
	Register(Procedure{
		Name: "owdbidir",
		Description: "Periodic UDP and ICMP one-way-delay probes; one invocation covers " +
			"both directions when direction is omitted.",
		Mode:              OncePerRound,
		SupportsDirection: true,
		Run:               MeasOWD,
		Params:            owdParams(),
		ScheduleTest: &ScheduleTest{
			// Fixture lives next to the shared owd implementation.
			GoldenBase: "owd",
			Params:     experiment.ParamMap{"duration_ms": "3000"},
		},
	})
}

// owdParams is the shared parameter metadata of the two MeasOWD registrations.
func owdParams() []ParamSpec {
	return []ParamSpec{
		{
			Name:        "duration_ms",
			Description: "Probe duration in milliseconds.",
			Default:     uint(60000),
		},
	}
}

// Bidirectional, if called without direction parameter
func MeasOWD(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
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

	interval := 1 * time.Millisecond

	for _, direction := range directions {
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("owd-udp_%v.csv", direction.StringLower())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
				Interval: interval,
				Duration: duration,
				Pad:      0,
			}},
		})
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("owd-icmp_%v.csv", direction.StringLower())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.ICMPSender{Params: pkg.ICMPParams{
				Interval:  interval,
				Duration_: duration,
			}},
		})
	}

	e.Tcpdump(resultPath, ts, 5*time.Second+duration+2*time.Second)

	return nil
}
