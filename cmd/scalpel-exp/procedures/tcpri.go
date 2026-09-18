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
		Name: "tcpri",
		Description: "One TCP flow (or UDP rate test) of 12 s spanning a reconfiguration instant, " +
			"with concurrent ICMP and UDP OWD probes.",
		Mode: PerDirection,
		Run:  DurationTCPRI,
		Params: []ParamSpec{
			{
				Name:        "ccas",
				Description: "TCP congestion control algorithms to choose from, by name (see pkg.ParseTCPCCA).",
				Default:     pkg.TCPCCAS,
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func DurationTCPRI(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	ccas, err := params.TCPCCAs("ccas")
	if err != nil {
		return err
	}

	durationMs := 12000
	duration := time.Duration(durationMs) * time.Millisecond
	start := ts.Add(-duration / 2)
	// Note: time.Until uses the wall clock, unlike the rest of the schedule.
	// The golden schedule test relies on this branch because its fixed anchor
	// date lies in the past.
	if time.Until(start) < 2*time.Second {
		start = start.Add(15 * time.Second)
	}

	var nameSuffix string
	maxIdx := len(ccas) + 1
	idx := rng.Intn(maxIdx)
	if idx == maxIdx-1 {
		// Perform UDP rate test
		var rateMbps uint
		if direction == pkg.UL {
			rateMbps = 200
		} else {
			rateMbps = 500
		}
		nameSuffix = fmt.Sprintf("%v_%03d_%04d.csv", direction.StringLower(), rateMbps, durationMs)
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("udp_%s", nameSuffix)),
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
		cca := ccas[idx]
		nameSuffix = fmt.Sprintf("%v_%v_%04d.csv", direction.StringLower(), cca.String(), durationMs)
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("tcp_%s", nameSuffix)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.TCPSender{Params: pkg.TCPSenderParams{
				Duration_: duration,
				Bytes:     0,
				CCA:       cca,
			}},
		})
	}

	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, fmt.Sprintf("owd-icmp_%s", nameSuffix)),
		Direction: direction,
		StartAt:   start,
		Sender: &pkg.ICMPSender{Params: pkg.ICMPParams{
			Interval:  time.Millisecond,
			Duration_: duration,
		}},
	})
	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, fmt.Sprintf("owd-udp_%s", nameSuffix)),
		Direction: direction,
		StartAt:   start,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: duration,
			Pad:      0,
		}},
	})

	e.Tcpdump(resultPath, start.Add(-time.Second), duration+2*time.Second)

	return nil
}
