package procedures

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/alistanis/cartesian"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func init() {
	Register(Procedure{
		Name: "durationtcp",
		Description: "TCP flows across congestion control algorithms in random order, " +
			"each with concurrent ICMP and UDP OWD probes.",
		Mode: PerDirection,
		Run:  DurationTCP,
		Params: []ParamSpec{
			{
				Name:        "durations",
				Description: "TCP flow durations in milliseconds.",
				Default:     []uint{4000},
			},
			{
				Name:        "ccas",
				Description: "TCP congestion control algorithms to test, by name (see pkg.ParseTCPCCA).",
				Default:     pkg.TCPCCAS,
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func DurationTCP(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	// ccas := []pkg.TCPCCA{pkg.CUBIC, pkg.CUBIC_NO_HYSTART, pkg.BBR1}
	ccas, err := params.TCPCCAs("ccas")
	if err != nil {
		return err
	}

	gap := 2000 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)

	// durationsMs := []uint{100, 400, 1000, 3000}
	durationsMs, err := params.Uints("durations")
	if err != nil {
		return err
	}

	ccaIdxs := makeRange(0, uint(len(ccas)))
	args := cartesian.Product(durationsMs, ccaIdxs)

	for _, idx := range rng.Perm(len(args)) {
		durationMs := args[idx][0]
		duration := time.Duration(durationMs) * time.Millisecond
		cca := ccas[args[idx][1]]

		nextStart := start.Add(duration).Add(gap)
		if nextStart.After(deadline) {
			// Would take too much time
			break
		}

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
		e.RunClient(&pkg.SenderClient{
			IP: e.IP,
			Out: filepath.Join(resultPath, fmt.Sprintf("owd-icmp_%v_%v_%04d.csv",
				direction.StringLower(), cca.String(), durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.ICMPSender{Params: pkg.ICMPParams{
				Interval:  time.Millisecond,
				Duration_: duration,
			}},
		})
		e.RunClient(&pkg.SenderClient{
			IP: e.IP,
			Out: filepath.Join(resultPath, fmt.Sprintf("owd-udp_%v_%v_%04d.csv",
				direction.StringLower(), cca.String(), durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
				Interval: time.Millisecond,
				Duration: duration,
				Pad:      0,
			}},
		})

		start = nextStart
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
	}

	e.Tcpdump(resultPath, ts, 15*time.Second)

	return nil
}
