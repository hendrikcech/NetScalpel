package procedures

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/alistanis/cartesian"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func init() {
	Register(Procedure{
		Name: "packetdur",
		Description: "Send a fixed number of packets over progressively longer transmission " +
			"durations (lower packet rates) in random order.",
		Mode: PerDirection,
		Run:  PacketsDuration,
		Params: []ParamSpec{
			{
				Name:        "durations",
				Description: "Transmission durations in milliseconds.",
				Default:     []uint{10, 15, 20, 30, 35, 40, 50, 60, 70, 80, 100, 150, 200, 400},
			},
			{
				Name:        "packets",
				Description: "Number of packets per test.",
				Default:     []uint{1400},
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func PacketsDuration(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	// durationsMs := []uint{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	// durationsMs := []uint{30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 400}
	durationsMs, err := params.Uints("durations")
	if err != nil {
		return err
	}

	// numPackets := []uint{500}
	// numPackets := []uint{2500}
	numPackets, err := params.Uints("packets")
	if err != nil {
		return err
	}

	// packets=500; [f"{packets} P {duration} ms: {packets * (1000/duration) * 1400 * 8 / 1e6:.0f} Mbps" for duration in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130]]

	start := ts.Add(1 * time.Second)
	spacing := time.Duration(2000) * time.Millisecond
	deadline := experiment.NextRI(ts).Add(-time.Second)

	args := cartesian.Product(durationsMs, numPackets)
	for _, idx := range rng.Perm(len(args)) {
		durationMs := args[idx][0]
		duration := time.Duration(durationMs) * time.Millisecond
		packets := args[idx][1]

		pps := uint(packets * (1000 / durationMs))
		slog.Debug(fmt.Sprintf("%v packets over %v ms -> %v pps", packets, durationMs, pps))
		e.RunClient(&pkg.SenderClient{
			IP: e.IP,
			Out: filepath.Join(resultPath, fmt.Sprintf("packets_%v_%04d_%04d.csv",
				direction.StringLower(), packets, durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})

		// ppns := float64(packets) / float64(durationMs * 1e6)
		// if ppns > 1 {
		// 	panic(fmt.Sprintf("ppns should be < 1 but it's %v", ppns))
		// }
		// packetOnceEveryNs := uint(1 / ppns)
		// interval := time.Duration(packetOnceEveryNs) * time.Nanosecond
		// slog.Debug(fmt.Sprintf("%v packets over %v ms -> one packet every %v ns", packets, durationMs, packetOnceEveryNs))
		// e.RunClient(&pkg.SenderClient{
		// 	IP:        e.IP,
		// 	Out:       filepath.Join(resultPath, fmt.Sprintf("packets_%v_%04d_%04d.csv",
		// 		direction.StringLower(), packets, durationMs)),
		// 	Direction: direction,
		// 	StartAt:   start,
		// 	Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
		// 		Interval: interval,
		// 		Duration: duration,
		// 		Pad:      1400,
		// 	}},
		// })

		start = start.Add(duration).Add(spacing)

		if start.Add(duration).After(deadline) {
			// Next test would take too long
			break
		}
	}

	e.Tcpdump(resultPath, ts, 15*time.Second)

	return nil
}
