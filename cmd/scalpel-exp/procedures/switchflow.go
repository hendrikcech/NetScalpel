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
		Name: "switchflow",
		Description: "One UDP flow stops as a second identical-rate flow starts, " +
			"to observe the ramp-up of the new flow.",
		Mode: PerDirection,
		Run:  SwitchFlow,
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

// Start with one UDP flow. Flow 1 stops after 800 ms and flow 2 starts at the
// same time. Observe if flow 2 has a ramp-up.
func SwitchFlow(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond

	var rates []uint
	if direction == pkg.UL {
		rates = []uint{20, 40, 70}
	} else {
		rates = []uint{200, 250, 300, 700}
	}

	for _, idx := range rng.Perm(len(rates)) {
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

	e.Tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}
