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
		Name:        "multiflow",
		Description: "Two UDP flows with random start offsets and staggered durations.",
		Mode:        PerDirection,
		Run:         MultiFlow,
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func MultiFlow(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond

	var pps uint
	if direction == pkg.UL {
		pps = 70 * 1e6 / 8 / 1400
	} else {
		pps = 700 * 1e6 / 8 / 1400
	}

	offsets := []time.Duration{
		0 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		300 * time.Millisecond,
		400 * time.Millisecond,
		500 * time.Millisecond,
		600 * time.Millisecond,
		700 * time.Millisecond,
	}

	for _, idx := range rng.Perm(len(offsets)) {
		offset := offsets[idx]

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_a.csv", direction.StringLower(), offset.Milliseconds())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(offset)

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_b.csv", direction.StringLower(), offset.Milliseconds())),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration + (duration - offset), // all end after start + 2 * duration
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
