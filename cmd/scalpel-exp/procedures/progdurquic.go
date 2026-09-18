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
		Name:        "progdurquic",
		Description: "Unbounded QUIC transfers over progressively longer durations in random order.",
		Mode:        PerDirection,
		Run:         ProgressiveDurationQUIC,
		Params: []ParamSpec{
			{
				Name:        "durations",
				Description: "Transfer durations in milliseconds.",
				Default:     []uint{100, 400, 1000, 5000},
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func ProgressiveDurationQUIC(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	gap := 1000 * time.Millisecond
	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)

	durationsMs, err := params.Uints("durations")
	if err != nil {
		return err
	}

	// Execute the tasks in random order
	for _, idx := range rng.Perm(len(durationsMs)) {
		durationMs := durationsMs[idx]
		duration := time.Duration(durationMs) * time.Millisecond

		nextStart := start.Add(duration).Add(gap)
		if nextStart.After(deadline) {
			// Would take too much time
			break
		}

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("quic_%v_%04d.csv", direction.StringLower(), durationMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.QUICSender{Params: pkg.QUICParams{
				Duration_: duration,
				Bytes:     1 << 32,
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
