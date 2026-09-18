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
		Name:        "burst",
		Description: "Bursts of UDP packets at line rate to probe queue capacity.",
		Mode:        PerDirection,
		Run:         Burst,
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
	Register(Procedure{
		Name:        "multiburst",
		Description: "Two parallel UDP bursts per parameter combination to probe queue capacity.",
		Mode:        PerDirection,
		Run:         MultiBurst,
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func Burst(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	return _MultiBurst(e, ts, resultPath, params, []string{"a"})
}

func MultiBurst(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	return _MultiBurst(e, ts, resultPath, params, []string{"a", "b"})
}

// Send two bursts in parallel
func _MultiBurst(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap, runs []string) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)
	gap := 4000 * time.Millisecond

	pad := []uint{0, 700, 1400}
	var nums []uint
	if direction == pkg.UL {
		// nums = []uint{1000, 3000, 6000, 10000}
		nums = []uint{10000}
	} else {
		nums = []uint{3000}
	}
	args := cartesian.Product(nums, pad)

	// Execute the bursts in random order
	for i, idx := range rng.Perm(len(args)) {
		num := args[idx][0]
		pad := args[idx][1]
		if start.Add(gap).After(deadline) {
			slog.Info(fmt.Sprintf("Only executing %v/%v %v tests", i, len(args), direction))
			break
		}
		for run := range runs {
			// Single or multiple parallel bursts?
			var filename string
			if len(runs) == 1 {
				filename = fmt.Sprintf("burst_%v_%04d_%04d.csv", direction.StringLower(), num, pad)
			} else {
				filename = fmt.Sprintf("burst_%v_%04d_%04d_%v.csv", direction.StringLower(), num, pad, run)
			}
			e.RunClient(&pkg.SenderClient{
				IP:        e.IP,
				Out:       filepath.Join(resultPath, filename),
				Direction: direction,
				StartAt:   start,
				Sender: &pkg.BurstSender{Params: pkg.BurstParams{
					Timeout: gap,
					Num:     num,
					Pad:     pad,
				}},
			})
		}

		start = start.Add(gap)
	}

	if start.After(deadline) {
		panic(fmt.Sprintf("Too many tests: %v > %v", start, deadline))
	}

	e.Tcpdump(resultPath, ts, 15*time.Second)

	return nil
}
