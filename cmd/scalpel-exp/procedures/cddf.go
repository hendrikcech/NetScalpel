package procedures

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func init() {
	Register(Procedure{
		Name: "cddf",
		Description: "Prime the link with a rate test, then after a random cooldown " +
			"repeat the rate test on a second flow.",
		Mode: PerDirection,
		Run:  CoolDownDifferentFlow,
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func CoolDownDifferentFlow(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	start := ts.Add(1 * time.Second)
	duration := time.Duration(800) * time.Millisecond
	spacing := time.Duration(2000) * time.Millisecond
	deadline := experiment.NextRI(ts).Add(-time.Second)

	var pps uint
	if direction == pkg.UL {
		pps = 70 * 1e6 / 8 / 1400
	} else {
		pps = 700 * 1e6 / 8 / 1400
	}

	// coolDowns := []int{0, 10, 50, 100, 500, 1000, 4000}
	// for _, idx := range rng.Perm(len(coolDowns)) {
	var coolDowns []int64
outer:
	for {
		// coolDownMs := coolDowns[idx]
		coolDownMs := int64(rng.Intn(400)) / 10 * 10 // Limit to multiples of 10
		coolDownDuration := time.Duration(coolDownMs) * time.Millisecond

		for _, v := range coolDowns {
			if coolDownMs == v {
				// Don't test the same coolDown twice in one round to make it easier for analysis scripts
				continue outer
			}
		}

		// Add cool down before starting rate test
		nextStart := start.Add(duration).Add(coolDownDuration).Add(duration)
		if start.After(deadline) {
			// Would take too long
			break
		}
		coolDowns = append(coolDowns, coolDownMs)

		// Prime the link
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_a.csv", direction.StringLower(), coolDownMs)),
			Direction: direction,
			StartAt:   start,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         pps,
				Interval:    time.Millisecond,
				Duration:    duration,
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(duration).Add(coolDownDuration)

		// Test after a gap
		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, fmt.Sprintf("rate_%v_%04d_b.csv", direction.StringLower(), coolDownMs)),
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

		if start != nextStart {
			panic("start != nextStart")
		}

		start = start.Add(spacing)
	}

	slog.Info(fmt.Sprintf("Scheduled cooldowns: %v", coolDowns))

	e.Tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}
