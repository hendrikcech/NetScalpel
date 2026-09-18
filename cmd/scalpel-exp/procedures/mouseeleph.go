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
		Name:        "mouseeleph",
		Description: "Elephant flow at a fixed rate sharing the link with mouse flows at varying rates.",
		Mode:        PerDirection,
		Run:         MouseElephantFlows,
		Params: []ParamSpec{
			{
				Name:        "miceUL",
				Description: "Mouse flow rates in megabits per second for the uplink.",
				Default:     []uint{1, 5, 10, 20, 35, 70, 140},
			},
			{
				Name:        "miceDL",
				Description: "Mouse flow rates in megabits per second for the downlink.",
				Default:     []uint{1, 50, 100, 150, 250},
			},
		},
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func MouseElephantFlows(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	start := ts.Add(1 * time.Second)
	deadline := experiment.NextRI(ts).Add(-time.Second)
	duration := time.Duration(4000) * time.Millisecond
	spacing := time.Duration(3000) * time.Millisecond

	var elephMbps uint
	var miceMbps []uint
	if direction == pkg.UL {
		elephMbps = 140
		miceMbps, err = params.Uints("miceUL")
	} else {
		elephMbps = 250
		miceMbps, err = params.Uints("miceDL")
	}
	if err != nil {
		return err
	}

	offsets := []time.Duration{
		0 * time.Millisecond,
	}

outer:
	for i, idx := range rng.Perm(len(miceMbps)) {
		mouseMbps := miceMbps[idx]

		offset := offsets[0]

		var startEleph, startMouse time.Time
		durationEleph := duration
		var durationMouse time.Duration
		if offset >= 0 {
			startEleph = start
			startMouse = start.Add(offset)
			durationMouse = duration
		} else {
			startEleph = start.Add(-offset)
			startMouse = start
			durationMouse = 2 * duration // End at start + 2 * duration
		}

		nameEleph := fmt.Sprintf("%v_%d_eleph_%03d.csv", direction.StringLower(), i, elephMbps)
		nameMouse := fmt.Sprintf("%v_%d_mouse_%03d.csv", direction.StringLower(), i, mouseMbps)

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, nameEleph),
			Direction: direction,
			StartAt:   startEleph,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         uint(elephMbps*1e6) / 8 / 1400,
				Interval:    time.Millisecond,
				Duration:    durationEleph,
				PayloadSize: 1400,
			}}},
		})

		e.RunClient(&pkg.SenderClient{
			IP:        e.IP,
			Out:       filepath.Join(resultPath, nameMouse),
			Direction: direction,
			StartAt:   startMouse,
			Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
				Pps:         uint(mouseMbps*1e6) / 8 / 1400,
				Interval:    time.Millisecond,
				Duration:    durationMouse,
				PayloadSize: 1400,
			}}},
		})

		start = start.Add(durationMouse).Add(spacing)

		// Check if another test still fits into the current RI
		if start.Add(durationMouse).After(deadline) {
			break outer
		}
	}

	e.Tcpdump(resultPath, ts, start.Sub(ts)+time.Second)

	return nil
}
