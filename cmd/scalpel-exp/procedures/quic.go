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
		Name:        "simplequic",
		Description: "Two QUIC transfers: 10 MB bounded, then an unbounded transfer for 5 s.",
		Mode:        PerDirection,
		Run:         QUIC,
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	})
}

func QUIC(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	direction, err := params.Direction()
	if err != nil {
		return fmt.Errorf("Procedure requires valid 'direction' param: %w", err)
	}

	start := ts.Add(1 * time.Second)
	// deadline := experiment.NextRI(ts).Add(-time.Second)

	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, fmt.Sprintf("quic_%v_10M.csv", direction.StringLower())),
		Direction: direction,
		StartAt:   start,
		Sender: &pkg.QUICSender{Params: pkg.QUICParams{
			Duration_: 5 * time.Second,
			Bytes:     10 * 1e6,
		}},
	})

	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, fmt.Sprintf("quic_%v_unlim.csv", direction.StringLower())),
		Direction: direction,
		StartAt:   start.Add(7 * time.Second),
		Sender: &pkg.QUICSender{Params: pkg.QUICParams{
			Duration_: 5 * time.Second,
			Bytes:     1 << 32,
		}},
	})

	// The second flow ends at start+7s+5s; the capture must cover both flows
	// (start was previously never advanced, capturing only [ts, ts+2s]).
	end := start.Add(7 * time.Second).Add(5 * time.Second)
	e.Tcpdump(resultPath, ts, end.Sub(ts)+time.Second)

	return nil
}
