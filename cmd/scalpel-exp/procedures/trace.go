package procedures

import (
	"path/filepath"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func init() {
	Register(Procedure{
		Name: "trace",
		Description: "One-way-delay and rate probes at fixed offsets around a reconfiguration " +
			"instant, with paired packet captures.",
		Mode: OncePerRound,
		Run:  TraceRi,
		// trace does not accept a direction: it always probes both directions.
		ScheduleTest: &ScheduleTest{},
	})
}

func TraceRi(e *experiment.Executor, ts time.Time, resultPath string, params experiment.ParamMap) error {
	owdStart := ts.Add(7 * time.Second)
	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, "owd_ul.csv"),
		Direction: pkg.UL,
		StartAt:   owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	})

	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, "owd_dl.csv"),
		Direction: pkg.DL,
		StartAt:   owdStart,
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: time.Millisecond,
			Duration: time.Duration(3) * time.Second,
			Pad:      0,
		}},
	})

	rateStart := ts.Add(11 * time.Second)
	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, "rate_ul.csv"),
		Direction: pkg.UL,
		StartAt:   rateStart,
		Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
			Pps:         70 * 1e6 / 8 / 1400,
			Interval:    time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}}},
	})

	e.RunClient(&pkg.SenderClient{
		IP:        e.IP,
		Out:       filepath.Join(resultPath, "rate_dl.csv"),
		Direction: pkg.DL,
		StartAt:   rateStart,
		Sender: &pkg.RateSender{Params: []pkg.RateParams{pkg.RateParams{
			Pps:         700 * 1e6 / 8 / 1400,
			Interval:    time.Millisecond,
			Duration:    time.Duration(10) * time.Second, // 10
			PayloadSize: 1400,
		}}},
	})

	e.Tcpdump(resultPath, owdStart.Add(-500*time.Millisecond), 16*time.Second)

	return nil
}
