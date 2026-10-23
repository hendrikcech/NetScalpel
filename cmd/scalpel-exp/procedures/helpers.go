package procedures

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

// rng drives the schedule randomization (test order, cooldown values, rate
// picks). It is a package variable so the dry-run schedule tests can swap in
// a deterministically seeded source; seeding the global math/rand generator
// is a no-op since Go 1.24. Procedures run one at a time, so the unlocked
// source is fine.
var rng = rand.New(rand.NewSource(time.Now().UnixNano()))
var dryRunMu sync.Mutex

// ValidateSchedule exercises the procedure's real schedule construction with
// a fresh parameter map and a side-effect-free executor. Its private RNG seed
// is restored afterwards, so batch planning cannot consume runtime random
// choices.
func ValidateSchedule(p Procedure, overrides experiment.ParamMap, seed int64) (validationErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			validationErr = fmt.Errorf("procedure %s schedule panicked: %v", p.Name, recovered)
		}
	}()
	params, err := PrepareParams(p, overrides)
	if err != nil {
		return err
	}
	dryRunMu.Lock()
	defer dryRunMu.Unlock()
	previous := rng
	rng = rand.New(rand.NewSource(seed))
	defer func() { rng = previous }()
	e := experiment.NewExecutor(context.Background(), "127.0.0.1", nil)
	e.DryRun = true
	// A fixed anchor avoids rejecting otherwise valid plans merely because
	// validation happened close to an RI boundary. Procedure timing and layout
	// constraints are still exercised by the real schedule function.
	anchor := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := p.Run(e, anchor, "", params); err != nil {
		return err
	}
	return nil
}

// ratesFor returns the rate list prepared for the direction. Every
// rate-taking procedure registers ratesUL/ratesDL defaults, so the values
// are always present after preparation.
func ratesFor(direction pkg.Direction, params experiment.ParamMap) ([]uint, error) {
	if direction == pkg.UL {
		return params.Uints("ratesUL")
	}
	return params.Uints("ratesDL")
}

// https://stackoverflow.com/a/39868255
// min inclusive, max exclusive
func makeRange(min, max uint) []uint {
	a := make([]uint, max-min)
	for i := range a {
		a[i] = min + uint(i)
	}
	return a
}

func PPS(direction pkg.Direction) uint {
	if direction == pkg.UL {
		return 200 * 1000000 / 8 / 1400
	} else if direction == pkg.DL {
		return 500 * 1000000 / 8 / 1400
	} else {
		panic("Unknown direction")
	}
}
