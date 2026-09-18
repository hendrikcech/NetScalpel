package procedures

import (
	"math/rand"
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
