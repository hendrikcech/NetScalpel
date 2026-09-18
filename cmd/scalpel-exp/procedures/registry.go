// Package procedures defines and registers the experiment procedures of
// scalpel-exp. Adding a Go file that calls Register in its init function is
// all it takes to add a procedure to the next build; see README.md.
package procedures

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
)

// Mode controls how often the command invokes a procedure per round and how
// the direction parameter is handled.
type Mode uint8

const (
	// PerDirection procedures accept the optional direction=ul|dl parameter
	// implicitly (do not declare it in Params). Without a direction
	// override the command runs the procedure separately for DL and then UL
	// in every round.
	PerDirection Mode = iota
	// OncePerRound procedures run once per round. They accept an optional
	// direction only when SupportsDirection is set.
	OncePerRound
)

// ProcedureFunc schedules one invocation of a procedure. ts is the reference
// Starlink reconfiguration instant, resultPath the output directory of this
// invocation, and params the prepared parameters (see PrepareParams). It
// must only schedule through e.RunClient and e.Tcpdump so dry runs stay
// side-effect free.
type ProcedureFunc func(*experiment.Executor, time.Time, string, experiment.ParamMap) error

// ParamSpec declares one ordinary parameter. The Go type of Default
// determines how overrides are parsed and the value is typed after
// preparation; see README.md for the supported types. Use explicitly typed
// defaults, e.g. uint(50) rather than 50 (an int in any), and typed empty
// slices rather than untyped nil.
type ParamSpec struct {
	Name        string
	Description string
	Default     any
}

// ScheduleTest holds the inputs of the shared dry-run schedule test. The
// harness feeds Params through the same PrepareParams path as production,
// executes the procedure with Executor.DryRun, and compares the normalized
// schedule against <base>_<suffix>.golden next to the procedure source,
// where base is GoldenBase if non-empty, otherwise the procedure name, and
// suffix is uldl or bidir according to the mode.
type ScheduleTest struct {
	// Params are the test overrides, not the runtime defaults; the golden
	// file header shows exactly these. A PerDirection test must supply an
	// explicit direction. Empty (or nil) is valid otherwise.
	Params experiment.ParamMap
	// GoldenBase groups fixtures of a shared implementation under one
	// basename, e.g. "owd" for the owdbidir registration. It must be a
	// plain basename without directory components.
	GoldenBase string
	// AllowBeforeStart permits scheduling before the fixed test anchor ts
	// (tests spanning a reconfiguration instant, like rateri).
	AllowBeforeStart bool
}

// Procedure is one registered experiment definition.
type Procedure struct {
	Name        string
	Description string
	Mode        Mode
	// SupportsDirection marks OncePerRound procedures that accept an
	// optional direction parameter, e.g. owdbidir and ratebidir. It is not
	// needed for PerDirection, which always accepts direction.
	SupportsDirection bool
	Run               ProcedureFunc
	Params            []ParamSpec
	// ScheduleTest must be non-nil: a missing entry is a test coverage
	// failure, reported by the schedule tests. An empty &ScheduleTest{} is
	// valid for procedures that need no test overrides.
	ScheduleTest *ScheduleTest
}

var registry = make(map[string]Procedure)

// Register adds a procedure. It is intended to be called from init
// functions and panics with a diagnostic on developer errors: duplicate
// procedure names across modes, invalid mode, nil Run, duplicate parameter
// names, reserved direction declarations, and unsupported or untyped-nil
// defaults. The registry is populated during initialization and only read
// afterwards.
func Register(p Procedure) {
	if err := register(registry, p); err != nil {
		panic("procedure registration failed: " + err.Error())
	}
}

// register is the private helper behind Register. Tests use it with a local
// map to exercise the collision checks without contaminating the global
// registry.
func register(reg map[string]Procedure, p Procedure) error {
	if p.Name == "" {
		return fmt.Errorf("procedure with an empty name")
	}
	if p.Mode != PerDirection && p.Mode != OncePerRound {
		return fmt.Errorf("procedure %q has invalid mode %d", p.Name, p.Mode)
	}
	if p.Run == nil {
		return fmt.Errorf("procedure %q has a nil Run function", p.Name)
	}
	if _, dup := reg[p.Name]; dup {
		return fmt.Errorf("procedure %q registered twice: procedure names must be unique across all modes", p.Name)
	}
	seen := make(map[string]bool, len(p.Params))
	for _, spec := range p.Params {
		if seen[spec.Name] {
			return fmt.Errorf("procedure %q: duplicate parameter %q", p.Name, spec.Name)
		}
		seen[spec.Name] = true
		if spec.Name == "direction" {
			return fmt.Errorf("procedure %q: parameter name 'direction' is reserved for the implicit direction handling of the command", p.Name)
		}
		if _, err := cloneDefault(spec.Default); err != nil {
			return fmt.Errorf("procedure %q: parameter %q: %w", p.Name, spec.Name, err)
		}
	}
	reg[p.Name] = p
	return nil
}

// Lookup returns the procedure registered under name.
func Lookup(name string) (Procedure, bool) {
	p, ok := registry[name]
	return p, ok
}

// All returns all registered procedures sorted by name for stable help
// output and test ordering.
func All() []Procedure {
	out := make([]Procedure, 0, len(registry))
	for _, p := range registry {
		out = append(out, p)
	}
	slices.SortFunc(out, func(a, b Procedure) int { return cmp.Compare(a.Name, b.Name) })
	return out
}
