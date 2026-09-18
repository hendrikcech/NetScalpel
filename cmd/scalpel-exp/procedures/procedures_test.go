package procedures

// Dry-run schedule validation for every registered procedure: each runs
// against a fixed ts with Executor.DryRun set, so the schedule it builds is
// validated with no sockets and no sleeps:
//
//   - generic invariants: StartAt sanity, no zero StartAt, unique
//     non-empty Out paths, tcpdump windows covering every sender window
//   - golden snapshots: the normalized schedule is compared against
//     <base>_<suffix>.golden next to the procedure sources, where base is
//     ScheduleTest.GoldenBase if non-empty, otherwise the procedure name,
//     and suffix is uldl or bidir according to the mode; regenerate a single
//     snapshot with
//     `go test ./cmd/scalpel-exp/procedures -run '^TestProcedureSchedules/prograte_uldl$' -update`
//     and review the diff — the snapshots encode the procedure definitions.
//
// Inputs come from each registration's ScheduleTest metadata; there is no
// central input table.

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

var update = flag.Bool("update", false, "regenerate the golden schedule snapshots")

// procedureTs is the fixed schedule anchor: second :12 is an RI boundary, so
// experiment.NextRI(procedureTs) is :27 and every procedure gets its regular
// ~14s window.
var procedureTs = time.Date(2026, 1, 2, 15, 4, 12, 0, time.UTC)

// goldenFileName returns the fixture name <base>_<suffix>.golden for a
// procedure: base is ScheduleTest.GoldenBase if non-empty, otherwise the
// procedure name; suffix is uldl or bidir according to the mode.
func goldenFileName(p Procedure) (string, error) {
	base := p.Name
	if p.ScheduleTest.GoldenBase != "" {
		base = p.ScheduleTest.GoldenBase
	}
	if base != filepath.Base(base) || base == "." || base == ".." {
		return "", fmt.Errorf("golden base %q must be a plain basename without directory components", p.ScheduleTest.GoldenBase)
	}
	suffix := "uldl"
	if p.Mode == OncePerRound {
		suffix = "bidir"
	}
	return base + "_" + suffix + ".golden", nil
}

func TestProcedureSchedules(t *testing.T) {
	procs := All()
	if len(procs) == 0 {
		t.Fatal("no procedures registered")
	}

	// Metadata checks before running anything: every registration needs
	// schedule-test metadata, a valid basename, and a unique fixture path.
	// Extra fixtures in this directory (e.g. of a private procedure not
	// compiled in) are deliberately not rejected.
	seen := make(map[string]string, len(procs))
	for _, p := range procs {
		if p.ScheduleTest == nil {
			t.Errorf("procedure %q has no ScheduleTest metadata; every procedure must register schedule-test inputs (empty &ScheduleTest{} is valid)", p.Name)
			continue
		}
		name, err := goldenFileName(p)
		if err != nil {
			t.Errorf("procedure %q: %v", p.Name, err)
			continue
		}
		if prev, dup := seen[name]; dup {
			t.Errorf("procedures %q and %q resolve to the same golden fixture %q", prev, p.Name, name)
			continue
		}
		seen[name] = p.Name
	}

	for _, p := range procs {
		if p.ScheduleTest == nil {
			continue // Reported above.
		}
		base := p.Name
		if p.ScheduleTest.GoldenBase != "" {
			base = p.ScheduleTest.GoldenBase
		}
		suffix := "uldl"
		if p.Mode == OncePerRound {
			suffix = "bidir"
		}
		t.Run(base+"_"+suffix, func(t *testing.T) {
			testProcedureSchedule(t, p)
		})
	}
}

type scheduleEntry struct {
	kind      string // "sender" or "command"
	mode      string // sender mode, or command name
	direction string // sender direction, or "local"/"remote" for commands
	startAt   time.Time
	duration  time.Duration // sender duration, or command timeout
	out       string        // Out path relative to resultPath; empty for commands
}

func (s scheduleEntry) String() string {
	line := fmt.Sprintf("%-7s %-11s %-6s start=ts%+v dur=%v",
		s.kind, s.mode, s.direction, s.startAt.Sub(procedureTs), s.duration)
	if s.out != "" {
		line += " out=" + s.out
	}
	return line
}

func testProcedureSchedule(t *testing.T, p Procedure) {
	st := p.ScheduleTest

	if p.Mode == PerDirection {
		if _, ok := st.Params["direction"]; !ok {
			t.Fatalf("schedule test of PerDirection procedure %q must request an explicit direction: each snapshot describes one procedure invocation", p.Name)
		}
	}

	// Deterministic schedule randomization for a stable snapshot. Subtests
	// must not run in parallel: rng is shared package state.
	previousRng := rng
	t.Cleanup(func() { rng = previousRng })
	rng = rand.New(rand.NewSource(1))

	// Same preparation path as production.
	params, err := PrepareParams(p, st.Params)
	if err != nil {
		t.Fatalf("failed preparing schedule test params: %v", err)
	}

	resultPath := t.TempDir()
	e := experiment.NewExecutor(context.Background(), "192.0.2.1", nil)
	e.DryRun = true
	if err := p.Run(e, procedureTs, resultPath, params); err != nil {
		t.Fatalf("procedure returned an error: %v", err)
	}
	if len(e.Clients) == 0 {
		t.Fatalf("procedure scheduled no clients")
	}

	golden, err := goldenFileName(p)
	if err != nil {
		t.Fatalf("%v", err)
	}

	entries := normalizeSchedule(t, e, resultPath)
	assertScheduleInvariants(t, st.AllowBeforeStart, entries)
	// Invariant failures above must stop an invalid snapshot from being
	// written by -update.
	if t.Failed() {
		t.Fatal("skipping golden comparison/update because the schedule violated invariants")
	}
	compareGolden(t, p.Name, golden, st.Params, entries)
}

func normalizeSchedule(t *testing.T, e *experiment.Executor, resultPath string) []scheduleEntry {
	t.Helper()
	entries := make([]scheduleEntry, 0, len(e.Clients))
	for _, client := range e.Clients {
		switch c := client.(type) {
		case *pkg.SenderClient:
			out, err := filepath.Rel(resultPath, c.Out)
			if err != nil {
				// An Out path outside resultPath is itself a schedule bug.
				t.Errorf("Out path %q not below the result path: %v", c.Out, err)
				out = c.Out
			}
			entries = append(entries, scheduleEntry{
				kind:      "sender",
				mode:      c.Sender.SenderMode().String(),
				direction: c.Direction.String(),
				startAt:   c.StartAt,
				duration:  c.Sender.GetParams().GetDuration(),
				out:       out,
			})
		case *pkg.CommandClient:
			side := "remote"
			if c.Local {
				side = "local"
			}
			entries = append(entries, scheduleEntry{
				kind:      "command",
				mode:      c.Params.Name(),
				direction: side,
				startAt:   c.StartAt,
				duration:  c.Params.Timeout(),
			})
		default:
			t.Fatalf("unhandled client type %T", client)
		}
	}
	return entries
}

func assertScheduleInvariants(t *testing.T, allowBeforeStart bool, entries []scheduleEntry) {
	t.Helper()
	ts := procedureTs

	earliest := ts
	if allowBeforeStart {
		earliest = ts.Add(-10 * time.Second)
	}
	latest := ts.Add(5 * time.Minute)

	outs := make(map[string]bool)
	var tcpdumps, senders []scheduleEntry

	for _, en := range entries {
		// 2. A zero StartAt would run immediately and mix control traffic
		// into another test's window.
		if en.startAt.IsZero() {
			t.Errorf("%v: zero StartAt", en)
			continue
		}
		// 1. StartAt sanity bounds.
		if en.startAt.Before(earliest) || en.startAt.After(latest) {
			t.Errorf("%v: StartAt outside [%v, %v]", en, earliest, latest)
		}

		switch en.kind {
		case "sender":
			senders = append(senders, en)
			// 3. Duplicate Out paths silently overwrite results.
			if en.out == "" {
				t.Errorf("%v: empty Out path", en)
			} else if outs[en.out] {
				t.Errorf("%v: duplicate Out path %q", en, en.out)
			}
			outs[en.out] = true
		case "command":
			if strings.HasPrefix(en.mode, "tcpdump") {
				tcpdumps = append(tcpdumps, en)
			}
		}
	}

	// 4. Every tcpdump capture window must contain every sender window of
	// the same invocation.
	for _, td := range tcpdumps {
		tdEnd := td.startAt.Add(td.duration)
		for _, s := range senders {
			sEnd := s.startAt.Add(s.duration)
			if s.startAt.Before(td.startAt) || sEnd.After(tdEnd) {
				t.Errorf("sender window [ts%+v, ts%+v] (%v) not covered by tcpdump window [ts%+v, ts%+v]",
					s.startAt.Sub(ts), sEnd.Sub(ts), s.out,
					td.startAt.Sub(ts), tdEnd.Sub(ts))
			}
		}
	}
}

func compareGolden(t *testing.T, name, golden string, params experiment.ParamMap, entries []scheduleEntry) {
	t.Helper()

	lines := make([]string, 0, len(entries)+2)
	lines = append(lines,
		fmt.Sprintf("# procedure: %s", name),
		// The header shows the test overrides, not every effective default.
		fmt.Sprintf("# params: %s", formatParams(params)))
	body := make([]string, 0, len(entries))
	for _, en := range entries {
		body = append(body, en.String())
	}
	sort.Strings(body)
	got := strings.Join(append(lines, body...), "\n") + "\n"

	path := golden
	if *update {
		if err := os.WriteFile(path, []byte(got), 0644); err != nil {
			t.Fatal(err)
		}
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("missing golden snapshot %v (run with -update and review the result): %v", path, err)
	}
	if got != string(want) {
		t.Errorf("schedule for %q differs from the golden snapshot %v (intended? regenerate with -update and review):\n--- got ---\n%s--- want ---\n%s",
			name, path, got, want)
	}
}

func formatParams(params experiment.ParamMap) string {
	if len(params) == 0 {
		return "(none)"
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, params[k]))
	}
	return strings.Join(parts, ";")
}
