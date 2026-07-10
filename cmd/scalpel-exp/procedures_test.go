package main

// Dry-run schedule validation for every registered procedure: each runs
// against a fixed ts with Executor.DryRun set, so the schedule it builds is
// validated with no sockets and no sleeps:
//
//   - generic invariants: StartAt sanity, no zero StartAt, unique
//     non-empty Out paths, tcpdump windows covering every sender window
//   - golden snapshots: the normalized schedule is compared against
//     testdata/<registry>_<name>.golden; regenerate with
//     `go test ./cmd/scalpel-exp -run TestProcedureSchedules -update`
//     and review the diff — the snapshots encode the experiment definitions.

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

	"github.com/hendrikcech/netscalpel/pkg"
)

var update = flag.Bool("update", false, "regenerate the golden schedule snapshots")

// procedureTs is the fixed schedule anchor: second :12 is an RI boundary, so
// nextRi(procedureTs) is :27 and every procedure gets its regular ~14s
// window.
var procedureTs = time.Date(2026, 1, 2, 15, 4, 12, 0, time.UTC)

// Minimal valid params per registered procedure. A registry entry without a
// table entry fails the test, forcing the author of a new procedure to add
// one (and thereby a reviewed golden snapshot).
var uldlProcedureParams = map[string]ParamMap{
	"burst":        {"direction": "ul"},
	"multiburst":   {"direction": "ul"},
	"prograte":     {"direction": "ul"},
	"cddf":         {"direction": "ul"},
	"cdsf":         {"direction": "ul"},
	"multiflow":    {"direction": "ul"},
	"switchflow":   {"direction": "ul"},
	"mouseeleph":   {"direction": "ul"},
	"multidurrate": {"direction": "ul"},
	"simplequic":   {"direction": "ul"},
	"progdurquic":  {"direction": "ul"},
	"durationtcp":  {"direction": "ul"},
	"tcpri":        {"direction": "ul"},
	"rateri":       {"direction": "ul"},
	"owd":          {"direction": "ul", "duration_ms": "3000"},
	"rate":         {"direction": "ul", "duration_ms": "3000"},
	"packetdur":    {"direction": "ul"},
	"rampupprobe":  {"direction": "ul"},
}

var bidirProcedureParams = map[string]ParamMap{
	"trace":     {},
	"owdbidir":  {"duration_ms": "3000"},
	"ratebidir": {"duration_ms": "3000"},
}

// Procedures that deliberately schedule around ts instead of after it
// (spanning a Starlink reconfiguration instant).
var startsBeforeTs = map[string]bool{
	"rateri": true,
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

func TestProcedureSchedules(t *testing.T) {
	// Reverse check: stale table entries point at a renamed/removed procedure.
	for name := range uldlProcedureParams {
		if _, ok := proceduresUlDl[name]; !ok {
			t.Errorf("param table entry %q has no matching procedure in proceduresUlDl", name)
		}
	}
	for name := range bidirProcedureParams {
		if _, ok := proceduresBidir[name]; !ok {
			t.Errorf("param table entry %q has no matching procedure in proceduresBidir", name)
		}
	}

	for name, fn := range proceduresUlDl {
		t.Run("uldl_"+name, func(t *testing.T) {
			testProcedureSchedule(t, name, "uldl_"+name, fn, uldlProcedureParams)
		})
	}
	for name, fn := range proceduresBidir {
		t.Run("bidir_"+name, func(t *testing.T) {
			testProcedureSchedule(t, name, "bidir_"+name, fn, bidirProcedureParams)
		})
	}
}

func testProcedureSchedule(t *testing.T, name, id string, fn ProcedureFunc, paramTable map[string]ParamMap) {
	params, ok := paramTable[name]
	if !ok {
		t.Fatalf("procedure %q has no entry in the dry-run param table; add its minimal valid params to procedures_test.go", name)
	}

	// Deterministic schedule randomization for a stable snapshot. Subtests
	// must not run in parallel: rng is shared package state.
	rng = rand.New(rand.NewSource(1))

	resultPath := t.TempDir()
	e := NewExecutor(context.Background(), "192.0.2.1", nil)
	e.DryRun = true
	if err := fn(e, procedureTs, resultPath, params); err != nil {
		t.Fatalf("procedure returned an error: %v", err)
	}
	if len(e.Clients) == 0 {
		t.Fatalf("procedure scheduled no clients")
	}

	entries := normalizeSchedule(t, e, resultPath)
	assertScheduleInvariants(t, name, entries)
	compareGolden(t, name, id, params, entries)
}

func normalizeSchedule(t *testing.T, e *Executor, resultPath string) []scheduleEntry {
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

func assertScheduleInvariants(t *testing.T, name string, entries []scheduleEntry) {
	t.Helper()
	ts := procedureTs

	earliest := ts
	if startsBeforeTs[name] {
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

func compareGolden(t *testing.T, name, id string, params ParamMap, entries []scheduleEntry) {
	t.Helper()

	lines := make([]string, 0, len(entries)+2)
	lines = append(lines,
		fmt.Sprintf("# procedure: %s", name),
		fmt.Sprintf("# params: %s", formatParams(params)))
	body := make([]string, 0, len(entries))
	for _, en := range entries {
		body = append(body, en.String())
	}
	sort.Strings(body)
	got := strings.Join(append(lines, body...), "\n") + "\n"

	path := filepath.Join("testdata", id+".golden")
	if *update {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
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

func formatParams(params ParamMap) string {
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
