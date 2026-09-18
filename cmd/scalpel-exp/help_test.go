package main

// Tests for the dynamic procedures help generated from registration
// metadata. printProcedures must work in both CLI build variants without a
// server; these tests exercise the underlying renderers.

import (
	"strings"
	"testing"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
)

func TestProceduresListTextSortedAndComplete(t *testing.T) {
	all := procedures.All()
	if len(all) < 21 {
		t.Fatalf("expected at least the 21 built-in procedures, got %d", len(all))
	}
	lines := strings.Split(strings.TrimRight(proceduresListText(), "\n"), "\n")
	if len(lines) != len(all)+1 { // header + one line per procedure
		t.Fatalf("list has %d lines for %d procedures: %q", len(lines), len(all), lines)
	}
	for i, p := range all {
		fields := strings.Fields(lines[i+1])
		if fields[0] != p.Name {
			t.Errorf("line %d starts with %q, want %q (deterministic order required)", i+1, fields[0], p.Name)
		}
		wantMode := "[per-direction]"
		if p.Mode == procedures.OncePerRound {
			wantMode = "[once-per-round]"
		}
		if !strings.Contains(lines[i+1], wantMode) {
			t.Errorf("line for %q lacks mode %s: %s", p.Name, wantMode, lines[i+1])
		}
		if !strings.Contains(lines[i+1], p.Description) {
			t.Errorf("line for %q lacks its description: %s", p.Name, lines[i+1])
		}
	}
}

func TestProcedureHelpText(t *testing.T) {
	prograte, ok := procedures.Lookup("prograte")
	if !ok {
		t.Fatal("prograte is not registered")
	}
	help := procedureHelpText(prograte)
	for _, want := range []string{
		"prograte",
		"durations",
		"[]uint",
		"100,200,300,350,400,450,500", // inferred default shown
		"milliseconds",                // units from the description
		"per-direction",
		"once for DL and once for UL", // omission described separately
	} {
		if !strings.Contains(help, want) {
			t.Errorf("prograte help lacks %q:\n%s", want, help)
		}
	}

	trace, _ := procedures.Lookup("trace")
	if help := procedureHelpText(trace); !strings.Contains(help, "Direction: not supported.") {
		t.Errorf("trace help must state that direction is not supported:\n%s", help)
	}
	if help := procedureHelpText(trace); strings.Contains(help, "\nParameters:\n") {
		t.Errorf("trace help must not advertise parameters:\n%s", help)
	}

	owdBidir, _ := procedures.Lookup("owdbidir")
	help = procedureHelpText(owdBidir)
	for _, want := range []string{"once-per-round", "covers both directions", "duration_ms", "60000"} {
		if !strings.Contains(help, want) {
			t.Errorf("owdbidir help lacks %q:\n%s", want, help)
		}
	}

	// CCA defaults are rendered by name, not as raw ints.
	tcpri, _ := procedures.Lookup("tcpri")
	if help := procedureHelpText(tcpri); !strings.Contains(help, "cubic") || !strings.Contains(help, "bbr1") {
		t.Errorf("tcpri help should name the default CCAs:\n%s", help)
	}
}
