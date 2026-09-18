package procedures

import (
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
)

func noopRun(*experiment.Executor, time.Time, string, experiment.ParamMap) error {
	return nil
}

func validTestProcedure() Procedure {
	return Procedure{
		Name:        "testproc",
		Description: "Synthetic procedure for registration checks.",
		Mode:        PerDirection,
		Run:         noopRun,
		ScheduleTest: &ScheduleTest{
			Params: experiment.ParamMap{"direction": "ul"},
		},
	}
}

// All registration error checks run against a private map so the global
// registry stays clean.
func TestRegisterRejectsDeveloperErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(p *Procedure)
	}{
		{"empty name", func(p *Procedure) { p.Name = "" }},
		{"invalid mode", func(p *Procedure) { p.Mode = Mode(9) }},
		{"nil run", func(p *Procedure) { p.Run = nil }},
		{"duplicate parameter names", func(p *Procedure) {
			p.Params = []ParamSpec{
				{Name: "durations", Default: []uint{1}},
				{Name: "durations", Default: []uint{2}},
			}
		}},
		{"reserved direction parameter", func(p *Procedure) {
			p.Params = []ParamSpec{{Name: "direction", Default: "ul"}}
		}},
		{"untyped nil default", func(p *Procedure) {
			p.Params = []ParamSpec{{Name: "x", Default: nil}}
		}},
		{"untyped int default", func(p *Procedure) {
			p.Params = []ParamSpec{{Name: "x", Default: 50}}
		}},
		{"unsupported default type", func(p *Procedure) {
			p.Params = []ParamSpec{{Name: "x", Default: true}}
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := validTestProcedure()
			c.mutate(&p)
			if err := register(map[string]Procedure{}, p); err == nil {
				t.Errorf("register() accepted an invalid registration: %+v", p)
			}
		})
	}

	// The valid baseline must pass, so the rejections above are not vacuous.
	if err := register(map[string]Procedure{}, validTestProcedure()); err != nil {
		t.Errorf("register() rejected a valid procedure: %v", err)
	}
}

func TestRegisterRejectsDuplicateNamesAcrossModes(t *testing.T) {
	reg := map[string]Procedure{}
	if err := register(reg, validTestProcedure()); err != nil {
		t.Fatal(err)
	}
	dup := validTestProcedure()
	dup.Mode = OncePerRound
	err := register(reg, dup)
	if err == nil {
		t.Fatal("register() accepted a duplicate name with a different mode")
	}
	if len(reg) != 1 {
		t.Errorf("rejected registration modified the map: %v", reg)
	}
}

// Register panics with a diagnostic; registering a duplicate of a real
// built-in name must panic and must not modify the global registry.
func TestRegisterPanicsOnDuplicate(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Register() did not panic on a duplicate name")
		}
	}()
	burst, ok := Lookup("burst")
	if !ok {
		t.Fatal("built-in procedure burst is missing")
	}
	burst.Description = "pretend duplicate"
	Register(burst)
}

// The migration must preserve all 21 registered procedure names.
func TestAllProceduresPresentSorted(t *testing.T) {
	want := []string{
		"burst", "multiburst", "prograte", "cddf", "cdsf", "multiflow",
		"switchflow", "mouseeleph", "multidurrate", "simplequic",
		"progdurquic", "durationtcp", "tcpri", "rateri", "owd", "rate",
		"packetdur", "rampupprobe", "trace", "owdbidir", "ratebidir",
	}
	all := All()
	got := make(map[string]bool, len(all))
	for _, p := range all {
		got[p.Name] = true
	}
	for _, name := range want {
		if !got[name] {
			t.Errorf("procedure %q is not registered", name)
		}
	}
	for i := 1; i < len(all); i++ {
		if all[i-1].Name >= all[i].Name {
			t.Fatalf("All() is not sorted by name: %v before %v", all[i-1].Name, all[i].Name)
		}
	}
	for _, p := range all {
		if got, ok := Lookup(p.Name); !ok || got.Name != p.Name {
			t.Errorf("Lookup(%q) returned %v, %v", p.Name, got.Name, ok)
		}
	}
	if _, ok := Lookup("no-such-procedure"); ok {
		t.Error("Lookup returned an unknown procedure")
	}
}

func TestProcedureModes(t *testing.T) {
	onceWithoutDir := map[string]bool{"trace": true}
	onceWithDir := map[string]bool{"owdbidir": true, "ratebidir": true}
	for _, p := range All() {
		switch p.Mode {
		case OncePerRound:
			if p.SupportsDirection != onceWithDir[p.Name] {
				t.Errorf("procedure %q: unexpected SupportsDirection=%v", p.Name, p.SupportsDirection)
			}
			if _, ok := onceWithoutDir[p.Name]; !ok && !onceWithDir[p.Name] {
				t.Errorf("procedure %q: unexpected OncePerRound registration", p.Name)
			}
		case PerDirection:
			if onceWithoutDir[p.Name] || onceWithDir[p.Name] {
				t.Errorf("procedure %q: expected a OncePerRound registration", p.Name)
			}
		}
	}
}
