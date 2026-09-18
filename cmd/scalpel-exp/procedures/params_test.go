package procedures

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func mustLookup(t *testing.T, name string) Procedure {
	t.Helper()
	p, ok := Lookup(name)
	if !ok {
		t.Fatalf("procedure %q is not registered", name)
	}
	return p
}

func TestPrepareParamsAppliesDefaults(t *testing.T) {
	p := mustLookup(t, "prograte")
	prepared, err := PrepareParams(p, experiment.ParamMap{"direction": "ul"})
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared["durations"]; !reflect.DeepEqual(got, []uint{100, 200, 300, 350, 400, 450, 500}) {
		t.Errorf("default durations: got %#v", got)
	}
	if got := prepared["direction"]; got != "ul" {
		t.Errorf("direction: got %#v", got)
	}
}

// Prepared maps must not share mutable storage with the registration or with
// each other: DL, UL, and later rounds are independent invocations.
func TestPrepareParamsDefaultsAreIndependent(t *testing.T) {
	p := mustLookup(t, "prograte")
	dl, err := PrepareParams(p, experiment.ParamMap{"direction": "dl"})
	if err != nil {
		t.Fatal(err)
	}
	ul, err := PrepareParams(p, experiment.ParamMap{"direction": "ul"})
	if err != nil {
		t.Fatal(err)
	}
	dl["durations"].([]uint)[0] = 99999
	if ul["durations"].([]uint)[0] == 99999 {
		t.Error("DL and UL invocations share duration slice storage")
	}
	again, err := PrepareParams(p, experiment.ParamMap{"direction": "ul"})
	if err != nil {
		t.Fatal(err)
	}
	if again["durations"].([]uint)[0] == 99999 {
		t.Error("mutating a prepared map modified the registered defaults")
	}
}

func TestPrepareParamsScalarOverrides(t *testing.T) {
	p := mustLookup(t, "cdsf")
	cases := []struct {
		name      string
		params    experiment.ParamMap
		wantStep  uint
		wantError bool
	}{
		{"raw string", experiment.ParamMap{"step": "25"}, 25, false},
		{"typed uint", experiment.ParamMap{"step": uint(25)}, 25, false},
		{"malformed", experiment.ParamMap{"step": "abc"}, 0, true},
		{"negative", experiment.ParamMap{"step": "-1"}, 0, true},
		{"overflow", experiment.ParamMap{"step": "4294967296"}, 0, true},
		{"list for scalar", experiment.ParamMap{"step": []string{"25", "50"}}, 0, true},
		{"wrong type", experiment.ParamMap{"step": 25}, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prepared, err := PrepareParams(p, c.params)
			if c.wantError {
				if err == nil {
					t.Fatalf("expected an error, got %#v", prepared)
				}
				if !strings.Contains(err.Error(), "cdsf") || !strings.Contains(err.Error(), "step") {
					t.Errorf("error should name the procedure and parameter: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := prepared["step"]; got != c.wantStep {
				t.Errorf("step: got %#v, want %v", got, c.wantStep)
			}
		})
	}
}

func TestPrepareParamsListOverrides(t *testing.T) {
	p := mustLookup(t, "multidurrate")

	raw, err := PrepareParams(p, experiment.ParamMap{"durations": []string{"150", "2500"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := raw["durations"]; !reflect.DeepEqual(got, []uint{150, 2500}) {
		t.Errorf("raw list: got %#v", got)
	}

	single, err := PrepareParams(p, experiment.ParamMap{"durations": "150"})
	if err != nil {
		t.Fatal(err)
	}
	if got := single["durations"]; !reflect.DeepEqual(got, []uint{150}) {
		t.Errorf("single value as list: got %#v", got)
	}

	bad, err := PrepareParams(p, experiment.ParamMap{"durations": []string{"150", "x"}})
	if err == nil {
		t.Errorf("malformed element: expected an error, got %#v", bad)
	}

	// Typed overrides are copied, never retained as caller storage.
	typed := []uint{5, 6}
	prepared, err := PrepareParams(p, experiment.ParamMap{"durations": typed})
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared["durations"]; !reflect.DeepEqual(got, []uint{5, 6}) {
		t.Fatalf("typed list: got %#v", got)
	}
	typed[0] = 999
	if prepared["durations"].([]uint)[0] == 999 {
		t.Error("PrepareParams retained the caller's slice storage")
	}
}

func TestPrepareParamsTCPCCAOverrides(t *testing.T) {
	p := mustLookup(t, "durationtcp")

	// The CLI parser delivers comma-separated values as []string.
	raw, err := PrepareParams(p, experiment.ParamMap{"ccas": []string{"cubic", "bbr1"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := raw["ccas"]; !reflect.DeepEqual(got, []pkg.TCPCCA{pkg.CUBIC, pkg.BBR1}) {
		t.Errorf("string list: got %#v", got)
	}

	single, err := PrepareParams(p, experiment.ParamMap{"ccas": "bbr1"})
	if err != nil {
		t.Fatal(err)
	}
	if got := single["ccas"]; !reflect.DeepEqual(got, []pkg.TCPCCA{pkg.BBR1}) {
		t.Errorf("single name: got %#v", got)
	}

	typed := pkg.TCPCCAS
	prepared, err := PrepareParams(p, experiment.ParamMap{"ccas": typed})
	if err != nil {
		t.Fatal(err)
	}
	if got := prepared["ccas"]; !reflect.DeepEqual(got, pkg.TCPCCAS) {
		t.Fatalf("typed list: got %#v", got)
	}
	prepared["ccas"].([]pkg.TCPCCA)[0] = pkg.BBR3
	if pkg.TCPCCAS[0] == pkg.BBR3 {
		t.Error("PrepareParams retained the pkg.TCPCCAS backing array")
	}

	_, err = PrepareParams(p, experiment.ParamMap{"ccas": "notacca"})
	if err == nil {
		t.Error("unknown CCA: expected an error")
	}
	if err != nil && !strings.Contains(err.Error(), "durationtcp") {
		t.Errorf("error should name the procedure: %v", err)
	}
}

func TestPrepareParamsStringTypes(t *testing.T) {
	// No built-in uses string parameters; exercise the supported types with a
	// synthetic procedure.
	p := Procedure{
		Name: "strproc",
		Mode: PerDirection,
		Run:  noopRun,
		Params: []ParamSpec{
			{Name: "label", Description: "Run label.", Default: "baseline"},
			{Name: "labels", Description: "Run labels.", Default: []string{"a", "b"}},
		},
	}
	prepared, err := PrepareParams(p, experiment.ParamMap{
		"label":  "probe",
		"labels": []string{"x", "y"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := prepared.String("label"); got != "probe" {
		t.Errorf("label: got %#v", got)
	}
	if got := prepared["labels"]; !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Errorf("string list: got %#v", got)
	}
	def, err := PrepareParams(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := def["label"]; got != "baseline" {
		t.Errorf("default label: got %#v", got)
	}
	if got := def["labels"]; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Errorf("default labels: got %#v", got)
	}

	if _, err := PrepareParams(p, experiment.ParamMap{"label": []string{"a", "b"}}); err == nil {
		t.Error("scalar string: expected a list to be rejected")
	}
}

func TestPrepareParamsUnknownParameter(t *testing.T) {
	p := mustLookup(t, "prograte")
	_, err := PrepareParams(p, experiment.ParamMap{"nope": "1"})
	if err == nil {
		t.Fatal("unknown parameter: expected an error")
	}
	if !strings.Contains(err.Error(), "prograte") || !strings.Contains(err.Error(), "nope") {
		t.Errorf("error should name the procedure and parameter: %v", err)
	}
}

func TestPrepareParamsDirection(t *testing.T) {
	perDir := mustLookup(t, "burst")
	owdBidir := mustLookup(t, "owdbidir")
	trace := mustLookup(t, "trace")

	t.Run("per-direction omitted", func(t *testing.T) {
		prepared, err := PrepareParams(perDir, experiment.ParamMap{})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := prepared["direction"]; ok {
			t.Error("an omitted direction must stay absent from the prepared parameters")
		}
	})

	t.Run("per-direction canonicalized", func(t *testing.T) {
		for _, in := range []string{"ul", "UL", "Ul"} {
			prepared, err := PrepareParams(perDir, experiment.ParamMap{"direction": in})
			if err != nil {
				t.Fatalf("%q: %v", in, err)
			}
			if got := prepared["direction"]; got != "ul" {
				t.Errorf("%q: prepared direction is %#v, want the canonical lowercase form", in, got)
			}
		}
	})

	t.Run("per-direction invalid", func(t *testing.T) {
		if _, err := PrepareParams(perDir, experiment.ParamMap{"direction": "sideways"}); err == nil {
			t.Error("invalid direction: expected an error")
		}
		if _, err := PrepareParams(perDir, experiment.ParamMap{"direction": 42}); err == nil {
			t.Error("non-string direction: expected an error")
		}
	})

	t.Run("once-per-round with direction support", func(t *testing.T) {
		prepared, err := PrepareParams(owdBidir, experiment.ParamMap{"direction": "DL"})
		if err != nil {
			t.Fatal(err)
		}
		if got := prepared["direction"]; got != "dl" {
			t.Errorf("direction: got %#v", got)
		}
		prepared, err = PrepareParams(owdBidir, experiment.ParamMap{})
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := prepared["direction"]; ok {
			t.Error("omitted direction must stay absent: absence means both directions in one invocation")
		}
	})

	t.Run("direction rejected without support", func(t *testing.T) {
		_, err := PrepareParams(trace, experiment.ParamMap{"direction": "ul"})
		if err == nil {
			t.Fatal("trace: expected direction to be rejected")
		}
		if !strings.Contains(err.Error(), "trace") {
			t.Errorf("error should name the procedure: %v", err)
		}
	})
}

func TestRatesFor(t *testing.T) {
	p := mustLookup(t, "multidurrate")

	ul, err := PrepareParams(p, experiment.ParamMap{"direction": "ul"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := ratesFor(pkg.UL, ul)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []uint{140}) {
		t.Errorf("UL default rates: got %v", got)
	}

	dl, err := PrepareParams(p, experiment.ParamMap{"direction": "dl"})
	if err != nil {
		t.Fatal(err)
	}
	got, err = ratesFor(pkg.DL, dl)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []uint{500}) {
		t.Errorf("DL default rates: got %v", got)
	}

	// The opposite direction's key is ignored.
	got, err = ratesFor(pkg.UL, experiment.ParamMap{"ratesUL": []uint{70}, "ratesDL": []uint{700}})
	if err != nil || !reflect.DeepEqual(got, []uint{70}) {
		t.Errorf("UL param: got %v (err %v)", got, err)
	}
	got, err = ratesFor(pkg.DL, experiment.ParamMap{"ratesUL": []uint{70}, "ratesDL": []uint{700}})
	if err != nil || !reflect.DeepEqual(got, []uint{700}) {
		t.Errorf("DL param: got %v (err %v)", got, err)
	}
}
