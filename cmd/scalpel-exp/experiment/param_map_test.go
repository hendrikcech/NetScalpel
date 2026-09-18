package experiment

// Table tests for the ParamMap accessors. They accept both normalized typed
// values (from procedures.PrepareParams) and raw string representations, and
// must return errors for wrong dynamic types instead of panicking.

import (
	"reflect"
	"testing"

	"github.com/hendrikcech/netscalpel/pkg"
)

func TestParamMapDirection(t *testing.T) {
	cases := []struct {
		name    string
		params  ParamMap
		want    pkg.Direction
		wantErr bool
	}{
		{"ul", ParamMap{"direction": "ul"}, pkg.UL, false},
		{"dl", ParamMap{"direction": "dl"}, pkg.DL, false},
		{"uppercase", ParamMap{"direction": "DL"}, pkg.DL, false},
		{"missing", ParamMap{}, 0, true},
		{"wrong type", ParamMap{"direction": 42}, 0, true},
		{"invalid value", ParamMap{"direction": "sideways"}, 0, true},
	}
	for _, c := range cases {
		got, err := c.params.Direction()
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected an error, got %v", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
		} else if got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestParamMapString(t *testing.T) {
	got, err := ParamMap{"label": "baseline"}.String("label")
	if err != nil || got != "baseline" {
		t.Errorf("present: got %q (err %v)", got, err)
	}
	if _, err := (ParamMap{}).String("label"); err == nil {
		t.Errorf("missing: expected an error")
	}
	if _, err := (ParamMap{"label": 42}).String("label"); err == nil {
		t.Errorf("wrong type: expected an error")
	}
}

func TestParamMapUint(t *testing.T) {
	cases := []struct {
		name    string
		params  ParamMap
		want    uint
		wantErr bool
	}{
		{"valid", ParamMap{"step": "42"}, 42, false},
		{"zero", ParamMap{"step": "0"}, 0, false},
		{"typed", ParamMap{"step": uint(42)}, 42, false},
		{"missing", ParamMap{}, 0, true},
		{"not a number", ParamMap{"step": "abc"}, 0, true},
		{"negative", ParamMap{"step": "-1"}, 0, true},
		{"overflow", ParamMap{"step": "4294967296"}, 0, true},
		{"wrong type", ParamMap{"step": []string{"42"}}, 0, true},
	}
	for _, c := range cases {
		got, err := c.params.Uint("step")
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected an error, got %v", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
		} else if got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestParamMapUints(t *testing.T) {
	cases := []struct {
		name    string
		params  ParamMap
		want    []uint
		wantErr bool
	}{
		{"raw list", ParamMap{"durations": []string{"100", "200"}}, []uint{100, 200}, false},
		{"single element", ParamMap{"durations": "100"}, []uint{100}, false},
		{"typed list", ParamMap{"durations": []uint{100, 200}}, []uint{100, 200}, false},
		{"missing", ParamMap{}, nil, true},
		{"bad element", ParamMap{"durations": []string{"100", "x"}}, nil, true},
		{"wrong type", ParamMap{"durations": 42}, nil, true},
	}
	for _, c := range cases {
		got, err := c.params.Uints("durations")
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected an error, got %v", c.name, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: unexpected error: %v", c.name, err)
		} else if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

// The typed getters must not hand out the map's own slice storage.
func TestParamMapUintsCopies(t *testing.T) {
	stored := []uint{1, 2}
	params := ParamMap{"durations": stored}
	got, err := params.Uints("durations")
	if err != nil {
		t.Fatal(err)
	}
	got[0] = 99
	if stored[0] != 1 || params["durations"].([]uint)[0] != 1 {
		t.Errorf("Uints returned the caller's slice storage")
	}
}

func TestParamMapOrDefaults(t *testing.T) {
	// Present keys are parsed, absent keys yield the default, parse errors
	// are not masked by the default.
	got, err := ParamMap{"durations": []string{"100", "200"}}.UintsOr("durations", []uint{1})
	if err != nil || !reflect.DeepEqual(got, []uint{100, 200}) {
		t.Errorf("UintsOr present: got %v (err %v)", got, err)
	}
	got, err = ParamMap{}.UintsOr("durations", []uint{1})
	if err != nil || !reflect.DeepEqual(got, []uint{1}) {
		t.Errorf("UintsOr absent: got %v (err %v)", got, err)
	}
	if _, err := (ParamMap{"durations": "x"}).UintsOr("durations", []uint{1}); err == nil {
		t.Errorf("UintsOr invalid: expected an error")
	}

	gotUint, err := ParamMap{"step": "25"}.UintOr("step", 50)
	if err != nil || gotUint != 25 {
		t.Errorf("UintOr present: got %v (err %v)", gotUint, err)
	}
	gotUint, err = ParamMap{}.UintOr("step", 50)
	if err != nil || gotUint != 50 {
		t.Errorf("UintOr absent: got %v (err %v)", gotUint, err)
	}
	if _, err := (ParamMap{"step": "x"}).UintOr("step", 50); err == nil {
		t.Errorf("UintOr invalid: expected an error")
	}

	gotCCAs, err := ParamMap{"ccas": "bbr1"}.TCPCCAsOr("ccas", pkg.TCPCCAS)
	if err != nil || !reflect.DeepEqual(gotCCAs, []pkg.TCPCCA{pkg.BBR1}) {
		t.Errorf("TCPCCAsOr present: got %v (err %v)", gotCCAs, err)
	}
	gotCCAs, err = ParamMap{}.TCPCCAsOr("ccas", pkg.TCPCCAS)
	if err != nil || !reflect.DeepEqual(gotCCAs, pkg.TCPCCAS) {
		t.Errorf("TCPCCAsOr absent: got %v (err %v)", gotCCAs, err)
	}
	if _, err := (ParamMap{"ccas": "notacca"}).TCPCCAsOr("ccas", pkg.TCPCCAS); err == nil {
		t.Errorf("TCPCCAsOr invalid: expected an error")
	}
}

func TestParamMapStrings(t *testing.T) {
	got, err := ParamMap{"ccas": []string{"cubic", "bbr1"}}.Strings("ccas")
	if err != nil || !reflect.DeepEqual(got, []string{"cubic", "bbr1"}) {
		t.Errorf("list: got %v (err %v)", got, err)
	}
	got, err = ParamMap{"ccas": "cubic"}.Strings("ccas")
	if err != nil || !reflect.DeepEqual(got, []string{"cubic"}) {
		t.Errorf("single: got %v (err %v)", got, err)
	}
	if _, err := (ParamMap{}).Strings("ccas"); err == nil {
		t.Errorf("missing: expected an error")
	}
	if _, err := (ParamMap{"ccas": 42}).Strings("ccas"); err == nil {
		t.Errorf("wrong type: expected an error, got a panic or a value")
	}
}

func TestParamMapTCPCCAs(t *testing.T) {
	got, err := ParamMap{"ccas": []string{"cubic", "bbr1"}}.TCPCCAs("ccas")
	if err != nil || !reflect.DeepEqual(got, []pkg.TCPCCA{pkg.CUBIC, pkg.BBR1}) {
		t.Errorf("raw list: got %v (err %v)", got, err)
	}
	got, err = ParamMap{"ccas": pkg.TCPCCAS}.TCPCCAs("ccas")
	if err != nil || !reflect.DeepEqual(got, pkg.TCPCCAS) {
		t.Errorf("typed list: got %v (err %v)", got, err)
	}
	if _, err := (ParamMap{"ccas": "notacca"}).TCPCCAs("ccas"); err == nil {
		t.Errorf("invalid: expected an error")
	}
	if _, err := (ParamMap{}).TCPCCAs("ccas"); err == nil {
		t.Errorf("missing: expected an error")
	}
}
