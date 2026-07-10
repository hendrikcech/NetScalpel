package main

// Table tests for the ParamMap parsing/validation helpers used by every
// procedure.

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

func TestParamMapUint(t *testing.T) {
	cases := []struct {
		name    string
		params  ParamMap
		want    uint
		wantErr bool
	}{
		{"valid", ParamMap{"step": "42"}, 42, false},
		{"zero", ParamMap{"step": "0"}, 0, false},
		{"missing", ParamMap{}, 0, true},
		{"not a number", ParamMap{"step": "abc"}, 0, true},
		{"negative", ParamMap{"step": "-1"}, 0, true},
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
		{"list", ParamMap{"durations": []string{"100", "200"}}, []uint{100, 200}, false},
		{"single element", ParamMap{"durations": "100"}, []uint{100}, false},
		{"missing", ParamMap{}, nil, true},
		{"bad element", ParamMap{"durations": []string{"100", "x"}}, nil, true},
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
}

func TestParamMapTCPCCAs(t *testing.T) {
	got, err := ParamMap{"ccas": []string{"cubic", "bbr1"}}.TCPCCAs("ccas")
	if err != nil || !reflect.DeepEqual(got, []pkg.TCPCCA{pkg.CUBIC, pkg.BBR1}) {
		t.Errorf("list: got %v (err %v)", got, err)
	}
	if _, err := (ParamMap{"ccas": "notacca"}).TCPCCAs("ccas"); err == nil {
		t.Errorf("invalid: expected an error")
	}
	if _, err := (ParamMap{}).TCPCCAs("ccas"); err == nil {
		t.Errorf("missing: expected an error")
	}
}

func TestParseParams(t *testing.T) {
	got, err := parseParams("")
	if err != nil || len(got) != 0 {
		t.Errorf("empty: got %v (err %v)", got, err)
	}

	got, err = parseParams("direction=ul")
	if err != nil || !reflect.DeepEqual(got, ParamMap{"direction": "ul"}) {
		t.Errorf("single: got %v (err %v)", got, err)
	}

	got, err = parseParams("direction=ul;durations=100,200")
	want := ParamMap{"direction": "ul", "durations": []string{"100", "200"}}
	if err != nil || !reflect.DeepEqual(map[string]any(got), map[string]any(want)) {
		t.Errorf("list: got %v (err %v)", got, err)
	}

	if _, err := parseParams("novalue"); err == nil {
		t.Errorf("missing '=': expected an error")
	}
	if _, err := parseParams("a=b=c"); err == nil {
		t.Errorf("double '=': expected an error")
	}
}
