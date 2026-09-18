package main

// Table tests for the raw CLI parameter parser. The typed accessors live in
// the experiment package and the default/override preparation in the
// procedures package; see their tests.

import (
	"reflect"
	"testing"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
)

func TestParseParams(t *testing.T) {
	got, err := parseParams("")
	if err != nil || len(got) != 0 {
		t.Errorf("empty: got %v (err %v)", got, err)
	}

	got, err = parseParams("direction=ul")
	if err != nil || !reflect.DeepEqual(got, experiment.ParamMap{"direction": "ul"}) {
		t.Errorf("single: got %v (err %v)", got, err)
	}

	got, err = parseParams("direction=ul;durations=100,200")
	want := experiment.ParamMap{"direction": "ul", "durations": []string{"100", "200"}}
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
