package main

import (
	"reflect"
	"strings"
	"testing"
)

// FuzzParseParams checks that any string parseParams accepts round-trips:
// serializing the resulting map back to k=v;k2=v1,v2 form and parsing again
// must yield the same map.
func FuzzParseParams(f *testing.F) {
	f.Add("")
	f.Add("direction=ul;durations=100,200")
	f.Add("a=;b=1,")
	f.Add("ccas=cubic,direction=DL") // the ,-instead-of-; mistake: must error, not panic

	f.Fuzz(func(t *testing.T, paramStr string) {
		params, err := parseParams(paramStr)
		if err != nil {
			return
		}

		pairs := make([]string, 0, len(params))
		for k, v := range params {
			switch vv := v.(type) {
			case string:
				pairs = append(pairs, k+"="+vv)
			case []string:
				pairs = append(pairs, k+"="+strings.Join(vv, ","))
			default:
				t.Fatalf("parseParams stored a %T for key %q", v, k)
			}
		}
		serialized := strings.Join(pairs, ";")

		reparsed, err := parseParams(serialized)
		if err != nil {
			t.Fatalf("re-parsing %q (from %q) failed: %v", serialized, paramStr, err)
		}
		if !reflect.DeepEqual(params, reparsed) {
			t.Fatalf("round trip of %q via %q: %v != %v", paramStr, serialized, params, reparsed)
		}
	})
}
