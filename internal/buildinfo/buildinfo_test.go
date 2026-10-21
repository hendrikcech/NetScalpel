package buildinfo

import (
	"encoding/json"
	"runtime/debug"
	"strings"
	"testing"
)

func TestProvenance(t *testing.T) {
	if Version != "2.0.0-dev" {
		t.Fatalf("default version %q", Version)
	}
	unknown := read(nil)
	b, _ := json.Marshal(unknown)
	if unknown.Dirty != nil || !strings.Contains(string(b), `"dirty":null`) || unknown.GoVersion == "" {
		t.Fatalf("unknown provenance: %s", b)
	}
	for _, dirty := range []string{"true", "false"} {
		i := read(&debug.BuildInfo{GoVersion: "go-test", Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "rev"}, {Key: "vcs.time", Value: "time"}, {Key: "vcs.modified", Value: dirty}}})
		if i.Revision != "rev" || i.Time != "time" || i.GoVersion != "go-test" || i.Dirty == nil || *i.Dirty != (dirty == "true") {
			t.Fatalf("bad provenance %+v", i)
		}
	}
}

func TestInjectedProvenanceOverridesVCS(t *testing.T) {
	v, r, rev, tm, d := Version, Release, Revision, Time, Dirty
	t.Cleanup(func() { Version, Release, Revision, Time, Dirty = v, r, rev, tm, d })
	Version, Release, Revision, Time, Dirty = "2.0.0", "v2.0.0", "injected", "injected-time", "false"
	i := read(&debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "automatic"}, {Key: "vcs.modified", Value: "true"}}})
	if i.Version != "2.0.0" || i.Release != "v2.0.0" || i.Revision != "injected" || i.Time != "injected-time" || i.Dirty == nil || *i.Dirty {
		t.Fatalf("%+v", i)
	}
	if !strings.Contains(String(), "dirty=false") {
		t.Fatal(String())
	}
}

func TestBinaryAndBuildTags(t *testing.T) {
	i := read(&debug.BuildInfo{Path: "github.com/hendrikcech/netscalpel/cmd/scalpel-exp", Settings: []debug.BuildSetting{{Key: "-tags", Value: "feature_a,custom"}}})
	if i.Binary != "scalpel-exp" || strings.Join(i.BuildTags, ",") != "feature_a,custom" {
		t.Fatalf("identity missing: %+v", i)
	}
}
