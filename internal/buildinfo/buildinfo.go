// Package buildinfo describes the running binary, not the peer or source checkout.
package buildinfo

import (
	"fmt"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

// These strings can be set with go build -ldflags '-X ...'. Empty provenance
// stays unknown; in particular an unreported dirty state is not clean.
var Version = "2.0.0-dev"
var Release = ""
var Revision = ""
var Time = ""
var Dirty = ""

type Info struct {
	Version   string   `json:"version"`
	Release   string   `json:"release,omitempty"`
	Revision  string   `json:"revision,omitempty"`
	Time      string   `json:"time,omitempty"`
	Dirty     *bool    `json:"dirty"`
	GoVersion string   `json:"go_version"`
	Binary    string   `json:"binary,omitempty"`
	BuildTags []string `json:"build_tags"`
}

func Read() Info { b, _ := debug.ReadBuildInfo(); return read(b) }

func read(b *debug.BuildInfo) Info {
	i := Info{Version: Version, Release: Release, GoVersion: runtime.Version(), BuildTags: []string{}}
	dirty := ""
	if b != nil {
		if b.Path != "" {
			i.Binary = filepath.Base(b.Path)
		}
		if b.GoVersion != "" {
			i.GoVersion = b.GoVersion
		}
		for _, s := range b.Settings {
			switch s.Key {
			case "-tags":
				i.BuildTags = strings.FieldsFunc(s.Value, func(r rune) bool { return r == ',' || r == ' ' })
			case "vcs.revision":
				i.Revision = s.Value
			case "vcs.time":
				i.Time = s.Value
			case "vcs.modified":
				dirty = s.Value
			}
		}
	}
	if Revision != "" {
		i.Revision = Revision
	}
	if Time != "" {
		i.Time = Time
	}
	if Dirty != "" {
		dirty = Dirty
	}
	if v, err := strconv.ParseBool(dirty); err == nil {
		i.Dirty = &v
	}
	return i
}

func String() string {
	i := Read()
	return fmt.Sprintf("%s (binary=%s tags=%s release=%s revision=%s time=%s dirty=%v go=%s)", i.Version, i.Binary, strings.Join(i.BuildTags, ","), i.Release, i.Revision, i.Time, dirtyString(i.Dirty), i.GoVersion)
}

func dirtyString(v *bool) string {
	if v == nil {
		return "unknown"
	}
	return strconv.FormatBool(*v)
}
