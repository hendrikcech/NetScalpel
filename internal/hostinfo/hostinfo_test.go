package hostinfo

import (
	"encoding/json"
	"runtime"
	"testing"
	"time"
)

func TestBasicsAndSnapshots(t *testing.T) {
	h := Basics()
	if h.OS != runtime.GOOS || h.Arch != runtime.GOARCH || h.LogicalCPUs < 1 || h.GOMAXPROCS < 1 || h.Hostname == "" {
		t.Fatalf("bad basics: %+v", h)
	}
	start := time.Now()
	s := Load()
	if s.Time.Before(start) || s.Time.After(time.Now()) {
		t.Fatal("snapshot time out of bounds")
	}
	if runtime.GOOS == "linux" && (s.LoadAvg == nil || s.MemoryAvailableBytes == nil) {
		t.Fatalf("missing basic Linux load: %+v", s)
	}
	if _, err := json.Marshal(s); err != nil {
		t.Fatal(err)
	}
	if field("A: x\nCpus_allowed_list:\t0-3\n", "Cpus_allowed_list") != "0-3" {
		t.Fatal("field parser")
	}
}
