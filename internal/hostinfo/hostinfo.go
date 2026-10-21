// Package hostinfo collects small, unprivileged metadata snapshots. It never
// invokes external diagnostics or reads the environment or command line.
package hostinfo

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type Host struct {
	Hostname         string            `json:"hostname"`
	OS               string            `json:"os"`
	Distro           string            `json:"distro,omitempty"`
	Kernel           string            `json:"kernel,omitempty"`
	Arch             string            `json:"arch"`
	CPUModel         string            `json:"cpu_model,omitempty"`
	LogicalCPUs      int               `json:"logical_cpus"`
	Affinity         string            `json:"cpu_affinity,omitempty"`
	GOMAXPROCS       int               `json:"gomaxprocs"`
	MemoryTotalBytes *uint64           `json:"memory_total_bytes,omitempty"`
	Limits           map[string]string `json:"limits,omitempty"`
	Warnings         []string          `json:"warnings,omitempty"`
}

type Snapshot struct {
	Time                 time.Time         `json:"time"`
	LoadAvg              *[3]float64       `json:"loadavg_1_5_15,omitempty"`
	MemoryAvailableBytes *uint64           `json:"memory_available_bytes,omitempty"`
	PSI                  map[string]string `json:"psi,omitempty"`
	Cgroup               map[string]string `json:"cgroup,omitempty"`
	Warnings             []string          `json:"warnings,omitempty"`
}

func text(path string) string { b, _ := os.ReadFile(path); return strings.TrimSpace(string(b)) }
func field(s, key string) string {
	for _, line := range strings.Split(s, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == key {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
func memory(key string) *uint64 {
	f := strings.Fields(field(text("/proc/meminfo"), key))
	if len(f) == 0 {
		return nil
	}
	n, err := strconv.ParseUint(f[0], 10, 64)
	if err != nil {
		return nil
	}
	n *= 1024
	return &n
}

func Basics() Host {
	h := Host{OS: runtime.GOOS, Arch: runtime.GOARCH, LogicalCPUs: runtime.NumCPU(), GOMAXPROCS: runtime.GOMAXPROCS(0)}
	h.Hostname, _ = os.Hostname()
	h.Kernel = text("/proc/sys/kernel/osrelease")
	for _, line := range strings.Split(text("/etc/os-release"), "\n") {
		if strings.HasPrefix(line, "PRETTY_NAME=") {
			h.Distro = strings.Trim(strings.TrimPrefix(line, "PRETTY_NAME="), "\"")
		}
	}
	h.CPUModel = field(text("/proc/cpuinfo"), "model name")
	h.Affinity = field(text("/proc/self/status"), "Cpus_allowed_list")
	h.MemoryTotalBytes = memory("MemTotal")
	h.Limits = cgroup(true)
	if h.Kernel == "" || h.Affinity == "" || h.MemoryTotalBytes == nil || h.CPUModel == "" || h.Distro == "" {
		h.Warnings = append(h.Warnings, "some optional Linux host metadata unavailable")
	}
	if len(h.Limits) == 0 {
		h.Warnings = append(h.Warnings, "cgroup v2 limits unavailable")
	}
	return h
}

func Load() Snapshot {
	s := Snapshot{Time: time.Now().UTC(), MemoryAvailableBytes: memory("MemAvailable"), PSI: map[string]string{}, Cgroup: cgroup(false)}
	f := strings.Fields(text("/proc/loadavg"))
	if len(f) >= 3 {
		var a [3]float64
		valid := true
		for i := range a {
			var err error
			a[i], err = strconv.ParseFloat(f[i], 64)
			valid = valid && err == nil
		}
		if valid {
			s.LoadAvg = &a
		}
	}
	for _, k := range []string{"cpu", "memory", "io"} {
		if v := text("/proc/pressure/" + k); v != "" {
			s.PSI[k] = v
		}
	}
	if s.LoadAvg == nil || s.MemoryAvailableBytes == nil || len(s.PSI) != 3 {
		s.Warnings = append(s.Warnings, "some optional load/memory/PSI metadata unavailable")
	}
	if len(s.Cgroup) == 0 {
		s.Warnings = append(s.Warnings, "cgroup v2 metadata unavailable")
	}
	return s
}

func cgroup(limits bool) map[string]string {
	var dir string
	for _, line := range strings.Split(text("/proc/self/cgroup"), "\n") {
		if strings.HasPrefix(line, "0::") {
			dir = filepath.Join("/sys/fs/cgroup", strings.TrimPrefix(line, "0::"))
		}
	}
	if dir == "" {
		return nil
	}
	keys := []string{"cpu.stat", "memory.current", "memory.events"}
	if limits {
		keys = []string{"cpu.max", "memory.max", "memory.high", "cpuset.cpus.effective", "pids.max"}
	}
	m := map[string]string{}
	for _, k := range keys {
		if v := text(filepath.Join(dir, k)); v != "" {
			m[k] = v
		}
	}
	return m
}
