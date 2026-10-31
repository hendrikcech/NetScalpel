package main

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateOutputPathCollisions(t *testing.T) {
	dir := t.TempDir()
	for _, tc := range []struct {
		name string
		out  string
		log  string
		info string
	}{
		{
			name: "cleaned output aliases info",
			out:  dir + string(filepath.Separator) + "nested" + string(filepath.Separator) + ".." + string(filepath.Separator) + "result.csv",
			info: filepath.Join(dir, "result.csv"),
		},
		{
			name: "output aliases log",
			out:  filepath.Join(dir, "same"),
			log:  filepath.Join(dir, "same"),
		},
		{
			name: "info aliases log",
			log:  filepath.Join(dir, "same-info-log"),
			info: filepath.Join(dir, "same-info-log"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateOutputPathCollisions(tc.out, tc.log, tc.info); err == nil {
				t.Fatal("expected output collision")
			}
		})
	}
}

func TestValidateOutputPathCollisionsAllowsEmptyInfoAndDistinctPaths(t *testing.T) {
	if err := validateOutputPathCollisions("", "", ""); err != nil {
		t.Fatalf("empty paths should be allowed when CSV is written to stdout: %v", err)
	}
	if err := validateOutputPathCollisions("results.csv", "run.log", "results.csv.info.json"); err != nil {
		t.Fatalf("distinct output paths were rejected: %v", err)
	}
}

func TestValidateServerFlagsRejectsClientOnlyOptions(t *testing.T) {
	if err := validateServerFlags([]string{"--info=server.json"}); err == nil || !strings.Contains(err.Error(), "client-only") {
		t.Fatalf("validateServerFlags(--info) = %v", err)
	}
	if err := validateServerFlags(nil); err != nil {
		t.Fatalf("server defaults rejected: %v", err)
	}
	for _, args := range [][]string{{"--timestamp-tx=software"}, {"--timestamp-rx=hardware"}} {
		if err := validateServerFlags(args); err != nil {
			t.Fatalf("server timestamp config rejected: %v", err)
		}
	}
}
