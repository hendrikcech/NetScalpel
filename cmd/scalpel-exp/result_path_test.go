package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
)

func TestBatchInvocationPathKeepsIdentityInManifest(t *testing.T) {
	ip, port := failureRPC(t)
	ctx := context.Background()
	rpcClient, err := dialRpcClient(ctx, ip, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()
	c := &Client{IP: ip, Port: port, Results: t.TempDir(), Rounds: 2}
	defer func() {
		if c.slogFile != nil {
			c.slogFile.Close()
		}
	}()
	proc := procedures.Procedure{
		Name: "rate", Mode: procedures.PerDirection,
		Run: func(*experiment.Executor, time.Time, string, experiment.ParamMap) error { return nil },
	}
	ts := time.Now().Add(time.Hour)
	name := experiment.NextRI(ts).Format("20060102T150405") + "_rate_dl"
	for position := uint(1); position <= 2; position++ {
		c.round = position - 1
		c.batchInvocation = &BatchInvocationMetadata{
			BatchID: "test-batch", EntryID: "rate-entry", Ancestry: []string{"load"},
			ExecutionPosition: position, Round: position, InvocationPosition: 1,
		}
		e := experiment.NewExecutor(ctx, ip, rpcClient)
		path, err := c.scheduleProcedureInvocation(ctx, e, ts, proc, experiment.ParamMap{"direction": "dl"})
		if err != nil {
			t.Fatal(err)
		}
		wantName := name
		if position == 2 {
			wantName += "_1"
		}
		if filepath.Base(path) != wantName {
			t.Fatalf("invocation path=%s, want %s", path, wantName)
		}
		if err := c.finishProcedureInvocation(ctx, e, rpcClient, path); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(path, "info.json"))
		if err != nil {
			t.Fatal(err)
		}
		var manifest experiment.Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.BatchID != "test-batch" || manifest.BatchEntryID != "rate-entry" ||
			manifest.BatchPosition != position || manifest.BatchRound != position || manifest.BatchInvoke != 1 ||
			len(manifest.BatchAncestry) != 1 || manifest.BatchAncestry[0] != "load" {
			t.Fatalf("batch identity lost from manifest: %+v", manifest)
		}
	}
}

func TestResultPathAddsSuffixOnlyOnCollision(t *testing.T) {
	base := filepath.Join(t.TempDir(), "results", "batch-id")
	ts := time.Date(2026, 10, 23, 13, 18, 12, 0, time.UTC)
	name := "20261023T131812_rate_dl"
	first, err := mkResultPath(base, ts, "_rate_dl")
	if err != nil {
		t.Fatal(err)
	}
	if first != filepath.Join(base, name) {
		t.Fatalf("unexpected first path: %s", first)
	}
	sentinel := filepath.Join(first, "info.json")
	if err := os.WriteFile(sentinel, []byte("existing results"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := mkResultPath(base, ts, "_rate_dl")
	if err != nil {
		t.Fatal(err)
	}
	if second != filepath.Join(base, name+"_1") {
		t.Fatalf("unexpected collision path: %s", second)
	}
	if err := os.WriteFile(filepath.Join(base, name+"_2"), []byte("existing file"), 0600); err != nil {
		t.Fatal(err)
	}
	third, err := mkResultPath(base, ts, "_rate_dl")
	if err != nil {
		t.Fatal(err)
	}
	if third != filepath.Join(base, name+"_3") {
		t.Fatalf("did not skip occupied directory and file names: %s", third)
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "existing results" {
		t.Fatalf("existing results were changed: %q, %v", data, err)
	}
}

func TestResultPathConcurrentCreationIsExclusive(t *testing.T) {
	base := filepath.Join(t.TempDir(), "results")
	ts := time.Date(2026, 10, 23, 13, 18, 12, 0, time.UTC)
	type outcome struct {
		path string
		err  error
	}
	const callers = 32
	results := make(chan outcome, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			path, err := mkResultPath(base, ts, "_rate_dl")
			results <- outcome{path, err}
		})
	}
	wg.Wait()
	close(results)
	seen := make(map[string]bool, callers)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if seen[result.path] {
			t.Fatalf("directory was reused: %s", result.path)
		}
		seen[result.path] = true
	}
	if !seen[filepath.Join(base, "20261023T131812_rate_dl")] {
		t.Fatal("normal path was not used")
	}
}

func TestResultPathCreationErrors(t *testing.T) {
	base := t.TempDir()
	file := filepath.Join(base, "file")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		base   string
		suffix string
	}{
		{"parent is a file", file, "_rate_dl"},
		{"invalid directory name", base, strings.Repeat("x", 256)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, err := mkResultPath(tc.base, time.Now(), tc.suffix)
			if err == nil || path != "" {
				t.Fatalf("path=%q error=%v, want creation failure", path, err)
			}
		})
	}
}
