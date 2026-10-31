package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

func TestTimestampConfigCLIParsesSourcePreferences(t *testing.T) {
	previous := cli
	defer func() { cli = previous }()

	cli.TimestampTx = "hw"
	cli.TimestampRx = "software"
	config, err := normalizedTimestampConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.TX != pkg.TimestampHardware || config.RX != pkg.TimestampSoftware {
		t.Fatalf("hardware preference config = %+v", config)
	}
}

func TestBatchTimestampPreferenceDoesNotPreflightProcedureCompatibility(t *testing.T) {
	root := t.TempDir()
	batchPath := filepath.Join(root, "batch.yaml")
	results := filepath.Join(root, "results")
	if err := os.WriteFile(batchPath, []byte("mode: sequential\nsteps: [{id: quic, procedure: simplequic, rounds: 1, params: {direction: ul}}]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &Client{
		IP: "127.0.0.1", Port: 0, Results: results,
		TimestampConfig: pkg.TimestampConfig{TX: pkg.TimestampHardware, RX: pkg.TimestampSoftware},
	}
	err := RunBatchFile(context.Background(), batchPath, results, client)
	if err == nil {
		t.Fatal("expected the closed RPC port to fail the batch")
	}
	if strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("batch failed on timestamp compatibility before running: %v", err)
	}
	entries, readErr := os.ReadDir(results)
	if readErr != nil || len(entries) != 1 || !entries[0].IsDir() {
		t.Fatalf("batch did not reach normal execution after accepting preferred source: entries=%v err=%v", entries, readErr)
	}
	summary, readErr := os.ReadFile(filepath.Join(results, entries[0].Name(), batchSummaryFileName))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(summary), "rpc dial") {
		t.Fatalf("batch summary did not record the expected runtime RPC failure: %s", summary)
	}
}

func TestBatchRejectsInvalidTimestampEnumBeforeOutputCreation(t *testing.T) {
	root := t.TempDir()
	batchPath := filepath.Join(root, "batch.yaml")
	results := filepath.Join(root, "results")
	if err := os.WriteFile(batchPath, []byte("mode: sequential\nsteps: [{id: one, procedure: trace, rounds: 1}]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &Client{TimestampConfig: pkg.TimestampConfig{TX: pkg.TimestampSource(255)}}
	err := RunBatchFile(context.Background(), batchPath, results, client)
	if err == nil || !strings.Contains(err.Error(), "invalid timestamp config") {
		t.Fatalf("invalid timestamp enum error = %v", err)
	}
	if _, err := os.Stat(results); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("batch created output despite invalid timestamp enum: %v", err)
	}
}

func TestClientRunRejectsInvalidTimestampEnumAtEntry(t *testing.T) {
	client := &Client{
		IP: "127.0.0.1", Port: 0, Rounds: 1, Procedure: "trace",
		TimestampConfig: pkg.TimestampConfig{TX: pkg.TimestampSource(255)},
	}
	err := client.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "invalid timestamp config") {
		t.Fatalf("invalid timestamp enum error = %v", err)
	}
	if strings.Contains(err.Error(), "rpc dial") || client.experimentID != "" {
		t.Fatalf("Run performed work before rejecting invalid timestamp enum: err=%v id=%q", err, client.experimentID)
	}
}

func TestClientRunDefersProtocolTimestampSelectionToRuntime(t *testing.T) {
	client := &Client{
		IP: "127.0.0.1", Port: 0, Rounds: 1, Procedure: "simplequic",
		Params:          experiment.ParamMap{"direction": "ul"},
		TimestampConfig: pkg.TimestampConfig{TX: pkg.TimestampSoftware, RX: pkg.TimestampSoftware},
	}
	err := client.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "rpc dial") {
		t.Fatalf("Run error = %v, want ordinary RPC failure after skipping timestamp preflight", err)
	}
	if strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("Run rejected protocol/source compatibility in cmd: %v", err)
	}
}

func TestExecutorCopiesRequestedTimestampConfigWithoutPreflight(t *testing.T) {
	config := pkg.TimestampConfig{TX: pkg.TimestampHardware, RX: pkg.TimestampSoftware}
	e := experiment.NewExecutor(context.Background(), "127.0.0.1", nil)
	e.DryRun = true
	e.TimestampConfig = &config
	sender := &pkg.SenderClient{Direction: pkg.UL, Sender: &pkg.ICMPSender{}}
	e.RunClient(sender)
	if sender.TimestampConfig != config {
		t.Fatalf("sender requested config = %+v, want %+v", sender.TimestampConfig, config)
	}
	if err := e.Wait(); err != nil {
		t.Fatalf("dry-run rejected timestamp request before runtime resolution: %v", err)
	}
}
