package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/hendrikcech/netscalpel/pkg"
	"go.yaml.in/yaml/v3"
)

func batchYAML(mode string) string {
	return fmt.Sprintf(`mode: %s
seed: 23
steps:
  - id: A
    procedure: burst
    rounds: 2
  - id: B
    procedure: rate
    rounds: 2
    params:
      direction: dl
      duration_ms: 1000
  - id: inner
    mode: shuffled
    steps:
      - id: C
        procedure: ratebidir
        rounds: 2
        params:
          duration_ms: 2000
      - id: D
        procedure: trace
        rounds: 1
`, mode)
}

func buildTestBatch(t *testing.T, mode string) *BatchPlan {
	t.Helper()
	plan, err := buildBatchPlan([]byte(batchYAML(mode)), "test-batch")
	if err != nil {
		t.Fatalf("build batch plan: %v", err)
	}
	return plan
}

func TestBatchPlanningModesAndDirectionExpansion(t *testing.T) {
	t.Run("sequential rounds and nested order", func(t *testing.T) {
		plan := buildTestBatch(t, "sequential")
		if len(plan.Execution) != 9 {
			t.Fatalf("got %d invocations, want 9", len(plan.Execution))
		}
		if direction, exists := plan.Entries[0].Parameters["direction"]; exists {
			t.Errorf("entry with omitted direction incorrectly reports direction=%v", direction)
		}
		if direction := plan.Entries[1].Parameters["direction"]; direction != "dl" {
			t.Errorf("entry lost explicit direction: got %v, want dl", direction)
		}
		wantFirst := []struct {
			entry string
			round uint
			call  uint
			dir   string
		}{
			{"A", 1, 1, "dl"}, {"A", 1, 2, "ul"},
			{"A", 2, 1, "dl"}, {"A", 2, 2, "ul"},
			{"B", 1, 1, "dl"}, {"B", 2, 1, "dl"},
		}
		for i, want := range wantFirst {
			got := plan.Execution[i]
			if got.EntryID != want.entry || got.Round != want.round || got.InvocationPosition != want.call {
				t.Errorf("execution[%d]=%+v; want entry=%s round=%d invocation=%d", i, got, want.entry, want.round, want.call)
			}
			if got.Parameters["direction"] != want.dir {
				t.Errorf("execution[%d] direction=%v, want %q", i, got.Parameters["direction"], want.dir)
			}
		}
		if plan.Execution[6].EntryID != "C" && plan.Execution[6].EntryID != "D" {
			t.Fatalf("nested block did not start after direct entries: %+v", plan.Execution[6])
		}
		for _, invocation := range plan.Execution[6:] {
			if invocation.EntryID != "C" && invocation.EntryID != "D" {
				t.Fatalf("nested block was not contiguous: %+v", plan.Execution)
			}
		}
	})

	t.Run("shuffled order is seed reproducible and block atomic", func(t *testing.T) {
		first := buildTestBatch(t, "shuffled")
		second := buildTestBatch(t, "shuffled")
		if !reflect.DeepEqual(first.Execution, second.Execution) {
			t.Fatalf("same seed produced different execution plans\n%+v\n%+v", first.Execution, second.Execution)
		}
		positions := make(map[string][]int)
		for i, invocation := range first.Execution {
			positions[invocation.EntryID] = append(positions[invocation.EntryID], i)
		}
		blockPositions := append(append([]int(nil), positions["C"]...), positions["D"]...)
		if len(blockPositions) != 3 {
			t.Fatalf("nested block invocations = %v", blockPositions)
		}
		min, max := blockPositions[0], blockPositions[0]
		for _, pos := range blockPositions[1:] {
			if pos < min {
				min = pos
			}
			if pos > max {
				max = pos
			}
		}
		if max-min != 2 {
			t.Fatalf("nested block was split by shuffled sibling: %v", blockPositions)
		}
	})

	t.Run("interleaving keeps rounds and nested blocks indivisible", func(t *testing.T) {
		plan := buildTestBatch(t, "interleaved")
		if len(plan.Execution) != 9 {
			t.Fatalf("got %d invocations, want 9", len(plan.Execution))
		}
		byEntryRound := make(map[string][]int)
		blockPositions := []int{}
		for i, invocation := range plan.Execution {
			if invocation.EntryID == "C" || invocation.EntryID == "D" {
				blockPositions = append(blockPositions, i)
			} else {
				key := fmt.Sprintf("%s/%d", invocation.EntryID, invocation.Round)
				byEntryRound[key] = append(byEntryRound[key], i)
			}
		}
		for key, positions := range byEntryRound {
			if len(positions) > 1 && positions[len(positions)-1]-positions[0] != len(positions)-1 {
				t.Errorf("procedure-round %s split across positions %v", key, positions)
			}
		}
		if len(blockPositions) != 3 {
			t.Fatalf("nested block positions = %v", blockPositions)
		}
		min, max := blockPositions[0], blockPositions[0]
		for _, pos := range blockPositions[1:] {
			if pos < min {
				min = pos
			}
			if pos > max {
				max = pos
			}
		}
		if max-min != 2 {
			t.Fatalf("nested block was not atomic: %v", blockPositions)
		}
	})
}

func TestBatchYAMLStrictValidation(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{"empty steps", "mode: sequential\nsteps: []\n", "non-empty"},
		{"unknown root field", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: 1}]\nextra: true\n", "unknown field"},
		{"unknown step field", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: 1, extra: true}]\n", "unknown field"},
		{"duplicate yaml key", "mode: sequential\nmode: shuffled\nsteps: [{id: x, procedure: trace, rounds: 1}]\n", "duplicate"},
		{"duplicate ID", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: 1}, {id: x, procedure: trace, rounds: 1}]\n", "duplicate batch ID"},
		{"multiple documents", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: 1}]\n---\nmode: sequential\nsteps: [{id: y, procedure: trace, rounds: 1}]\n", "exactly one document"},
		{"alias", "mode: sequential\nsteps: &steps [{id: x, procedure: trace, rounds: 1}]\nother: *steps\n", "aliases are not supported"},
		{"merge", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: 1}]\n<<: {foo: bar}\n", "merge"},
		{"quoted rounds", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: '1'}]\n", "native YAML integer"},
		{"explicit coercion", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: !!int '1'}]\n", "native YAML integer"},
		{"null rounds", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: null}]\n", "native YAML integer"},
		{"fractional rounds", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: 1.5}]\n", "native YAML integer"},
		{"zero rounds", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: 0}]\n", "between 1"},
		{"negative rounds", "mode: sequential\nsteps: [{id: x, procedure: trace, rounds: -1}]\n", "between 1"},
		{"unknown param", "mode: sequential\nsteps: [{id: x, procedure: rate, rounds: 1, params: {missing: 1}}]\n", "no parameter"},
		{"parameter type", "mode: sequential\nsteps: [{id: x, procedure: rate, rounds: 1, params: {duration_ms: '1000'}}]\n", "unsigned integer"},
		{"null parameter", "mode: sequential\nsteps: [{id: x, procedure: rate, rounds: 1, params: {duration_ms: null}}]\n", "unsigned integer"},
		{"invalid schedule constraint", "mode: sequential\nsteps: [{id: x, procedure: cdsf, rounds: 1, params: {max: 0}}]\n", "schedule panicked"},
		{"duplicate nested ID", "mode: sequential\nsteps: [{id: parent, mode: sequential, steps: [{id: child, procedure: trace, rounds: 1}]}, {id: child, procedure: trace, rounds: 1}]\n", "duplicate batch ID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildBatchPlan([]byte(tc.yaml), "test-batch")
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("error=%v; want substring %q", err, tc.want)
			}
		})
	}
}

func TestBatchBooleanParams(t *testing.T) {
	for _, value := range []bool{true, false} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(fmt.Sprint(value)), &node); err != nil {
				t.Fatal(err)
			}
			got, err := parseBatchParamValue(node.Content[0], false, "single_config")
			if err != nil {
				t.Fatal(err)
			}
			if got != value {
				t.Errorf("single_config = %#v, want %t", got, value)
			}
		})
	}
	for _, value := range []string{"'true'", "1", "null", "[true]", "{value: true}", "yes", "!!bool 'true'"} {
		t.Run("invalid "+value, func(t *testing.T) {
			var node yaml.Node
			if err := yaml.Unmarshal([]byte(value), &node); err != nil {
				t.Fatal(err)
			}
			_, err := parseBatchParamValue(node.Content[0], false, "single_config")
			if err == nil || !strings.Contains(err.Error(), "single_config must be a native YAML boolean") {
				t.Fatalf("error = %v; want native YAML boolean error", err)
			}
		})
	}
}

func TestBatchNativeYAMLParamsResolveWithoutSharing(t *testing.T) {
	yaml := `mode: sequential
seed: -7
steps:
  - id: durations
    procedure: multidurrate
    rounds: 1
    params:
      durations: [1000, 2000]
      ratesUL: [140, 200]
  - id: ccas
    procedure: tcpri
    rounds: 1
    params:
      ccas: ["cubic", "bbr1"]
`
	plan, err := buildBatchPlan([]byte(yaml), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Seed != -7 {
		t.Fatalf("seed=%d, want -7", plan.Seed)
	}
	if got := plan.Entries[0].Parameters["durations"]; !reflect.DeepEqual(got, []uint{1000, 2000}) {
		t.Errorf("resolved duration list = %#v", got)
	}
	if got := plan.Entries[1].Parameters["ccas"]; !reflect.DeepEqual(got, []string{"cubic", "bbr1"}) {
		t.Errorf("resolved CCA names = %#v", got)
	}
	first := plan.Execution[0].Parameters
	first["durations"].([]uint)[0] = 77
	if plan.Execution[1].Parameters["durations"].([]uint)[0] == 77 {
		t.Fatal("planned rounds share mutable parameter slices")
	}
}

func TestBatchPlanLimitsAndAncestry(t *testing.T) {
	tooDeep := "mode: sequential\nsteps:\n"
	for i := 0; i < MaxBatchDepth; i++ {
		indent := 2 + i*4
		tooDeep += strings.Repeat(" ", indent) + "- id: b" + fmt.Sprint(i) + "\n"
		tooDeep += strings.Repeat(" ", indent+2) + "mode: sequential\n"
		tooDeep += strings.Repeat(" ", indent+2) + "steps:\n"
	}
	leafIndent := 2 + MaxBatchDepth*4
	tooDeep += strings.Repeat(" ", leafIndent) + "- id: leaf\n"
	tooDeep += strings.Repeat(" ", leafIndent+2) + "procedure: trace\n"
	tooDeep += strings.Repeat(" ", leafIndent+2) + "rounds: 1\n"
	if _, err := buildBatchPlan([]byte(tooDeep), "test-batch"); err == nil || !strings.Contains(err.Error(), "maximum depth") {
		t.Fatalf("expected depth limit error, got %v", err)
	}
	tooMany := fmt.Sprintf("mode: sequential\nsteps: [{id: many, procedure: trace, rounds: %d}]\n", MaxPlannedInvocations+1)
	if _, err := buildBatchPlan([]byte(tooMany), "test-batch"); err == nil || !strings.Contains(err.Error(), "between 1") {
		t.Fatalf("expected invocation limit error, got %v", err)
	}
}

func TestBatchRunnerContinuesOrdinaryFailuresAndAggregatesRoundPrecedence(t *testing.T) {
	plan, err := buildBatchPlan([]byte(`mode: sequential
seed: 1
steps:
  - id: directions
    procedure: burst
    rounds: 1
  - id: later
    procedure: ratebidir
    rounds: 1
`), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	var saved []*BatchSummary
	summary, err := executeBatchPlan(context.Background(), plan, func(_ context.Context, invocation BatchPlannedInvocation) BatchInvocationOutcome {
		calls++
		if invocation.EntryID == "directions" && invocation.InvocationPosition == 1 {
			return BatchInvocationOutcome{Err: errors.New("confirmed test failure"), RemoteConfirmed: true}
		}
		return BatchInvocationOutcome{RemoteConfirmed: true}
	}, func(s *BatchSummary) error {
		copySummary := *s
		saved = append(saved, &copySummary)
		return nil
	})
	if err == nil {
		t.Fatal("expected a failed batch result")
	}
	if calls != 3 {
		t.Fatalf("runner made %d calls; want all 3 invocations", calls)
	}
	if summary.Entries[0].Rounds[0].Status != "failed" {
		t.Fatalf("partially failed round status=%q, want failed", summary.Entries[0].Rounds[0].Status)
	}
	if summary.Entries[1].Rounds[0].Status != "success" {
		t.Fatalf("later round status=%q, want success", summary.Entries[1].Rounds[0].Status)
	}
	if summary.Counts.Failed != 1 || summary.Counts.Successful != 1 || summary.InvocationCounts.Failed != 1 || summary.InvocationCounts.Successful != 2 {
		t.Fatalf("unexpected round/invocation totals: rounds=%+v invocations=%+v", summary.Counts, summary.InvocationCounts)
	}
	if len(saved) == 0 || saved[len(saved)-1].FinishedAt == nil {
		t.Fatal("final summary was not saved")
	}
}

func TestBatchRunnerCancellationAndFatalStops(t *testing.T) {
	plan := buildTestBatch(t, "sequential")
	t.Run("cancellation marks active and leaves future not run", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		summary, err := executeBatchPlan(ctx, plan, func(_ context.Context, _ BatchPlannedInvocation) BatchInvocationOutcome {
			calls++
			cancel()
			return BatchInvocationOutcome{Err: context.Canceled, Cancelled: true}
		}, func(*BatchSummary) error { return nil })
		if err == nil {
			t.Fatal("expected cancellation result")
		}
		if calls != 1 || summary.Invocations[0].Status != "cancelled" || summary.Invocations[1].Status != "not_run" {
			t.Fatalf("calls=%d statuses=%q,%q", calls, summary.Invocations[0].Status, summary.Invocations[1].Status)
		}
	})
	t.Run("fatal persistence failure stops without retry", func(t *testing.T) {
		calls := 0
		summary, err := executeBatchPlan(context.Background(), plan, func(_ context.Context, _ BatchPlannedInvocation) BatchInvocationOutcome {
			calls++
			return BatchInvocationOutcome{Err: &pkg.ResultPersistenceError{Err: errors.New("disk full")}, Fatal: true, RemoteConfirmed: true}
		}, func(*BatchSummary) error { return nil })
		if err == nil {
			t.Fatal("expected fatal persistence result")
		}
		if calls != 1 || summary.Invocations[0].Status != "failed" || summary.Invocations[1].Status != "not_run" {
			t.Fatalf("calls=%d statuses=%q,%q", calls, summary.Invocations[0].Status, summary.Invocations[1].Status)
		}
	})
	t.Run("failure outranks cancellation in a partially completed round", func(t *testing.T) {
		calls := 0
		summary, _ := executeBatchPlan(context.Background(), plan, func(_ context.Context, invocation BatchPlannedInvocation) BatchInvocationOutcome {
			calls++
			if invocation.EntryID == "A" && invocation.InvocationPosition == 1 {
				return BatchInvocationOutcome{Err: errors.New("failed direction"), RemoteConfirmed: true}
			}
			if invocation.EntryID == "A" && invocation.InvocationPosition == 2 {
				return BatchInvocationOutcome{Err: context.Canceled, Cancelled: true}
			}
			return BatchInvocationOutcome{RemoteConfirmed: true}
		}, func(*BatchSummary) error { return nil })
		if calls != 2 || summary.Entries[0].Rounds[0].Status != "failed" {
			t.Fatalf("calls=%d round status=%q", calls, summary.Entries[0].Rounds[0].Status)
		}
		if summary.InvocationCounts.Failed != 1 || summary.InvocationCounts.Cancelled != 1 || summary.InvocationCounts.NotRun != uint(len(plan.Execution)-2) {
			t.Fatalf("unexpected invocation totals: %+v", summary.InvocationCounts)
		}
	})
	t.Run("unconfirmed remote work stops", func(t *testing.T) {
		calls := 0
		_, err := executeBatchPlan(context.Background(), plan, func(_ context.Context, _ BatchPlannedInvocation) BatchInvocationOutcome {
			calls++
			return BatchInvocationOutcome{Err: errors.New("gather transport failure"), RemoteConfirmed: false}
		}, func(*BatchSummary) error { return nil })
		if err == nil || calls != 1 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})
}

func TestBatchValidationDoesNotCreateResultsOrDial(t *testing.T) {
	root := t.TempDir()
	batchPath := root + "/invalid.yaml"
	if err := os.WriteFile(batchPath, []byte(`mode: sequential
steps: [{id: bad, procedure: cdsf, rounds: 1, params: {max: 0}}]
`), 0600); err != nil {
		t.Fatal(err)
	}
	results := root + "/results-must-not-exist"
	client := &Client{IP: "127.0.0.1", Port: 1, Results: results}
	err := RunBatchFile(context.Background(), batchPath, results, client)
	if err == nil || !strings.Contains(err.Error(), "schedule panicked") {
		t.Fatalf("expected pre-run schedule validation error, got %v", err)
	}
	if _, err := os.Stat(results); !os.IsNotExist(err) {
		t.Fatalf("results directory created before full validation: %v", err)
	}
}

func TestBatchStartModeValidation(t *testing.T) {
	for _, tc := range []struct {
		mode StartMode
		want StartMode
	}{
		{"", StartModeStarlink},
		{"STARLINK", StartModeStarlink},
		{"Immediate", StartModeImmediate},
		{"invalid", ""},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			root := t.TempDir()
			batchPath := filepath.Join(root, "batch.yaml")
			results := filepath.Join(root, "results")
			if err := os.WriteFile(batchPath, []byte("mode: sequential\nsteps: [{id: one, procedure: trace, rounds: 1}]\n"), 0600); err != nil {
				t.Fatal(err)
			}
			client := &Client{IP: "127.0.0.1", Port: 1, Results: results, StartMode: tc.mode}
			err := RunBatchFile(context.Background(), batchPath, results, client)
			if tc.want == "" {
				if err == nil || !strings.Contains(err.Error(), "unknown start mode") {
					t.Fatalf("expected start-mode validation error, got %v", err)
				}
				if _, err := os.Stat(results); !os.IsNotExist(err) {
					t.Fatalf("results created despite invalid start mode: %v", err)
				}
				return
			}
			if client.StartMode != tc.want {
				t.Fatalf("start mode = %q, want %q", client.StartMode, tc.want)
			}
			if err == nil {
				t.Fatal("expected the closed RPC port to fail the batch")
			}
			dirs, err := os.ReadDir(results)
			if err != nil || len(dirs) != 1 {
				t.Fatalf("batch results: dirs=%v err=%v", dirs, err)
			}
			summary, err := os.ReadFile(filepath.Join(results, dirs[0].Name(), batchSummaryFileName))
			if err != nil || !strings.Contains(string(summary), "rpc dial") {
				t.Fatalf("expected RPC dial failure in summary: %s, %v", summary, err)
			}
		})
	}
}

func TestRunBatchFileKeepsRunsInExclusiveBatchDirectories(t *testing.T) {
	root := t.TempDir()
	batchPath := filepath.Join(root, "batch.yaml")
	results := filepath.Join(root, "results")
	if err := os.WriteFile(batchPath, []byte("mode: sequential\nsteps: [{id: one, procedure: trace, rounds: 1}]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	client := &Client{IP: "127.0.0.1", Port: 1, Results: results}
	originalStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	defer func() {
		os.Stdout = originalStdout
		_ = writer.Close()
		_ = reader.Close()
	}()
	for run := 0; run < 2; run++ {
		if err := RunBatchFile(context.Background(), batchPath, results, client); err == nil {
			t.Fatalf("run %d unexpectedly succeeded with a closed RPC port", run+1)
		}
		if client.Results != results {
			t.Fatalf("run %d left client.Results=%q, want restored %q", run+1, client.Results, results)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	os.Stdout = originalStdout
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	dirs, err := os.ReadDir(results)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirs) != 2 {
		t.Fatalf("results root has %d entries, want 2 batch directories", len(dirs))
	}
	seen := make(map[string]bool)
	for _, dir := range dirs {
		if !dir.IsDir() {
			t.Fatalf("results root entry %q is not a batch directory", dir.Name())
		}
		if seen[dir.Name()] {
			t.Fatalf("batch directory %q was reused", dir.Name())
		}
		seen[dir.Name()] = true
		batchDir := filepath.Join(results, dir.Name())
		var plan BatchPlan
		planData, err := os.ReadFile(filepath.Join(batchDir, batchPlanFileName))
		if err != nil {
			t.Fatalf("read plan in %q: %v", batchDir, err)
		}
		if err := json.Unmarshal(planData, &plan); err != nil {
			t.Fatalf("decode plan in %q: %v", batchDir, err)
		}
		var summary BatchSummary
		summaryData, err := os.ReadFile(filepath.Join(batchDir, batchSummaryFileName))
		if err != nil {
			t.Fatalf("read summary in %q: %v", batchDir, err)
		}
		if err := json.Unmarshal(summaryData, &summary); err != nil {
			t.Fatalf("decode summary in %q: %v", batchDir, err)
		}
		if plan.BatchID != dir.Name() || summary.BatchID != dir.Name() {
			t.Errorf("directory %q contains plan/summary IDs %q/%q", dir.Name(), plan.BatchID, summary.BatchID)
		}
		if !strings.Contains(string(output), "Batch results: "+batchDir) {
			t.Errorf("output did not report actual results directory %q:\n%s", batchDir, output)
		}
	}
}

func TestCreateBatchResultsDirRefusesExistingBatchDirectory(t *testing.T) {
	root := t.TempDir()
	existing := filepath.Join(root, "existing-batch")
	if err := os.Mkdir(existing, 0755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(existing, batchPlanFileName)
	if err := os.WriteFile(marker, []byte("keep this metadata"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := createBatchResultsDir(root, "existing-batch"); err == nil {
		t.Fatal("existing batch directory was accepted")
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "keep this metadata" {
		t.Fatalf("existing batch metadata changed to %q", data)
	}
}

func TestRefreshBatchSummaryAggregatesNestedBlocksInOneHierarchy(t *testing.T) {
	plan, err := buildBatchPlan([]byte(`mode: sequential
steps:
  - id: outer
    mode: sequential
    steps:
      - id: direct
        procedure: burst
        rounds: 1
      - id: inner
        mode: sequential
        steps:
          - id: nested
            procedure: trace
            rounds: 1
  - id: outside
    procedure: trace
    rounds: 1
`), "nested-counts")
	if err != nil {
		t.Fatal(err)
	}
	summary := newBatchSummary(plan)
	for i := range summary.Invocations {
		switch summary.Invocations[i].EntryID {
		case "direct":
			if summary.Invocations[i].InvocationPosition == 1 {
				summary.Invocations[i].Status = "success"
			} else {
				summary.Invocations[i].Status = "failed"
			}
		case "nested":
			summary.Invocations[i].Status = "cancelled"
		case "outside":
			summary.Invocations[i].Status = "not_run"
		}
	}
	finished := summary.StartedAt
	summary.FinishedAt = &finished
	refreshBatchSummary(summary, plan)

	if got := summary.Entries[0].Rounds[0].Status; got != "failed" {
		t.Errorf("partially failed round status=%q, want failed", got)
	}
	if got := summary.Entries[0].InvocationCounts; got != (BatchCounts{Successful: 1, Failed: 1}) {
		t.Errorf("direct invocation counts=%+v", got)
	}
	if got := summary.Blocks[0].Counts; got != (BatchCounts{Failed: 1, Cancelled: 1}) {
		t.Errorf("outer block round counts=%+v", got)
	}
	if got := summary.Blocks[0].InvocationCounts; got != (BatchCounts{Successful: 1, Failed: 1, Cancelled: 1}) {
		t.Errorf("outer block invocation counts=%+v", got)
	}
	if got := summary.Blocks[1].Counts; got != (BatchCounts{Cancelled: 1}) {
		t.Errorf("inner block round counts=%+v", got)
	}
	if got := summary.Blocks[1].InvocationCounts; got != (BatchCounts{Cancelled: 1}) {
		t.Errorf("inner block invocation counts=%+v", got)
	}
	if got := summary.Counts; got != (BatchCounts{Failed: 1, Cancelled: 1, NotRun: 1}) {
		t.Errorf("summary round counts=%+v", got)
	}
	if got := summary.InvocationCounts; got != (BatchCounts{Successful: 1, Failed: 1, Cancelled: 1, NotRun: 1}) {
		t.Errorf("summary invocation counts=%+v", got)
	}
}

func TestBatchRunnerPersistenceFailureIsFatalAndAttemptsFinalSave(t *testing.T) {
	plan := buildTestBatch(t, "sequential")
	saves := 0
	calls := 0
	summary, err := executeBatchPlan(context.Background(), plan, func(_ context.Context, _ BatchPlannedInvocation) BatchInvocationOutcome {
		calls++
		return BatchInvocationOutcome{RemoteConfirmed: true}
	}, func(*BatchSummary) error {
		saves++
		if saves == 2 {
			return errors.New("summary disk error")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected persistence failure")
	}
	if calls != 0 {
		t.Fatalf("measurement started after pre-run summary failure: %d calls", calls)
	}
	if saves < 3 {
		t.Fatalf("expected best-effort final save after fatal persistence failure; saves=%d", saves)
	}
	if summary.FinishedAt == nil || len(summary.Errors) == 0 {
		t.Fatalf("failure summary incomplete: %+v", summary)
	}
}

func TestFlagWasSetIncludesExplicitDefaultValues(t *testing.T) {
	for _, args := range [][]string{{"client", "--rounds", "1"}, {"client", "--rounds=1"}, {"client", "--params="}} {
		name := "rounds"
		if strings.Contains(args[len(args)-1], "params") {
			name = "params"
		}
		if !flagWasSet(args, name) {
			t.Errorf("explicit %s not detected in %v", name, args)
		}
	}
}
