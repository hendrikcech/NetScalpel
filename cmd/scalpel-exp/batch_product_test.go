package main

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
	"go.yaml.in/yaml/v3"
)

func productBatchYAML(mode, steps string) string {
	return fmt.Sprintf("mode: %s\nseed: 23\nsteps:\n%s", mode, steps)
}

func TestBatchProductMatchesExplicitEntriesInEveryMode(t *testing.T) {
	productSteps := `  - id: "run-{duration_ms}"
    procedure: rate
    rounds: 2
    product:
      duration_ms: [1000, 2000]
`
	explicitSteps := `  - id: run-1000
    procedure: rate
    rounds: 2
    params: {duration_ms: 1000}
  - id: run-2000
    procedure: rate
    rounds: 2
    params: {duration_ms: 2000}
`
	for _, mode := range []string{"sequential", "shuffled", "interleaved"} {
		t.Run(mode, func(t *testing.T) {
			productPlan, err := buildBatchPlan([]byte(productBatchYAML(mode, productSteps)), "test-batch")
			if err != nil {
				t.Fatalf("build product plan: %v", err)
			}
			explicitPlan, err := buildBatchPlan([]byte(productBatchYAML(mode, explicitSteps)), "test-batch")
			if err != nil {
				t.Fatalf("build explicit plan: %v", err)
			}
			if !reflect.DeepEqual(productPlan.Entries, explicitPlan.Entries) {
				t.Errorf("generated entries differ from explicit entries\nproduct:  %+v\nexplicit: %+v", productPlan.Entries, explicitPlan.Entries)
			}
			if !reflect.DeepEqual(productPlan.Execution, explicitPlan.Execution) {
				t.Errorf("generated execution differs from explicit entries\nproduct:  %+v\nexplicit: %+v", productPlan.Execution, explicitPlan.Execution)
			}
			if got, want := len(productPlan.Execution), 8; got != want {
				t.Fatalf("got %d invocations; want %d (2 configs x 2 rounds x 2 directions)", got, want)
			}
			for _, entry := range productPlan.Entries {
				if entry.Rounds != 2 || entry.InvocationsPerRound != 2 {
					t.Errorf("entry has rounds/invocations %d/%d; want 2/2", entry.Rounds, entry.InvocationsPerRound)
				}
			}
			for _, invocation := range productPlan.Execution {
				if invocation.Parameters["direction"] != "dl" && invocation.Parameters["direction"] != "ul" {
					t.Errorf("missing direction expansion in invocation: %+v", invocation)
				}
			}
		})
	}
}

func TestBatchProductAxesSortAndPreserveCandidateOrder(t *testing.T) {
	plan, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "d-{durations}-r-{ratesUL}"
    procedure: multidurrate
    rounds: 1
    product:
      ratesUL: [100, 200]
      durations: [1000, 2000]
`)), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"d-1000-r-100", "d-1000-r-200", "d-2000-r-100", "d-2000-r-200"}
	for i, id := range want {
		if plan.Entries[i].ID != id {
			t.Errorf("entries[%d].ID=%q; want %q", i, plan.Entries[i].ID, id)
		}
	}
}

func TestBatchProductDirectionAxisUsesPerConfigurationExpansion(t *testing.T) {
	plan, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "r-{duration_ms}-{direction}"
    procedure: rate
    rounds: 2
    product:
      duration_ms: [1000, 2000]
      direction: [DL, UL]
`)), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Entries) != 4 || len(plan.Execution) != 8 {
		t.Fatalf("entries/invocations=%d/%d; want 4/8", len(plan.Entries), len(plan.Execution))
	}
	wantIDs := []string{"r-1000-DL", "r-2000-DL", "r-1000-UL", "r-2000-UL"}
	for i, want := range wantIDs {
		if plan.Entries[i].ID != want {
			t.Fatalf("entry IDs=%v; want %v", []string{plan.Entries[0].ID, plan.Entries[1].ID, plan.Entries[2].ID, plan.Entries[3].ID}, wantIDs)
		}
	}
	for _, entry := range plan.Entries {
		if entry.InvocationsPerRound != 1 {
			t.Errorf("explicit product direction expanded %d calls per round; want 1", entry.InvocationsPerRound)
		}
	}
	for _, invocation := range plan.Execution {
		if invocation.Parameters["direction"] != "dl" && invocation.Parameters["direction"] != "ul" {
			t.Errorf("direction was not canonically resolved: %+v", invocation)
		}
	}
}

func TestBatchProductNestedAncestryAndAtomicity(t *testing.T) {
	plan, err := buildBatchPlan([]byte(productBatchYAML("shuffled", `  - id: group
    mode: shuffled
    steps:
      - id: "r-{duration_ms}"
        procedure: ratebidir
        rounds: 2
        product:
          duration_ms: [1000, 2000]
      - id: nested-trace
        procedure: trace
        rounds: 1
  - id: outside
    procedure: trace
    rounds: 1
`)), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Blocks) != 1 || plan.Blocks[0].ID != "group" {
		t.Fatalf("unexpected blocks: %+v", plan.Blocks)
	}
	productPositions := make([]int, 0, 4)
	for i, invocation := range plan.Execution {
		if invocation.EntryID == "r-1000" || invocation.EntryID == "r-2000" || invocation.EntryID == "nested-trace" {
			productPositions = append(productPositions, i)
			if !reflect.DeepEqual(invocation.Ancestry, []string{"group"}) {
				t.Errorf("entry %q ancestry=%v; want [group]", invocation.EntryID, invocation.Ancestry)
			}
		}
	}
	if len(productPositions) != 5 {
		t.Fatalf("nested block invocation positions=%v; want 5 invocations", productPositions)
	}
	for i := 1; i < len(productPositions); i++ {
		if productPositions[i] != productPositions[i-1]+1 {
			t.Fatalf("nested block split by outside sibling: positions %v", productPositions)
		}
	}
	for _, entry := range plan.Entries {
		if entry.ID == "r-1000" || entry.ID == "r-2000" {
			if !reflect.DeepEqual(entry.Ancestry, []string{"group"}) {
				t.Errorf("planned entry %q ancestry=%v; want [group]", entry.ID, entry.Ancestry)
			}
		}
	}
}

func TestBatchProductListCandidatesAndIndependentParams(t *testing.T) {
	plan, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "rate-{ratesUL}"
    procedure: multidurrate
    rounds: 1
    product:
      durations: [[1000, 2000]]
      ratesUL: [100, 200]
`)), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.root.Children) != 2 {
		t.Fatalf("generated %d entries; want 2", len(plan.root.Children))
	}
	first := plan.root.Children[0].Params["durations"].([]uint)
	second := plan.root.Children[1].Params["durations"].([]uint)
	first[0] = 99
	if second[0] != 1000 {
		t.Fatalf("product leaves share mutable list candidates: second=%v", second)
	}
	firstInvocation := plan.Execution[0].Parameters["durations"].([]uint)
	secondInvocation := plan.Execution[2].Parameters["durations"].([]uint)
	firstInvocation[0] = 88
	if secondInvocation[0] != 1000 {
		t.Fatalf("planned invocations share mutable list parameters: second=%v", secondInvocation)
	}
}

func TestBatchProductListParameterScalarPlaceholder(t *testing.T) {
	plan, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "duration-{durations}"
    procedure: multidurrate
    rounds: 1
    product:
      durations: [1000, 2000]
`)), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{plan.Entries[0].ID, plan.Entries[1].ID}; !reflect.DeepEqual(got, []string{"duration-1000", "duration-2000"}) {
		t.Fatalf("IDs=%v", got)
	}
	if got := plan.Entries[0].Parameters["durations"]; !reflect.DeepEqual(got, []uint{1000}) {
		t.Fatalf("scalar list parameter resolved to %#v; want []uint{1000}", got)
	}
}

func TestBatchProductStringAndCanonicalScalarIDs(t *testing.T) {
	plan, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "cca-{ccas}"
    procedure: tcpri
    rounds: 1
    product:
      ccas: ["cubic", "bbr1"]
`)), "test-batch")
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{plan.Entries[0].ID, plan.Entries[1].ID}; !reflect.DeepEqual(got, []string{"cca-cubic", "cca-bbr1"}) {
		t.Fatalf("string IDs=%v", got)
	}

	var node yaml.Node
	if err := yaml.Unmarshal([]byte("{feature: [true, false]}"), &node); err != nil {
		t.Fatal(err)
	}
	proc := procedures.Procedure{Name: "test", Mode: procedures.OncePerRound, Params: []procedures.ParamSpec{{Name: "feature", Default: false}}}
	product, err := parseBatchProduct(node.Content[0], proc, "flag-{feature}", experiment.ParamMap{}, "test product")
	if err != nil {
		t.Fatal(err)
	}
	entry := &batchNode{ID: "template", Procedure: "test", Rounds: 1, Params: experiment.ParamMap{}, Ancestry: []string{"template"}, Product: product}
	leaves, err := expandBatchProduct(entry)
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{leaves[0].ID, leaves[1].ID}; !reflect.DeepEqual(got, []string{"flag-true", "flag-false"}) {
		t.Fatalf("boolean IDs=%v", got)
	}

	if err := yaml.Unmarshal([]byte("{duration_ms: [0x10]}"), &node); err != nil {
		t.Fatal(err)
	}
	proc = procedures.Procedure{Name: "test", Mode: procedures.OncePerRound, Params: []procedures.ParamSpec{{Name: "duration_ms", Default: uint(1)}}}
	product, err = parseBatchProduct(node.Content[0], proc, "run-{duration_ms}", experiment.ParamMap{}, "test product")
	if err != nil {
		t.Fatal(err)
	}
	entry.Product = product
	leaves, err = expandBatchProduct(entry)
	if err != nil {
		t.Fatal(err)
	}
	if leaves[0].ID != "run-16" {
		t.Fatalf("numeric ID=%q; want canonical run-16", leaves[0].ID)
	}
}

func TestBatchProductTemplateValidation(t *testing.T) {
	cases := []struct {
		name  string
		step  string
		match string
	}{
		{"empty product", `  - id: x
    procedure: rate
    rounds: 1
    product: {}`, "at least one axis"},
		{"product must be mapping", `  - id: x
    procedure: rate
    rounds: 1
    product: []`, "YAML mapping"},
		{"axis must be sequence", `  - id: "x-{duration_ms}"
    procedure: rate
    rounds: 1
    product: {duration_ms: 1000}`, "non-empty YAML sequence"},
		{"empty axis", `  - id: "x-{duration_ms}"
    procedure: rate
    rounds: 1
    product: {duration_ms: []}`, "at least one candidate"},
		{"unknown parameter", `  - id: "x-{missing}"
    procedure: rate
    rounds: 1
    product: {missing: [1]}`, "no parameter"},
		{"overlapping parameters", `  - id: "x-{duration_ms}"
    procedure: rate
    rounds: 1
    params: {duration_ms: 1000}
    product: {duration_ms: [2000]}`, "both params and product"},
		{"unsupported direction", `  - id: "x-{direction}"
    procedure: trace
    rounds: 1
    product: {direction: [ul]}`, "does not support the direction"},
		{"product only on procedure", `  - id: parent
    mode: sequential
    product: {duration_ms: [1000]}
    steps:
      - id: leaf
        procedure: trace
        rounds: 1`, "only allowed on procedure entries"},
		{"no placeholder", `  - id: fixed
    procedure: rate
    rounds: 1
    product: {duration_ms: [1000]}`, "at least one product placeholder"},
		{"empty placeholder", `  - id: "x-{}"
    procedure: rate
    rounds: 1
    product: {duration_ms: [1000]}`, "empty placeholder"},
		{"unknown placeholder", `  - id: "x-{duration_ms:03}"
    procedure: rate
    rounds: 1
    product: {duration_ms: [1000]}`, "unknown product parameter"},
		{"unmatched opening brace", `  - id: "x-{duration_ms"
    procedure: rate
    rounds: 1
    product: {duration_ms: [1000]}`, "unmatched opening brace"},
		{"unmatched closing brace", `  - id: "x-duration_ms}"
    procedure: rate
    rounds: 1
    product: {duration_ms: [1000]}`, "unmatched closing brace"},
		{"nested braces", `  - id: "x-{{duration_ms}}"
    procedure: rate
    rounds: 1
    product: {duration_ms: [1000]}`, "malformed placeholder"},
		{"list candidate substitution", `  - id: "x-{durations}"
    procedure: multidurrate
    rounds: 1
    product: {durations: [[1000, 2000]]}`, "list-valued candidate"},
		{"invalid scalar candidate", `  - id: "x-{duration_ms}"
    procedure: rate
    rounds: 1
    product: {duration_ms: ['1000']}`, "unsigned integer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildBatchPlan([]byte(productBatchYAML("sequential", tc.step)), "test-batch")
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.match)) {
				t.Fatalf("error=%v; want substring %q", err, tc.match)
			}
		})
	}
}

func TestBatchProductGeneratedIDConstraintsAndCollisions(t *testing.T) {
	valid64 := strings.Repeat("a", 63) + "{duration_ms}"
	if _, err := buildBatchPlan([]byte(productBatchYAML("sequential", fmt.Sprintf(`  - id: %q
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}
`, valid64))), "test-batch"); err != nil {
		t.Fatalf("template longer than the ID limit should pass when its rendered ID is valid: %v", err)
	}

	cases := []struct {
		name  string
		steps string
		match string
	}{
		{"too long", fmt.Sprintf(`  - id: %q
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}
`, strings.Repeat("a", 64)+"{duration_ms}"), "must match"},
		{"invalid leading character", `  - id: "_{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}`, "must match"},
		{"invalid candidate characters", `  - id: "cca-{ccas}"
    procedure: tcpri
    rounds: 1
    product: {ccas: ["bad name"]}`, "must match"},
		{"repeated rendered ID in one product", `  - id: "x-{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1, 1]}`, "duplicate batch ID"},
		{"unused axis still requires unique IDs", `  - id: "fixed-{ratesUL}"
    procedure: multidurrate
    rounds: 1
    product:
      durations: [[1000], [2000]]
      ratesUL: [100]
`, "duplicate batch ID"},
		{"collision with later explicit leaf", `  - id: "run-{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}
  - id: run-1
    procedure: trace
    rounds: 1
`, "duplicate batch ID"},
		{"collision with nested block ID", `  - id: "block-{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}
  - id: block-1
    mode: sequential
    steps:
      - id: inside
        procedure: trace
        rounds: 1
`, "duplicate batch ID"},
		{"collision with nested leaf ID", `  - id: "run-{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}
  - id: group
    mode: sequential
    steps:
      - id: run-1
        procedure: trace
        rounds: 1
`, "duplicate batch ID"},
		{"collision across products", `  - id: "same-{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}
  - id: "same-{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1]}
`, "duplicate batch ID"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildBatchPlan([]byte(productBatchYAML("sequential", tc.steps)), "test-batch")
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.match)) {
				t.Fatalf("error=%v; want substring %q", err, tc.match)
			}
		})
	}
}

func TestBatchProductRejectsRenderedIDsInsideExpansion(t *testing.T) {
	cases := []struct {
		name     string
		template []batchProductTemplatePart
		selected map[string]batchProductCandidate
	}{
		{
			name:     "oversized literal",
			template: []batchProductTemplatePart{{Literal: strings.Repeat("x", 1<<20)}, {Param: "value"}},
			selected: map[string]batchProductCandidate{"value": {TemplateValue: "1", TemplateScalar: true}},
		},
		{
			name:     "oversized scalar",
			template: []batchProductTemplatePart{{Literal: "run-"}, {Param: "value"}},
			selected: map[string]batchProductCandidate{"value": {TemplateValue: strings.Repeat("x", 1<<20), TemplateScalar: true}},
		},
		{
			name:     "repeated placeholders",
			template: []batchProductTemplatePart{{Param: "value"}, {Literal: "-"}, {Param: "value"}},
			selected: map[string]batchProductCandidate{"value": {TemplateValue: strings.Repeat("x", 40), TemplateScalar: true}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertBatchProductRenderBounded(t, tc.template, tc.selected)
		})
	}

	product := &batchProduct{
		Axes:     []batchProductAxis{{Name: "value", Candidates: []batchProductCandidate{{Value: "bad value", TemplateValue: "bad value", TemplateScalar: true}}}},
		Template: []batchProductTemplatePart{{Literal: "run-"}, {Param: "value"}},
	}
	entry := &batchNode{ID: "template", Procedure: "ratebidir", Rounds: 1, Params: experiment.ParamMap{}, Ancestry: []string{"template"}, Product: product}
	leaves, err := expandBatchProduct(entry)
	if err == nil || !strings.Contains(err.Error(), "must match") {
		t.Fatalf("expandBatchProduct error=%v; want rendered ID validation error", err)
	}
	if len(leaves) != 0 {
		t.Fatalf("invalid ID retained %d product leaves", len(leaves))
	}
}

func assertBatchProductRenderBounded(t *testing.T, template []batchProductTemplatePart, selected map[string]batchProductCandidate) {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	id, err := renderBatchProductID(template, selected)
	runtime.ReadMemStats(&after)
	if err == nil || !strings.Contains(err.Error(), "must match") {
		t.Fatalf("renderBatchProductID(%d parts) error=%v; want bounded ID error", len(template), err)
	}
	if id != "" {
		t.Fatalf("invalid rendered ID returned %d bytes", len(id))
	}
	if len(err.Error()) > 256 {
		t.Fatalf("oversized rendered ID error is %d bytes", len(err.Error()))
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 256<<10 {
		t.Fatalf("rendering allocated %d bytes for a rejected ID; want less than 256 KiB", allocated)
	}
}

func TestBatchProductLimitsAreCheckedBeforeExpansion(t *testing.T) {
	values := make([]string, 5001)
	for i := range values {
		values[i] = fmt.Sprint(i + 1)
	}
	tooManyDirectionConfigs := productBatchYAML("sequential", fmt.Sprintf(`  - id: "r-{duration_ms}"
    procedure: rate
    rounds: 1
    product: {duration_ms: [%s]}
`, strings.Join(values, ",")))
	if _, err := buildBatchPlan([]byte(tooManyDirectionConfigs), "test-batch"); err == nil || !strings.Contains(err.Error(), "more than 10000 invocations") {
		t.Fatalf("omitted-direction product error=%v; want invocation limit", err)
	}

	tooManyRounds := productBatchYAML("sequential", `  - id: "r-{duration_ms}"
    procedure: ratebidir
    rounds: 5001
    product: {duration_ms: [1, 2]}
`)
	if _, err := buildBatchPlan([]byte(tooManyRounds), "test-batch"); err == nil || !strings.Contains(err.Error(), "more than 10000 invocations") {
		t.Fatalf("round-weighted product error=%v; want invocation limit", err)
	}

	aggregateProducts := productBatchYAML("sequential", `  - id: "a-{duration_ms}"
    procedure: ratebidir
    rounds: 5000
    product: {duration_ms: [1]}
  - id: "b-{duration_ms}"
    procedure: ratebidir
    rounds: 5001
    product: {duration_ms: [1]}
`)
	if _, err := buildBatchPlan([]byte(aggregateProducts), "test-batch"); err == nil || !strings.Contains(err.Error(), "batch plans more than 10000 invocations") {
		t.Fatalf("aggregate product error=%v; want invocation limit", err)
	}

	aggregateWithOrdinary := productBatchYAML("sequential", `  - id: "a-{duration_ms}"
    procedure: ratebidir
    rounds: 1
    product: {duration_ms: [1, 2]}
  - id: ordinary
    procedure: ratebidir
    rounds: 9999
`)
	if _, err := buildBatchPlan([]byte(aggregateWithOrdinary), "test-batch"); err == nil || !strings.Contains(err.Error(), "batch plans more than 10000 invocations") {
		t.Fatalf("aggregate product/ordinary error=%v; want invocation limit", err)
	}
}

func TestBatchProductInvocationLimitExactBoundaries(t *testing.T) {
	t.Run("one configuration at 10000 rounds", func(t *testing.T) {
		plan, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "run-{duration_ms}"
    procedure: ratebidir
    rounds: 10000
    product: {duration_ms: [1000]}
`)), "test-batch")
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Execution) != MaxPlannedInvocations {
			t.Fatalf("planned %d invocations; want %d", len(plan.Execution), MaxPlannedInvocations)
		}
	})

	t.Run("two configurations with explicit direction at 10000", func(t *testing.T) {
		plan, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "run-{duration_ms}"
    procedure: rate
    rounds: 5000
    product:
      duration_ms: [1000, 2000]
      direction: [dl]
`)), "test-batch")
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.Execution) != MaxPlannedInvocations {
			t.Fatalf("planned %d invocations; want %d", len(plan.Execution), MaxPlannedInvocations)
		}
	})

	t.Run("two configurations with omitted direction exceed limit", func(t *testing.T) {
		_, err := buildBatchPlan([]byte(productBatchYAML("sequential", `  - id: "run-{duration_ms}"
    procedure: rate
    rounds: 5000
    product: {duration_ms: [1000, 2000]}
`)), "test-batch")
		if err == nil || !strings.Contains(err.Error(), "more than 10000 invocations") {
			t.Fatalf("omitted-direction boundary error=%v; want invocation limit", err)
		}
	})
}
