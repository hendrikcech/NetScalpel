package main

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
	"github.com/hendrikcech/netscalpel/internal/buildinfo"
	"github.com/hendrikcech/netscalpel/pkg"
)

const (
	MaxBatchFileBytes      = 8 << 20
	MaxBatchDepth          = 16
	MaxPlannedInvocations  = 10000
	MaxBatchIDLength       = 64
	maxSummaryErrorBytes   = 4096
	batchPlanFileName      = "batch-plan.json"
	batchSummaryFileName   = "batch-summary.json"
	batchFileFormatVersion = 1
	batchSummarySchema     = 1
)

var batchIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

type batchNode struct {
	ID        string
	Mode      string
	IsBlock   bool
	Procedure string
	Rounds    uint
	Params    experiment.ParamMap
	Children  []*batchNode
	Proc      procedures.Procedure
	Invokes   []experiment.ParamMap
	Resolved  []map[string]any
	Ancestry  []string
}

type BatchPlan struct {
	SchemaVersion         int                      `json:"schema_version"`
	BatchID               string                   `json:"batch_id"`
	Seed                  int64                    `json:"seed"`
	Mode                  string                   `json:"mode"`
	Build                 buildinfo.Info           `json:"build"`
	CreatedAt             time.Time                `json:"created_at"`
	MaxDepth              int                      `json:"max_depth"`
	MaxPlannedInvocations int                      `json:"max_planned_invocations"`
	Entries               []BatchPlannedEntry      `json:"entries"`
	Blocks                []BatchPlannedBlock      `json:"blocks"`
	Execution             []BatchPlannedInvocation `json:"execution"`
	root                  *batchNode               `json:"-"`
}

type BatchPlannedEntry struct {
	ID                  string         `json:"id"`
	Procedure           string         `json:"procedure"`
	Ancestry            []string       `json:"ancestry"`
	Rounds              uint           `json:"rounds"`
	InvocationsPerRound uint           `json:"invocations_per_round"`
	Parameters          map[string]any `json:"parameters"`
}

type BatchPlannedBlock struct {
	ID       string   `json:"id"`
	Mode     string   `json:"mode"`
	Ancestry []string `json:"ancestry"`
}

type BatchPlannedInvocation struct {
	ExecutionPosition  uint           `json:"execution_position"`
	EntryID            string         `json:"entry_id"`
	Ancestry           []string       `json:"ancestry"`
	Procedure          string         `json:"procedure"`
	EntryRounds        uint           `json:"entry_rounds"`
	Round              uint           `json:"round"`
	InvocationPosition uint           `json:"invocation_position"`
	Parameters         map[string]any `json:"parameters"`
}

type BatchInvocationMetadata struct {
	BatchID            string
	EntryID            string
	Ancestry           []string
	ExecutionPosition  uint
	Round              uint
	InvocationPosition uint
}

type BatchCounts struct {
	Successful uint `json:"successful"`
	Failed     uint `json:"failed"`
	Cancelled  uint `json:"cancelled"`
	NotRun     uint `json:"not_run"`
}

type BatchRoundSummary struct {
	Round  uint   `json:"round"`
	Status string `json:"status"`
}

type BatchEntrySummary struct {
	ID                 string              `json:"id"`
	Procedure          string              `json:"procedure"`
	Ancestry           []string            `json:"ancestry"`
	PlannedRounds      uint                `json:"planned_rounds"`
	PlannedInvocations uint                `json:"planned_invocations"`
	Counts             BatchCounts         `json:"counts"`
	InvocationCounts   BatchCounts         `json:"invocation_counts"`
	Rounds             []BatchRoundSummary `json:"rounds"`
}

type BatchBlockSummary struct {
	ID               string      `json:"id"`
	Ancestry         []string    `json:"ancestry"`
	Counts           BatchCounts `json:"counts"`
	InvocationCounts BatchCounts `json:"invocation_counts"`
	Status           string      `json:"status"`
}

type BatchInvocationSummary struct {
	ExecutionPosition  uint     `json:"execution_position"`
	EntryID            string   `json:"entry_id"`
	Ancestry           []string `json:"ancestry"`
	Round              uint     `json:"round"`
	InvocationPosition uint     `json:"invocation_position"`
	Status             string   `json:"status"`
	ResultPath         string   `json:"result_path,omitempty"`
	Error              string   `json:"error,omitempty"`
}

type BatchSummary struct {
	SchemaVersion    int                      `json:"schema_version"`
	BatchID          string                   `json:"batch_id"`
	PlanFile         string                   `json:"plan_file"`
	Seed             int64                    `json:"seed"`
	Mode             string                   `json:"mode"`
	StartedAt        time.Time                `json:"started_at"`
	FinishedAt       *time.Time               `json:"finished_at,omitempty"`
	Status           string                   `json:"status"`
	Counts           BatchCounts              `json:"counts"`
	InvocationCounts BatchCounts              `json:"invocation_counts"`
	Entries          []BatchEntrySummary      `json:"entries"`
	Blocks           []BatchBlockSummary      `json:"blocks"`
	Invocations      []BatchInvocationSummary `json:"invocations"`
	Errors           []string                 `json:"errors,omitempty"`
}

type BatchInvocationOutcome struct {
	Err             error
	Fatal           bool
	Failed          bool
	RemoteConfirmed bool
	Cancelled       bool
	ResultPath      string
}

type BatchInvokeFunc func(context.Context, BatchPlannedInvocation) BatchInvocationOutcome
type BatchSaveFunc func(*BatchSummary) error

type FatalInvocationError struct{ Err error }

func (e *FatalInvocationError) Error() string { return e.Err.Error() }
func (e *FatalInvocationError) Unwrap() error { return e.Err }

func LoadBatchPlan(path string) (*BatchPlan, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open batch file: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, MaxBatchFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read batch file: %w", err)
	}
	if len(data) > MaxBatchFileBytes {
		return nil, fmt.Errorf("batch file exceeds %d bytes", MaxBatchFileBytes)
	}
	return buildBatchPlan(data, pkg.GenID("batch"))
}

func buildBatchPlan(data []byte, batchID string) (*BatchPlan, error) {
	if len(data) > MaxBatchFileBytes {
		return nil, fmt.Errorf("batch file exceeds %d bytes", MaxBatchFileBytes)
	}
	if !batchIDPattern.MatchString(batchID) {
		return nil, fmt.Errorf("invalid generated batch ID %q", batchID)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var document yaml.Node
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode batch YAML: %w", err)
	}
	var extra yaml.Node
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, errors.New("batch YAML must contain exactly one document")
		}
		return nil, fmt.Errorf("decode trailing YAML document: %w", err)
	}
	if len(document.Content) != 1 {
		return nil, errors.New("batch YAML document is empty")
	}
	root := document.Content[0]
	if err := rejectAliasesAndDuplicateKeys(root); err != nil {
		return nil, err
	}
	fields, err := yamlFields(root, "batch root")
	if err != nil {
		return nil, err
	}
	if err := onlyFields(fields, "batch root", "mode", "seed", "steps"); err != nil {
		return nil, err
	}
	mode, err := requiredString(fields, "mode", "batch root")
	if err != nil {
		return nil, err
	}
	if !validBatchMode(mode) {
		return nil, fmt.Errorf("batch root: invalid mode %q", mode)
	}
	seed := int64(0)
	if seedNode, ok := fields["seed"]; ok {
		seed, err = yamlInt64(seedNode, "batch root seed")
		if err != nil {
			return nil, err
		}
	} else {
		seed, err = randomBatchSeed()
		if err != nil {
			return nil, fmt.Errorf("generate batch seed: %w", err)
		}
	}
	stepsNode, ok := fields["steps"]
	if !ok {
		return nil, errors.New("batch root: missing required field \"steps\"")
	}
	steps, err := parseBatchSteps(stepsNode, 1, nil)
	if err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, errors.New("batch root: steps must be non-empty")
	}
	plan := &BatchPlan{
		SchemaVersion:         batchFileFormatVersion,
		BatchID:               batchID,
		Seed:                  seed,
		Mode:                  mode,
		Build:                 buildinfo.Read(),
		CreatedAt:             time.Now().UTC(),
		MaxDepth:              MaxBatchDepth,
		MaxPlannedInvocations: MaxPlannedInvocations,
	}
	plan.root = &batchNode{Mode: mode, Children: steps}
	plannedCount := 0
	if err := prepareBatchNode(plan.root, seed, &plannedCount); err != nil {
		return nil, err
	}
	if err := dryRunBatchNode(plan.root, seed); err != nil {
		return nil, err
	}
	collectBatchMetadata(plan.root, nil, plan)
	rng := rand.New(rand.NewSource(seed))
	planExecution(plan.root, rng, plan)
	return plan, nil
}

func randomBatchSeed() (int64, error) {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b[:])), nil
}

func validBatchMode(mode string) bool {
	return mode == "sequential" || mode == "shuffled" || mode == "interleaved"
}

func parseBatchSteps(node *yaml.Node, depth int, ancestry []string) ([]*batchNode, error) {
	return parseBatchStepsSeen(node, depth, ancestry, make(map[string]struct{}))
}

func parseBatchStepsSeen(node *yaml.Node, depth int, ancestry []string, seenIDs map[string]struct{}) ([]*batchNode, error) {
	if depth > MaxBatchDepth {
		return nil, fmt.Errorf("batch nesting exceeds maximum depth %d", MaxBatchDepth)
	}
	if node.Kind != yaml.SequenceNode || node.Tag != "!!seq" {
		return nil, fmt.Errorf("steps must be a YAML sequence")
	}
	steps := make([]*batchNode, 0, len(node.Content))
	for i, child := range node.Content {
		where := fmt.Sprintf("step %d", i+1)
		fields, err := yamlFields(child, where)
		if err != nil {
			return nil, err
		}
		if err := onlyFields(fields, where, "id", "procedure", "rounds", "params", "mode", "steps"); err != nil {
			return nil, err
		}
		id, err := requiredString(fields, "id", where)
		if err != nil {
			return nil, err
		}
		if !batchIDPattern.MatchString(id) || len(id) > MaxBatchIDLength {
			return nil, fmt.Errorf("%s: ID %q must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}", where, id)
		}
		if _, exists := seenIDs[id]; exists {
			return nil, fmt.Errorf("duplicate batch ID %q", id)
		}
		seenIDs[id] = struct{}{}
		stepAncestry := append(append([]string(nil), ancestry...), id)
		procedureNode, procedureSet := fields["procedure"]
		modeNode, modeSet := fields["mode"]
		if procedureSet == modeSet {
			return nil, fmt.Errorf("%s: must be either a procedure entry or a nested block", where)
		}
		if procedureSet {
			if _, ok := fields["steps"]; ok {
				return nil, fmt.Errorf("%s: procedure entries cannot have steps", where)
			}
			if _, ok := fields["mode"]; ok {
				return nil, fmt.Errorf("%s: procedure entries cannot have mode", where)
			}
			procedure, err := yamlString(procedureNode, where+" procedure")
			if err != nil {
				return nil, err
			}
			roundsNode, ok := fields["rounds"]
			if !ok {
				return nil, fmt.Errorf("%s: missing required field \"rounds\"", where)
			}
			rounds, err := yamlPositiveRounds(roundsNode, where+" rounds")
			if err != nil {
				return nil, err
			}
			step := &batchNode{ID: id, Procedure: procedure, Rounds: rounds, Params: experiment.ParamMap{}, Ancestry: stepAncestry}
			if paramsNode, ok := fields["params"]; ok {
				proc, found := procedures.Lookup(procedure)
				if !found {
					return nil, fmt.Errorf("%s: unknown procedure %q", where, procedure)
				}
				step.Params, err = parseBatchParams(paramsNode, proc, where+" params")
				if err != nil {
					return nil, err
				}
			}
			steps = append(steps, step)
			continue
		}
		if _, ok := fields["rounds"]; ok {
			return nil, fmt.Errorf("%s: nested blocks cannot have rounds", where)
		}
		if _, ok := fields["params"]; ok {
			return nil, fmt.Errorf("%s: nested blocks cannot have params", where)
		}
		mode, err := yamlString(modeNode, where+" mode")
		if err != nil {
			return nil, err
		}
		if !validBatchMode(mode) {
			return nil, fmt.Errorf("%s: invalid mode %q", where, mode)
		}
		childStepsNode, ok := fields["steps"]
		if !ok {
			return nil, fmt.Errorf("%s: nested block is missing required field \"steps\"", where)
		}
		children, err := parseBatchStepsSeen(childStepsNode, depth+1, stepAncestry, seenIDs)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		if len(children) == 0 {
			return nil, fmt.Errorf("%s: steps must be non-empty", where)
		}
		steps = append(steps, &batchNode{ID: id, Mode: mode, IsBlock: true, Children: children, Ancestry: stepAncestry})
	}
	return steps, nil
}

func parseBatchParams(node *yaml.Node, proc procedures.Procedure, where string) (experiment.ParamMap, error) {
	fields, err := yamlFields(node, where)
	if err != nil {
		return nil, err
	}
	specs := make(map[string]any, len(proc.Params)+1)
	for _, spec := range proc.Params {
		specs[spec.Name] = spec.Default
	}
	params := make(experiment.ParamMap, len(fields))
	for key, value := range fields {
		if key == "direction" {
			parsed, err := yamlString(value, where+" direction")
			if err != nil {
				return nil, err
			}
			params[key] = parsed
			continue
		}
		defaultValue, ok := specs[key]
		if !ok {
			return nil, fmt.Errorf("%s: procedure %q has no parameter %q", where, proc.Name, key)
		}
		parsed, err := parseBatchParamValue(value, defaultValue, where+" "+key)
		if err != nil {
			return nil, err
		}
		params[key] = parsed
	}
	return params, nil
}

func parseBatchParamValue(node *yaml.Node, defaultValue any, where string) (any, error) {
	switch defaultValue.(type) {
	case bool:
		if node.Kind != yaml.ScalarNode || node.Tag != "!!bool" || node.Style&yaml.TaggedStyle != 0 {
			return nil, fmt.Errorf("%s must be a native YAML boolean", where)
		}
		var value bool
		if err := node.Decode(&value); err != nil {
			return nil, fmt.Errorf("%s must be a native YAML boolean: %w", where, err)
		}
		return value, nil
	case uint:
		return yamlUint(node, where)
	case string:
		return yamlString(node, where)
	case []uint:
		if node.Kind == yaml.SequenceNode {
			values := make([]uint, len(node.Content))
			for i, child := range node.Content {
				value, err := yamlUint(child, fmt.Sprintf("%s[%d]", where, i))
				if err != nil {
					return nil, err
				}
				values[i] = value
			}
			return values, nil
		}
		return yamlUint(node, where)
	case []string, []pkg.TCPCCA:
		if node.Kind == yaml.SequenceNode {
			values := make([]string, len(node.Content))
			for i, child := range node.Content {
				value, err := yamlString(child, fmt.Sprintf("%s[%d]", where, i))
				if err != nil {
					return nil, err
				}
				values[i] = value
			}
			return values, nil
		}
		return yamlString(node, where)
	default:
		return nil, fmt.Errorf("%s: unsupported parameter type %T", where, defaultValue)
	}
}

func yamlFields(node *yaml.Node, where string) (map[string]*yaml.Node, error) {
	if node.Kind != yaml.MappingNode || node.Tag != "!!map" {
		return nil, fmt.Errorf("%s must be a YAML mapping", where)
	}
	fields := make(map[string]*yaml.Node, len(node.Content)/2)
	for i := 0; i < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
			return nil, fmt.Errorf("%s keys must be strings", where)
		}
		if key.Value == "<<" || key.Tag == "!!merge" {
			return nil, fmt.Errorf("%s: YAML merge keys are not supported", where)
		}
		if _, exists := fields[key.Value]; exists {
			return nil, fmt.Errorf("%s: duplicate key %q", where, key.Value)
		}
		fields[key.Value] = value
	}
	return fields, nil
}

func rejectAliasesAndDuplicateKeys(node *yaml.Node) error {
	if node.Kind == yaml.AliasNode {
		return errors.New("YAML aliases are not supported")
	}
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]struct{}, len(node.Content)/2)
		for i := 0; i < len(node.Content); i += 2 {
			key := node.Content[i]
			if key.Value == "<<" || key.Tag == "!!merge" {
				return errors.New("YAML merge keys are not supported")
			}
			identity := key.Tag + ":" + key.Value
			if _, exists := seen[identity]; exists {
				return fmt.Errorf("duplicate YAML key %q", key.Value)
			}
			seen[identity] = struct{}{}
		}
	}
	for _, child := range node.Content {
		if err := rejectAliasesAndDuplicateKeys(child); err != nil {
			return err
		}
	}
	return nil
}

func onlyFields(fields map[string]*yaml.Node, where string, allowed ...string) error {
	known := make(map[string]struct{}, len(allowed))
	for _, key := range allowed {
		known[key] = struct{}{}
	}
	for key := range fields {
		if _, ok := known[key]; !ok {
			return fmt.Errorf("%s: unknown field %q", where, key)
		}
	}
	return nil
}

func requiredString(fields map[string]*yaml.Node, name, where string) (string, error) {
	node, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("%s: missing required field %q", where, name)
	}
	return yamlString(node, where+" "+name)
}

func yamlString(node *yaml.Node, where string) (string, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!str" {
		return "", fmt.Errorf("%s must be a string", where)
	}
	return node.Value, nil
}

func yamlInt64(node *yaml.Node, where string) (int64, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" || node.Style&yaml.TaggedStyle != 0 {
		return 0, fmt.Errorf("%s must be a native YAML integer", where)
	}
	var value int64
	if err := node.Decode(&value); err != nil {
		return 0, fmt.Errorf("%s must fit a signed 64-bit integer: %w", where, err)
	}
	return value, nil
}

func yamlUint(node *yaml.Node, where string) (uint, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" || node.Style&yaml.TaggedStyle != 0 {
		return 0, fmt.Errorf("%s must be a native YAML unsigned integer", where)
	}
	var value uint
	if err := node.Decode(&value); err != nil {
		return 0, fmt.Errorf("%s must fit an unsigned integer: %w", where, err)
	}
	return value, nil
}

func yamlPositiveRounds(node *yaml.Node, where string) (uint, error) {
	if node.Kind != yaml.ScalarNode || node.Tag != "!!int" || node.Style&yaml.TaggedStyle != 0 {
		return 0, fmt.Errorf("%s must be a positive finite native YAML integer", where)
	}
	var value uint64
	if err := node.Decode(&value); err != nil || value == 0 || value > MaxPlannedInvocations {
		return 0, fmt.Errorf("%s must be between 1 and %d", where, MaxPlannedInvocations)
	}
	return uint(value), nil
}

func prepareBatchNode(node *batchNode, seed int64, plannedCount *int) error {
	for i, child := range node.Children {
		if child.IsBlock {
			if err := prepareBatchNode(child, seed+int64(i)+1, plannedCount); err != nil {
				return err
			}
			continue
		}
		proc, ok := procedures.Lookup(child.Procedure)
		if !ok {
			return fmt.Errorf("entry %q: unknown procedure %q", child.ID, child.Procedure)
		}
		child.Proc = proc
		client := Client{Procedure: child.Procedure, Params: child.Params}
		_, overrides, err := client.prepareOverrides()
		if err != nil {
			return fmt.Errorf("entry %q: %w", child.ID, err)
		}
		child.Invokes = overrides
		if len(overrides) > 0 && child.Rounds > uint(MaxPlannedInvocations)/uint(len(overrides)) {
			return fmt.Errorf("batch plans more than %d invocations", MaxPlannedInvocations)
		}
		*plannedCount += int(child.Rounds) * len(overrides)
		if *plannedCount > MaxPlannedInvocations {
			return fmt.Errorf("batch plans %d invocations; maximum is %d", *plannedCount, MaxPlannedInvocations)
		}
		child.Resolved = make([]map[string]any, len(overrides))
		for i, invocationParams := range overrides {
			prepared, err := procedures.PrepareParams(proc, invocationParams)
			if err != nil {
				return fmt.Errorf("entry %q: %w", child.ID, err)
			}
			child.Resolved[i] = experiment.ManifestParameters(prepared)
		}
	}
	return nil
}

func dryRunBatchNode(node *batchNode, seed int64) error {
	for i, child := range node.Children {
		if child.IsBlock {
			if err := dryRunBatchNode(child, seed+int64(i)+1); err != nil {
				return err
			}
			continue
		}
		for invocationIndex, overrides := range child.Invokes {
			if err := procedures.ValidateSchedule(child.Proc, overrides, seed+int64(invocationIndex)); err != nil {
				return fmt.Errorf("entry %q schedule validation invocation %d: %w", child.ID, invocationIndex+1, err)
			}
		}
	}
	return nil
}

func collectBatchMetadata(node *batchNode, ancestry []string, plan *BatchPlan) {
	for _, child := range node.Children {
		if child.IsBlock {
			plan.Blocks = append(plan.Blocks, BatchPlannedBlock{ID: child.ID, Mode: child.Mode, Ancestry: append([]string(nil), ancestry...)})
			collectBatchMetadata(child, append(append([]string(nil), ancestry...), child.ID), plan)
			continue
		}
		invocations := len(child.Resolved)
		parameters := cloneJSONMap(child.Resolved[0])
		if _, explicit := child.Params["direction"]; !explicit {
			delete(parameters, "direction")
		}
		plan.Entries = append(plan.Entries, BatchPlannedEntry{
			ID:                  child.ID,
			Procedure:           child.Procedure,
			Ancestry:            append([]string(nil), ancestry...),
			Rounds:              child.Rounds,
			InvocationsPerRound: uint(invocations),
			Parameters:          parameters,
		})
	}
}

func cloneJSONMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		switch v := value.(type) {
		case []uint:
			out[key] = append([]uint(nil), v...)
		case []string:
			out[key] = append([]string(nil), v...)
		case []pkg.TCPCCA:
			out[key] = append([]pkg.TCPCCA(nil), v...)
		default:
			out[key] = value
		}
	}
	return out
}

func planExecution(node *batchNode, rng *rand.Rand, plan *BatchPlan) {
	children := append([]*batchNode(nil), node.Children...)
	switch node.Mode {
	case "shuffled":
		rng.Shuffle(len(children), func(i, j int) { children[i], children[j] = children[j], children[i] })
		for _, child := range children {
			planChild(child, rng, plan)
		}
	case "interleaved":
		items := make([]plannedWork, 0)
		for _, child := range children {
			if child.IsBlock {
				items = append(items, plannedWork{block: child})
				continue
			}
			for round := uint(1); round <= child.Rounds; round++ {
				items = append(items, plannedWork{entry: child, round: round})
			}
		}
		rng.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
		for _, item := range items {
			if item.block != nil {
				planExecution(item.block, rng, plan)
			} else {
				planRound(item.entry, item.round, plan)
			}
		}
	default:
		for _, child := range children {
			planChild(child, rng, plan)
		}
	}
}

type plannedWork struct {
	entry *batchNode
	block *batchNode
	round uint
}

func planChild(child *batchNode, rng *rand.Rand, plan *BatchPlan) {
	if child.IsBlock {
		planExecution(child, rng, plan)
		return
	}
	for round := uint(1); round <= child.Rounds; round++ {
		planRound(child, round, plan)
	}
}

func planRound(entry *batchNode, round uint, plan *BatchPlan) {
	for i, params := range entry.Resolved {
		plan.Execution = append(plan.Execution, BatchPlannedInvocation{
			ExecutionPosition:  uint(len(plan.Execution) + 1),
			EntryID:            entry.ID,
			Ancestry:           append([]string(nil), entry.Ancestry[:len(entry.Ancestry)-1]...),
			Procedure:          entry.Procedure,
			EntryRounds:        entry.Rounds,
			Round:              round,
			InvocationPosition: uint(i + 1),
			Parameters:         cloneJSONMap(params),
		})
	}
}

func RunBatchFile(ctx context.Context, file, results string, client *Client) error {
	plan, err := LoadBatchPlan(file)
	if err != nil {
		return err
	}
	batchResults, err := createBatchResultsDir(results, plan.BatchID)
	if err != nil {
		return err
	}
	originalResults := client.Results
	client.Results = batchResults
	defer func() { client.Results = originalResults }()
	fmt.Fprintf(os.Stdout, "Batch results: %s\n", batchResults)
	if err := writeBatchJSON(batchResults, batchPlanFileName, plan); err != nil {
		return &FatalInvocationError{Err: fmt.Errorf("write batch plan: %w", err)}
	}
	logSetupErr := client.setupSlog(ctx, "")
	invoke := func(ctx context.Context, invocation BatchPlannedInvocation) BatchInvocationOutcome {
		if logSetupErr != nil {
			return BatchInvocationOutcome{Err: logSetupErr, Fatal: true, Failed: true, RemoteConfirmed: true}
		}
		return client.runBatchInvocation(ctx, plan.BatchID, invocation)
	}
	summary, runErr := executeBatchPlan(ctx, plan, invoke, func(summary *BatchSummary) error {
		return writeBatchJSON(batchResults, batchSummaryFileName, summary)
	})
	if client.slogFile != nil {
		if err := client.slogFile.Close(); err != nil {
			client.slogFile = nil
			if summary != nil {
				summary.Errors = append(summary.Errors, "close invocation log: "+truncateBatchError(err.Error()))
				summary.Status = "failed"
				if saveErr := writeBatchJSON(batchResults, batchSummaryFileName, summary); saveErr != nil {
					runErr = errors.Join(runErr, &FatalInvocationError{Err: fmt.Errorf("save summary after log close failure: %w", saveErr)})
				}
			}
			runErr = errors.Join(runErr, &FatalInvocationError{Err: fmt.Errorf("close invocation log: %w", err)})
		} else {
			client.slogFile = nil
		}
	}
	if summary != nil {
		printBatchSummary(os.Stdout, summary)
	}
	if runErr != nil {
		return runErr
	}
	if summary.Counts.Failed > 0 {
		return fmt.Errorf("batch completed with %d failed rounds and %d failed invocations", summary.Counts.Failed, countFailedInvocations(summary))
	}
	if summary.Counts.Cancelled > 0 || summary.Counts.NotRun > 0 {
		return errors.New("batch was cancelled or stopped before all planned work ran")
	}
	return nil
}

func createBatchResultsDir(resultsRoot, batchID string) (string, error) {
	if err := os.MkdirAll(resultsRoot, 0755); err != nil {
		return "", fmt.Errorf("create batch results root: %w", err)
	}
	batchResults := filepath.Join(resultsRoot, batchID)
	if err := os.Mkdir(batchResults, 0755); err != nil {
		return "", fmt.Errorf("create batch results directory %q: %w", batchResults, err)
	}
	return batchResults, nil
}

func (c *Client) runBatchInvocation(ctx context.Context, batchID string, invocation BatchPlannedInvocation) BatchInvocationOutcome {
	if err := ctx.Err(); err != nil {
		return BatchInvocationOutcome{Err: err, Cancelled: true, RemoteConfirmed: true}
	}
	proc, ok := procedures.Lookup(invocation.Procedure)
	if !ok {
		return BatchInvocationOutcome{Err: fmt.Errorf("procedure disappeared from registry: %q", invocation.Procedure), Fatal: true, Failed: true, RemoteConfirmed: true}
	}
	c.Procedure = invocation.Procedure
	c.Rounds = invocation.EntryRounds
	c.round = invocation.Round - 1
	c.Params = experiment.ParamMap(invocation.Parameters)
	c.experimentID = batchID
	c.batchInvocation = &BatchInvocationMetadata{
		BatchID: batchID, EntryID: invocation.EntryID,
		Ancestry:          append([]string(nil), invocation.Ancestry...),
		ExecutionPosition: invocation.ExecutionPosition, Round: invocation.Round,
		InvocationPosition: invocation.InvocationPosition,
	}
	rpcClient, err := dialRpcClient(ctx, c.IP, c.Port)
	if err != nil {
		return BatchInvocationOutcome{Err: err, Failed: !errors.Is(err, context.Canceled), Cancelled: ctx.Err() != nil, RemoteConfirmed: true}
	}
	e := experiment.NewExecutor(ctx, c.IP, rpcClient)
	resultPath, scheduleErr := c.scheduleProcedureInvocation(ctx, e, time.Now(), proc, experiment.ParamMap(invocation.Parameters))
	out := BatchInvocationOutcome{Err: scheduleErr, Failed: scheduleErr != nil && !errors.Is(scheduleErr, context.Canceled), ResultPath: resultPath, RemoteConfirmed: true}
	var fatalErr *FatalInvocationError
	if errors.As(scheduleErr, &fatalErr) {
		out.Fatal = true
		out.Failed = true
	}
	if resultPath != "" {
		finished := c.finishProcedureInvocationClassified(ctx, e, e.RpcClient, resultPath)
		out.Err = errors.Join(out.Err, finished.err)
		out.Fatal = out.Fatal || finished.fatal
		out.Failed = out.Failed || finished.failed
		out.RemoteConfirmed = finished.remoteConfirmed
		out.Cancelled = finished.cancelled
	}
	if e.RpcClient != rpcClient {
		_ = e.RpcClient.Close()
	}
	_ = rpcClient.Close()
	if ctx.Err() != nil {
		out.Cancelled = true
		out.Err = errors.Join(out.Err, ctx.Err())
	}
	return out
}

func executeBatchPlan(ctx context.Context, plan *BatchPlan, invoke BatchInvokeFunc, save BatchSaveFunc) (*BatchSummary, error) {
	summary := newBatchSummary(plan)
	if err := save(summary); err != nil {
		return finalizeAfterSaveFailure(summary, plan, save, fmt.Errorf("save initial batch summary: %w", err))
	}
	stop := false
	for i, planned := range plan.Execution {
		if stop || ctx.Err() != nil {
			break
		}
		summary.Invocations[i].Status = "running"
		refreshBatchSummary(summary, plan)
		if err := save(summary); err != nil {
			return finalizeAfterSaveFailure(summary, plan, save, fmt.Errorf("save batch progress before execution position %d: %w", planned.ExecutionPosition, err))
		}
		outcome := invoke(ctx, planned)
		invocation := &summary.Invocations[i]
		invocation.ResultPath = outcome.ResultPath
		if outcome.Err == nil && !outcome.RemoteConfirmed {
			outcome.Err = errors.New("remote completion could not be confirmed")
		}
		if outcome.Err == nil && outcome.Fatal {
			outcome.Err = errors.New("fatal batch failure")
		}
		if outcome.Err != nil {
			invocation.Error = truncateBatchError(outcome.Err.Error())
		}
		switch {
		case outcome.Failed || outcome.Fatal && !outcome.Cancelled:
			invocation.Status = "failed"
		case outcome.Cancelled || ctx.Err() != nil:
			invocation.Status = "cancelled"
		case outcome.Err != nil:
			invocation.Status = "failed"
		default:
			invocation.Status = "success"
		}
		refreshBatchSummary(summary, plan)
		if err := save(summary); err != nil {
			return finalizeAfterSaveFailure(summary, plan, save, fmt.Errorf("save batch progress after execution position %d: %w", planned.ExecutionPosition, err))
		}
		if outcome.Fatal || !outcome.RemoteConfirmed || outcome.Cancelled || ctx.Err() != nil {
			stop = true
		}
	}
	for i := range summary.Invocations {
		if summary.Invocations[i].Status == "running" {
			summary.Invocations[i].Status = "not_run"
		}
	}
	now := time.Now().UTC()
	summary.FinishedAt = &now
	refreshBatchSummary(summary, plan)
	if err := save(summary); err != nil {
		return finalizeAfterSaveFailure(summary, plan, save, fmt.Errorf("save final batch summary: %w", err))
	}
	if summary.Counts.Failed > 0 || summary.Counts.Cancelled > 0 || summary.Counts.NotRun > 0 {
		return summary, errors.New("batch did not complete successfully")
	}
	return summary, nil
}

func finalizeAfterSaveFailure(summary *BatchSummary, plan *BatchPlan, save BatchSaveFunc, cause error) (*BatchSummary, error) {
	for i := range summary.Invocations {
		if summary.Invocations[i].Status == "running" {
			summary.Invocations[i].Status = "not_run"
		}
	}
	now := time.Now().UTC()
	summary.FinishedAt = &now
	summary.Errors = append(summary.Errors, truncateBatchError(cause.Error()))
	refreshBatchSummary(summary, plan)
	summary.Status = "failed"
	_ = save(summary)
	return summary, &FatalInvocationError{Err: cause}
}

func newBatchSummary(plan *BatchPlan) *BatchSummary {
	summary := &BatchSummary{
		SchemaVersion: batchSummarySchema,
		BatchID:       plan.BatchID,
		PlanFile:      batchPlanFileName,
		Seed:          plan.Seed,
		Mode:          plan.Mode,
		StartedAt:     time.Now().UTC(),
		Status:        "running",
		Entries:       make([]BatchEntrySummary, len(plan.Entries)),
		Blocks:        make([]BatchBlockSummary, len(plan.Blocks)),
		Invocations:   make([]BatchInvocationSummary, len(plan.Execution)),
	}
	for i, entry := range plan.Entries {
		summary.Entries[i] = BatchEntrySummary{
			ID: entry.ID, Procedure: entry.Procedure, Ancestry: append([]string(nil), entry.Ancestry...),
			PlannedRounds: entry.Rounds, PlannedInvocations: entry.Rounds * entry.InvocationsPerRound,
			Rounds: make([]BatchRoundSummary, entry.Rounds),
		}
		for round := range summary.Entries[i].Rounds {
			summary.Entries[i].Rounds[round] = BatchRoundSummary{Round: uint(round + 1)}
		}
	}
	for i, block := range plan.Blocks {
		summary.Blocks[i] = BatchBlockSummary{ID: block.ID, Ancestry: append([]string(nil), block.Ancestry...)}
	}
	for i, invocation := range plan.Execution {
		summary.Invocations[i] = BatchInvocationSummary{
			ExecutionPosition:  invocation.ExecutionPosition,
			EntryID:            invocation.EntryID,
			Ancestry:           append([]string(nil), invocation.Ancestry...),
			Round:              invocation.Round,
			InvocationPosition: invocation.InvocationPosition,
			Status:             "not_run",
		}
	}
	refreshBatchSummary(summary, plan)
	return summary
}

func refreshBatchSummary(summary *BatchSummary, plan *BatchPlan) {
	entryIndex := make(map[string]int, len(summary.Entries))
	for i := range summary.Entries {
		entryIndex[summary.Entries[i].ID] = i
		summary.Entries[i].Counts = BatchCounts{}
		summary.Entries[i].InvocationCounts = BatchCounts{}
		for j := range summary.Entries[i].Rounds {
			summary.Entries[i].Rounds[j].Status = "success"
		}
	}
	for _, invocation := range summary.Invocations {
		idx, ok := entryIndex[invocation.EntryID]
		if !ok {
			continue
		}
		entry := &summary.Entries[idx]
		if invocation.Round > 0 && invocation.Round <= uint(len(entry.Rounds)) {
			round := &entry.Rounds[invocation.Round-1]
			round.Status = combineRoundStatus(round.Status, invocation.Status)
		}
		switch invocation.Status {
		case "success":
			entry.InvocationCounts.Successful++
		case "failed":
			entry.InvocationCounts.Failed++
		case "cancelled":
			entry.InvocationCounts.Cancelled++
		case "not_run":
			entry.InvocationCounts.NotRun++
		}
	}
	for i := range summary.Entries {
		entry := &summary.Entries[i]
		for _, round := range entry.Rounds {
			incrementBatchCount(&entry.Counts, round.Status)
		}
	}
	summary.Counts = BatchCounts{}
	summary.InvocationCounts = BatchCounts{}
	for _, entry := range summary.Entries {
		summary.Counts.Successful += entry.Counts.Successful
		summary.Counts.Failed += entry.Counts.Failed
		summary.Counts.Cancelled += entry.Counts.Cancelled
		summary.Counts.NotRun += entry.Counts.NotRun
		summary.InvocationCounts.Successful += entry.InvocationCounts.Successful
		summary.InvocationCounts.Failed += entry.InvocationCounts.Failed
		summary.InvocationCounts.Cancelled += entry.InvocationCounts.Cancelled
		summary.InvocationCounts.NotRun += entry.InvocationCounts.NotRun
	}
	for i := range summary.Blocks {
		block := &summary.Blocks[i]
		block.Counts = BatchCounts{}
		block.InvocationCounts = BatchCounts{}
	}
	blockIndex := make(map[string]int, len(summary.Blocks))
	for i := range summary.Blocks {
		blockIndex[summary.Blocks[i].ID] = i
	}
	for _, entry := range summary.Entries {
		for _, ancestor := range entry.Ancestry {
			idx, ok := blockIndex[ancestor]
			if !ok {
				continue
			}
			block := &summary.Blocks[idx]
			addBatchCounts(&block.Counts, entry.Counts)
			addBatchCounts(&block.InvocationCounts, entry.InvocationCounts)
		}
	}
	for i := range summary.Blocks {
		block := &summary.Blocks[i]
		block.Status = statusFromCounts(block.Counts)
	}
	summary.Status = statusFromCounts(summary.Counts)
	if summary.FinishedAt == nil {
		summary.Status = "running"
	}
}

func combineRoundStatus(current, incoming string) string {
	if current == "" {
		if incoming == "running" {
			return "not_run"
		}
		return incoming
	}
	if current == "failed" || incoming == "failed" {
		return "failed"
	}
	if current == "cancelled" || incoming == "cancelled" {
		return "cancelled"
	}
	if current == "not_run" || incoming == "not_run" || current == "running" || incoming == "running" {
		return "not_run"
	}
	return "success"
}

func incrementBatchCount(counts *BatchCounts, status string) {
	switch status {
	case "success":
		counts.Successful++
	case "failed":
		counts.Failed++
	case "cancelled":
		counts.Cancelled++
	case "not_run", "running":
		counts.NotRun++
	}
}

func addBatchCounts(dst *BatchCounts, src BatchCounts) {
	dst.Successful += src.Successful
	dst.Failed += src.Failed
	dst.Cancelled += src.Cancelled
	dst.NotRun += src.NotRun
}

func statusFromCounts(counts BatchCounts) string {
	if counts.Failed > 0 {
		return "failed"
	}
	if counts.Cancelled > 0 {
		return "cancelled"
	}
	if counts.NotRun > 0 {
		return "not_run"
	}
	return "success"
}

func truncateBatchError(message string) string {
	if len(message) <= maxSummaryErrorBytes {
		return message
	}
	return message[:maxSummaryErrorBytes] + "... (truncated)"
}

func writeBatchJSON(dir, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	f, err := os.CreateTemp(dir, ".batch-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	closeErr := d.Close()
	if err := errors.Join(err, closeErr); err != nil {
		return err
	}
	return nil
}

func printBatchSummary(w io.Writer, summary *BatchSummary) {
	fmt.Fprintf(w, "Batch %s: %s (seed %d)\n", summary.BatchID, summary.Status, summary.Seed)
	for _, entry := range summary.Entries {
		fmt.Fprintf(w, "  %s (%s): rounds success=%d failed=%d cancelled=%d not_run=%d; invocations success=%d failed=%d cancelled=%d not_run=%d\n",
			entry.ID, strings.Join(entry.Ancestry, "/"), entry.Counts.Successful, entry.Counts.Failed,
			entry.Counts.Cancelled, entry.Counts.NotRun,
			entry.InvocationCounts.Successful, entry.InvocationCounts.Failed,
			entry.InvocationCounts.Cancelled, entry.InvocationCounts.NotRun)
	}
	fmt.Fprintf(w, "Total: rounds success=%d failed=%d cancelled=%d not_run=%d; invocations success=%d failed=%d cancelled=%d not_run=%d\n",
		summary.Counts.Successful, summary.Counts.Failed, summary.Counts.Cancelled, summary.Counts.NotRun,
		summary.InvocationCounts.Successful, summary.InvocationCounts.Failed,
		summary.InvocationCounts.Cancelled, summary.InvocationCounts.NotRun)
}

func countFailedInvocations(summary *BatchSummary) uint { return summary.InvocationCounts.Failed }
