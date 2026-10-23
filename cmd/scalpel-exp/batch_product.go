package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
	"github.com/hendrikcech/netscalpel/pkg"
	"go.yaml.in/yaml/v3"
)

type batchProduct struct {
	Axes     []batchProductAxis
	Template []batchProductTemplatePart
}

type batchProductAxis struct {
	Name       string
	Candidates []batchProductCandidate
}

type batchProductCandidate struct {
	Value          any
	TemplateValue  any
	TemplateScalar bool
}

type batchProductTemplatePart struct {
	Literal string
	Param   string
}

func reserveBatchID(id, where string, seenIDs map[string]struct{}) error {
	if err := validateBatchID(id, where); err != nil {
		return err
	}
	if _, exists := seenIDs[id]; exists {
		return fmt.Errorf("duplicate batch ID %q", id)
	}
	seenIDs[id] = struct{}{}
	return nil
}

func validateBatchID(id, where string) error {
	if !batchIDPattern.MatchString(id) || len(id) > MaxBatchIDLength {
		return fmt.Errorf("%s: ID %q must match [A-Za-z0-9][A-Za-z0-9._-]{0,63}", where, id)
	}
	return nil
}

func parseBatchProduct(node *yaml.Node, proc procedures.Procedure, template string, params experiment.ParamMap, where string) (*batchProduct, error) {
	fields, err := yamlFields(node, where)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("%s must contain at least one axis", where)
	}

	specs := make(map[string]any, len(proc.Params))
	for _, spec := range proc.Params {
		specs[spec.Name] = spec.Default
	}
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	product := &batchProduct{Axes: make([]batchProductAxis, 0, len(names))}
	for _, name := range names {
		if _, overlaps := params[name]; overlaps {
			return nil, fmt.Errorf("%s: parameter %q cannot appear in both params and product", where, name)
		}
		var defaultValue any
		if name == "direction" {
			if !procedureSupportsDirection(proc) {
				return nil, fmt.Errorf("%s: procedure %q does not support the direction parameter", where, proc.Name)
			}
		} else {
			var ok bool
			defaultValue, ok = specs[name]
			if !ok {
				return nil, fmt.Errorf("%s: procedure %q has no parameter %q", where, proc.Name, name)
			}
		}

		axisNode := fields[name]
		if axisNode.Kind != yaml.SequenceNode || axisNode.Tag != "!!seq" {
			return nil, fmt.Errorf("%s %q must be a non-empty YAML sequence", where, name)
		}
		if len(axisNode.Content) == 0 {
			return nil, fmt.Errorf("%s %q must have at least one candidate", where, name)
		}
		axis := batchProductAxis{Name: name, Candidates: make([]batchProductCandidate, 0, len(axisNode.Content))}
		for i, candidateNode := range axisNode.Content {
			candidateWhere := fmt.Sprintf("%s %s candidate %d", where, name, i+1)
			var value any
			if name == "direction" {
				value, err = yamlString(candidateNode, candidateWhere)
			} else {
				value, err = parseBatchParamValue(candidateNode, defaultValue, candidateWhere)
			}
			if err != nil {
				return nil, err
			}
			axis.Candidates = append(axis.Candidates, batchProductCandidate{
				Value: value, TemplateValue: value, TemplateScalar: candidateNode.Kind == yaml.ScalarNode,
			})
		}
		product.Axes = append(product.Axes, axis)
	}

	product.Template, err = parseBatchProductTemplate(template, product, where+" ID template")
	if err != nil {
		return nil, err
	}
	return product, nil
}

func procedureSupportsDirection(proc procedures.Procedure) bool {
	return proc.Mode == procedures.PerDirection || (proc.Mode == procedures.OncePerRound && proc.SupportsDirection)
}

func parseBatchProductTemplate(template string, product *batchProduct, where string) ([]batchProductTemplatePart, error) {
	axes := make(map[string]batchProductAxis, len(product.Axes))
	for _, axis := range product.Axes {
		axes[axis.Name] = axis
	}
	parts := make([]batchProductTemplatePart, 0, len(template)/2+1)
	placeholders := 0
	for position := 0; position < len(template); {
		open := strings.IndexByte(template[position:], '{')
		close := strings.IndexByte(template[position:], '}')
		if close >= 0 && (open < 0 || close < open) {
			return nil, fmt.Errorf("%s has an unmatched closing brace", where)
		}
		if open < 0 {
			parts = append(parts, batchProductTemplatePart{Literal: template[position:]})
			break
		}
		open += position
		if open > position {
			parts = append(parts, batchProductTemplatePart{Literal: template[position:open]})
		}
		endOffset := strings.IndexByte(template[open+1:], '}')
		if endOffset < 0 {
			return nil, fmt.Errorf("%s has an unmatched opening brace", where)
		}
		end := open + 1 + endOffset
		name := template[open+1 : end]
		if name == "" {
			return nil, fmt.Errorf("%s contains an empty placeholder", where)
		}
		if strings.ContainsAny(name, "{}") {
			return nil, fmt.Errorf("%s contains a malformed placeholder %q", where, name)
		}
		axis, ok := axes[name]
		if !ok {
			return nil, fmt.Errorf("%s references unknown product parameter %q", where, name)
		}
		for _, candidate := range axis.Candidates {
			if !candidate.TemplateScalar {
				return nil, fmt.Errorf("%s placeholder %q has a list-valued candidate", where, name)
			}
			if _, err := batchProductScalarText(candidate.TemplateValue); err != nil {
				return nil, fmt.Errorf("%s placeholder %q: %w", where, name, err)
			}
		}
		parts = append(parts, batchProductTemplatePart{Param: name})
		placeholders++
		position = end + 1
	}
	if placeholders == 0 {
		return nil, fmt.Errorf("%s must contain at least one product placeholder", where)
	}
	return parts, nil
}

func batchProductScalarText(value any) (string, error) {
	switch v := value.(type) {
	case bool:
		return strconv.FormatBool(v), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case string:
		return v, nil
	default:
		return "", fmt.Errorf("value of type %T is not a scalar product candidate", value)
	}
}

func expandBatchProducts(root *batchNode, seenIDs map[string]struct{}) error {
	if !batchTreeHasProducts(root) {
		return nil
	}
	if _, err := preflightBatchProductExpansion(root); err != nil {
		return err
	}
	return expandBatchProductsInNode(root, seenIDs)
}

func batchTreeHasProducts(node *batchNode) bool {
	for _, child := range node.Children {
		if child.Product != nil || child.IsBlock && batchTreeHasProducts(child) {
			return true
		}
	}
	return false
}

// The preflight includes ordinary entries so all product-generated leaves fit
// the invocation budget before any Cartesian-product leaf is allocated.
func preflightBatchProductExpansion(root *batchNode) (uint64, error) {
	var total uint64
	var visit func(*batchNode) error
	visit = func(node *batchNode) error {
		for _, child := range node.Children {
			if child.IsBlock {
				if err := visit(child); err != nil {
					return err
				}
				continue
			}
			proc, ok := procedures.Lookup(child.Procedure)
			if !ok {
				return fmt.Errorf("entry %q: unknown procedure %q", child.ID, child.Procedure)
			}
			perRound := uint64(1)
			_, explicitDirection := child.Params["direction"]
			if child.Product != nil {
				for _, axis := range child.Product.Axes {
					if axis.Name == "direction" {
						explicitDirection = true
						break
					}
				}
			}
			if proc.Mode == procedures.PerDirection && !explicitDirection {
				perRound = 2
			}
			configurations := uint64(1)
			if child.Product != nil {
				for _, axis := range child.Product.Axes {
					axisSize := uint64(len(axis.Candidates))
					if axisSize == 0 || configurations > uint64(MaxPlannedInvocations)/axisSize {
						return fmt.Errorf("entry %q: batch product exceeds the maximum of %d configurations", child.ID, MaxPlannedInvocations)
					}
					configurations *= axisSize
				}
			}
			costPerConfig := uint64(child.Rounds) * perRound
			if configurations > uint64(MaxPlannedInvocations)/costPerConfig {
				return fmt.Errorf("entry %q: batch product plans more than %d invocations", child.ID, MaxPlannedInvocations)
			}
			cost := configurations * costPerConfig
			if cost > uint64(MaxPlannedInvocations)-total {
				return fmt.Errorf("batch plans more than %d invocations", MaxPlannedInvocations)
			}
			total += cost
		}
		return nil
	}
	if err := visit(root); err != nil {
		return 0, err
	}
	return total, nil
}

func expandBatchProductsInNode(node *batchNode, seenIDs map[string]struct{}) error {
	children := make([]*batchNode, 0, len(node.Children))
	for _, child := range node.Children {
		if child.IsBlock {
			if err := expandBatchProductsInNode(child, seenIDs); err != nil {
				return err
			}
			children = append(children, child)
			continue
		}
		if child.Product == nil {
			children = append(children, child)
			continue
		}
		leaves, err := expandBatchProduct(child)
		if err != nil {
			return err
		}
		for _, leaf := range leaves {
			if err := reserveBatchID(leaf.ID, "generated product entry", seenIDs); err != nil {
				return err
			}
			children = append(children, leaf)
		}
	}
	node.Children = children
	return nil
}

func expandBatchProduct(entry *batchNode) ([]*batchNode, error) {
	product := entry.Product
	count := 1
	for _, axis := range product.Axes {
		count *= len(axis.Candidates)
	}
	leaves := make([]*batchNode, 0, count)
	selected := make(map[string]batchProductCandidate, len(product.Axes))
	var visit func(int) error
	visit = func(axisIndex int) error {
		if axisIndex < len(product.Axes) {
			axis := product.Axes[axisIndex]
			for _, candidate := range axis.Candidates {
				selected[axis.Name] = candidate
				if err := visit(axisIndex + 1); err != nil {
					return err
				}
			}
			return nil
		}

		id, err := renderBatchProductID(product.Template, selected)
		if err != nil {
			return fmt.Errorf("entry %q: %w", entry.ID, err)
		}
		if err := validateBatchID(id, "generated product entry"); err != nil {
			return fmt.Errorf("entry %q: %w", entry.ID, err)
		}
		params := cloneBatchParamMap(entry.Params)
		for _, axis := range product.Axes {
			params[axis.Name] = cloneBatchParamValue(selected[axis.Name].Value)
		}
		ancestry := append([]string(nil), entry.Ancestry...)
		ancestry[len(ancestry)-1] = id
		leaves = append(leaves, &batchNode{
			ID: id, Procedure: entry.Procedure, Rounds: entry.Rounds,
			Params: params, Ancestry: ancestry,
		})
		return nil
	}
	if err := visit(0); err != nil {
		return nil, err
	}
	return leaves, nil
}

func renderBatchProductID(template []batchProductTemplatePart, selected map[string]batchProductCandidate) (string, error) {
	var id strings.Builder
	for _, part := range template {
		if part.Param == "" {
			if err := appendBatchProductIDText(&id, part.Literal); err != nil {
				return "", err
			}
			continue
		}
		candidate, ok := selected[part.Param]
		if !ok {
			return "", fmt.Errorf("missing product value for placeholder %q", part.Param)
		}
		text, err := batchProductScalarText(candidate.TemplateValue)
		if err != nil {
			return "", err
		}
		if err := appendBatchProductIDText(&id, text); err != nil {
			return "", err
		}
	}
	return id.String(), nil
}

func appendBatchProductIDText(id *strings.Builder, text string) error {
	if len(text) > MaxBatchIDLength-id.Len() {
		return fmt.Errorf("rendered ID must match [A-Za-z0-9][A-Za-z0-9._-]{0,63} (maximum %d bytes)", MaxBatchIDLength)
	}
	id.WriteString(text)
	return nil
}

func cloneBatchParamMap(params experiment.ParamMap) experiment.ParamMap {
	cloned := make(experiment.ParamMap, len(params))
	for name, value := range params {
		cloned[name] = cloneBatchParamValue(value)
	}
	return cloned
}

func cloneBatchParamValue(value any) any {
	switch v := value.(type) {
	case []uint:
		return append([]uint(nil), v...)
	case []string:
		return append([]string(nil), v...)
	case []pkg.TCPCCA:
		return append([]pkg.TCPCCA(nil), v...)
	default:
		return value
	}
}
