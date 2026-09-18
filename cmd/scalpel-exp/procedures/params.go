package procedures

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

// Supported parameter types are inferred from the typed ParamSpec defaults:
// uint, string, []uint, []string, and []pkg.TCPCCA. Preparation is a plain
// type switch, not a reflection-driven schema system; extend the cases below
// when a new procedure needs another type.

// PrepareParams validates the invocation overrides against the procedure's
// declared parameters and returns a fresh map with every ordinary parameter
// present: initialized from the registered defaults, then the overrides
// applied. Raw strings and []string from the CLI parser are parsed to the
// declared type (a single value for a list parameter becomes a one-element
// list); correctly typed values from Go callers and schedule metadata are
// accepted, copying their slices so caller storage stays unshared.
//
// The optional "direction" override is accepted for PerDirection procedures
// and for OncePerRound procedures with SupportsDirection, and canonicalized
// to "ul" or "dl"; an omitted direction stays absent. Errors name the
// procedure and parameter, and are returned before any scheduling happens.
func PrepareParams(p Procedure, overrides experiment.ParamMap) (experiment.ParamMap, error) {
	prepared := make(experiment.ParamMap, len(p.Params)+1)
	specs := make(map[string]ParamSpec, len(p.Params))
	for _, spec := range p.Params {
		def, err := cloneDefault(spec.Default)
		if err != nil {
			return nil, fmt.Errorf("procedure %s: parameter %s: %w", p.Name, spec.Name, err)
		}
		prepared[spec.Name] = def
		specs[spec.Name] = spec
	}

	for key, override := range overrides {
		if key == "direction" {
			if !supportsDirection(p) {
				return nil, fmt.Errorf("procedure %s does not support the direction parameter", p.Name)
			}
			direction, err := canonicalDirection(override)
			if err != nil {
				return nil, fmt.Errorf("procedure %s: parameter direction: %w", p.Name, err)
			}
			prepared["direction"] = direction
			continue
		}
		spec, ok := specs[key]
		if !ok {
			return nil, fmt.Errorf("procedure %s: unknown parameter %q", p.Name, key)
		}
		value, err := applyOverride(spec, override)
		if err != nil {
			return nil, fmt.Errorf("procedure %s: parameter %s: %w", p.Name, key, err)
		}
		prepared[key] = value
	}
	return prepared, nil
}

func supportsDirection(p Procedure) bool {
	return p.Mode == PerDirection || (p.Mode == OncePerRound && p.SupportsDirection)
}

// canonicalDirection parses ul/dl case-insensitively and returns the
// lowercase form used by the accessors and result naming.
func canonicalDirection(value any) (string, error) {
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("must be a string, got %T", value)
	}
	direction, err := pkg.ParseDirection(s)
	if err != nil {
		return "", err
	}
	return direction.StringLower(), nil
}

// cloneDefault deep-copies a supported default so prepared parameters never
// share mutable storage with the registration.
func cloneDefault(value any) (any, error) {
	switch v := value.(type) {
	case uint:
		return v, nil
	case string:
		return v, nil
	case []uint:
		return slices.Clone(v), nil
	case []string:
		return slices.Clone(v), nil
	case []pkg.TCPCCA:
		return slices.Clone(v), nil
	case nil:
		return nil, fmt.Errorf("untyped nil default: use a typed value such as uint(0) or []uint{}")
	default:
		return nil, fmt.Errorf("unsupported default type %T", value)
	}
}

func applyOverride(spec ParamSpec, override any) (any, error) {
	switch spec.Default.(type) {
	case uint:
		return overrideUint(override)
	case string:
		return overrideString(override)
	case []uint:
		return overrideUints(override)
	case []string:
		return overrideStrings(override)
	case []pkg.TCPCCA:
		return overrideTCPCCAs(override)
	default:
		// register() rejects these defaults; keep PrepareParams safe if a
		// Procedure is used without registration.
		return nil, fmt.Errorf("unsupported default type %T", spec.Default)
	}
}

// parseUintValue preserves the CLI parser's numeric range policy: 32-bit,
// no negatives.
func parseUintValue(s string) (uint, error) {
	parsed, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("failed parsing '%s' as uint", s)
	}
	return uint(parsed), nil
}

func overrideUint(override any) (any, error) {
	switch v := override.(type) {
	case uint:
		return v, nil
	case string:
		return parseUintValue(v)
	case []string:
		return nil, fmt.Errorf("scalar parameter does not accept a list value")
	default:
		return nil, fmt.Errorf("uint parameter accepts a uint or a string value, got %T", override)
	}
}

func overrideString(override any) (any, error) {
	switch v := override.(type) {
	case string:
		return v, nil
	case []string:
		return nil, fmt.Errorf("scalar parameter does not accept a list value")
	default:
		return nil, fmt.Errorf("string parameter accepts a string value, got %T", override)
	}
}

func overrideUints(override any) (any, error) {
	switch v := override.(type) {
	case []uint:
		return slices.Clone(v), nil
	case uint:
		return []uint{v}, nil
	case string:
		parsed, err := parseUintValue(v)
		if err != nil {
			return nil, err
		}
		return []uint{parsed}, nil
	case []string:
		list := make([]uint, len(v))
		for i := range v {
			parsed, err := parseUintValue(v[i])
			if err != nil {
				return nil, err
			}
			list[i] = parsed
		}
		return list, nil
	default:
		return nil, fmt.Errorf("uint list parameter accepts []uint, uint, string or []string, got %T", override)
	}
}

func overrideStrings(override any) (any, error) {
	switch v := override.(type) {
	case []string:
		return slices.Clone(v), nil
	case string:
		return []string{v}, nil
	default:
		return nil, fmt.Errorf("string list parameter accepts []string or string, got %T", override)
	}
}

func overrideTCPCCAs(override any) (any, error) {
	switch v := override.(type) {
	case []pkg.TCPCCA:
		return slices.Clone(v), nil
	case pkg.TCPCCA:
		return []pkg.TCPCCA{v}, nil
	case string:
		cca, err := pkg.ParseTCPCCA(v)
		if err != nil {
			return nil, err
		}
		return []pkg.TCPCCA{cca}, nil
	case []string:
		list := make([]pkg.TCPCCA, len(v))
		for i := range v {
			var err error
			list[i], err = pkg.ParseTCPCCA(v[i])
			if err != nil {
				return nil, fmt.Errorf("failed parsing '%s' as TCPCCA", v[i])
			}
		}
		return list, nil
	default:
		return nil, fmt.Errorf("TCPCCA list parameter accepts []pkg.TCPCCA or string names, got %T", override)
	}
}
