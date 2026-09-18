package experiment

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/hendrikcech/netscalpel/pkg"
)

// ParamMap holds the parameters of one procedure invocation. Values are
// either normalized typed values produced by procedures.PrepareParams
// (uint, string, []uint, []string, []pkg.TCPCCA, plus the optional direction
// as a string) or raw strings and []string from the CLI parser, which the
// accessors below still accept. Wrong dynamic types return errors instead of
// panicking.
type ParamMap map[string]any

func (p ParamMap) Direction() (pkg.Direction, error) {
	value, ok := p["direction"]
	if !ok {
		return 999, fmt.Errorf("Direction parameter not present")
	}
	directionStr, ok := value.(string)
	if !ok {
		return 999, fmt.Errorf("Direction parameter must be string")
	}
	direction, err := pkg.ParseDirection(directionStr)
	if err != nil {
		return 999, err
	}
	return direction, nil
}

// String returns the string stored under key.
func (p ParamMap) String(key string) (string, error) {
	value, ok := p[key]
	if !ok {
		return "", fmt.Errorf("Parameter '%v' not present", key)
	}
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("Parameter '%v' must be a string, got %T", key, value)
	}
	return s, nil
}

// Strings returns the strings stored under key; a single string value is
// treated as a one-element list.
func (p ParamMap) Strings(key string) ([]string, error) {
	value, ok := p[key]
	if !ok {
		return nil, fmt.Errorf("Parameter '%v' not present", key)
	}
	switch v := value.(type) {
	case []string:
		return slices.Clone(v), nil
	case string:
		return []string{v}, nil
	default:
		return nil, fmt.Errorf("Parameter '%v' must be a string or a list of strings, got %T", key, value)
	}
}

// UintsOr returns the values stored under key, or def if the key is absent.
func (p ParamMap) UintsOr(key string, def []uint) ([]uint, error) {
	if _, ok := p[key]; !ok {
		return def, nil
	}
	return p.Uints(key)
}

// Uints returns the uint list stored under key; a single string value is
// parsed as a one-element list.
func (p ParamMap) Uints(key string) ([]uint, error) {
	if list, ok := p[key].([]uint); ok {
		return slices.Clone(list), nil
	}
	listStr, err := p.Strings(key)
	if err != nil {
		return nil, err
	}
	list := make([]uint, len(listStr))
	for i := range listStr {
		parsed, err := strconv.ParseUint(listStr[i], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Parameter %v: failed parsing '%s' as uint", key, listStr[i])
		}
		list[i] = uint(parsed)
	}
	return list, nil
}

// UintOr returns the value stored under key, or def if the key is absent.
func (p ParamMap) UintOr(key string, def uint) (uint, error) {
	if _, ok := p[key]; !ok {
		return def, nil
	}
	return p.Uint(key)
}

// Uint returns the uint stored under key; a string value is parsed.
// The accepted numeric range matches the CLI parser (32-bit).
func (p ParamMap) Uint(key string) (uint, error) {
	switch v := p[key].(type) {
	case uint:
		return v, nil
	case string:
		parsed, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return 0, fmt.Errorf("Parameter '%v': failed parsing '%s' as uint", key, v)
		}
		return uint(parsed), nil
	case nil:
		if _, ok := p[key]; !ok {
			return 0, fmt.Errorf("Parameter '%v' not present", key)
		}
		return 0, fmt.Errorf("Parameter '%v' must be a uint or a string, got nil", key)
	default:
		return 0, fmt.Errorf("Parameter '%v' must be a uint or a string, got %T", key, v)
	}
}

// TCPCCAsOr returns the CCAs stored under key, or def if the key is absent.
func (p ParamMap) TCPCCAsOr(key string, def []pkg.TCPCCA) ([]pkg.TCPCCA, error) {
	if _, ok := p[key]; !ok {
		return def, nil
	}
	return p.TCPCCAs(key)
}

// TCPCCAs returns the congestion control algorithms stored under key; string
// values are parsed with pkg.ParseTCPCCA.
func (p ParamMap) TCPCCAs(key string) ([]pkg.TCPCCA, error) {
	if list, ok := p[key].([]pkg.TCPCCA); ok {
		return slices.Clone(list), nil
	}
	listStr, err := p.Strings(key)
	if err != nil {
		return nil, err
	}
	list := make([]pkg.TCPCCA, len(listStr))
	for i := range listStr {
		var err error
		list[i], err = pkg.ParseTCPCCA(listStr[i])
		if err != nil {
			return nil, fmt.Errorf("Parameter %v: failed parsing '%s' as TCPCCA", key, listStr[i])
		}
	}
	return list, nil
}
