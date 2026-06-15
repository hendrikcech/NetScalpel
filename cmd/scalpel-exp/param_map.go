package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/hendrikcech/netscalpel/pkg"
)

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

func (p ParamMap) Strings(key string) ([]string, error) {
	value, ok := p[key]
	if !ok {
		return nil, fmt.Errorf("Parameter '%v' not present", key)
	}
	listStr, ok := value.([]string)
	if !ok {
		// Only a single element
		listStr = []string{value.(string)}
	}
	return listStr, nil
}

func (p ParamMap) Uints(key string) ([]uint, error) {
	listStr, err := p.Strings(key)
	if err != nil {
		return nil, err
	}
	list := make([]uint, len(listStr))
	for i := range listStr {
		var err error
		parsed, err := strconv.ParseUint(listStr[i], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("Parameter %v: failed parsing '%s' as uint", key, listStr[i])
		}
		list[i] = uint(parsed)
	}
	return list, nil
}

func (p ParamMap) Uint(key string) (uint, error) {
	value, ok := p[key]
	if !ok {
		return 0, fmt.Errorf("Parameter '%v' not present", key)
	}
	parsed, err := strconv.ParseUint(value.(string), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("Parameter '%v': failed parsing '%s' as uint", key, value)
	}
	return uint(parsed), nil
}

func (p ParamMap) TCPCCAs(key string) ([]pkg.TCPCCA, error) {
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

// Parses semicolon-separated key=value pairs
// If value contains a comma, the value is parsed as a list
// Example:
// direction=ul;durations=100,200
func parseParams(paramStr string) (ParamMap, error) {
	params := make(map[string]any)
	if paramStr == "" {
		return params, nil
	}
	for _, kv := range strings.Split(paramStr, ";") {
		parts := strings.Split(kv, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("Invalid key-value pair: %v", kv)
		}
		key := parts[0]
		value := parts[1]
		valueParts := strings.Split(value, ",")
		if len(valueParts) == 1 {
			params[key] = value
		} else {
			params[key] = valueParts
		}
	}
	return params, nil
}
