package main

import (
	"fmt"
	"strings"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
)

// Parses semicolon-separated key=value pairs
// If value contains a comma, the value is parsed as a list
// Example:
// direction=ul;durations=100,200
func parseParams(paramStr string) (experiment.ParamMap, error) {
	params := make(experiment.ParamMap)
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
