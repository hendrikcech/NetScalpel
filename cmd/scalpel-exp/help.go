package main

// Rendering of the "procedures" command output for both CLI build variants.
// Everything is generated from registration metadata, so it works without a
// server or a valid measurement IP.

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
	"github.com/hendrikcech/netscalpel/pkg"
)

// printProcedures prints the detail help for name, or the list of all
// registered procedures when name is empty.
func printProcedures(name string) {
	if name == "" {
		fmt.Print(proceduresListText())
		return
	}
	p, ok := procedures.Lookup(name)
	if !ok {
		fmt.Fprintf(os.Stderr, "Unknown procedure %q. List procedures with 'scalpel-exp procedures'.\n", name)
		os.Exit(1)
	}
	fmt.Print(procedureHelpText(p))
}

// proceduresListText lists registered names, descriptions, and modes in
// deterministic (sorted) order.
func proceduresListText() string {
	var b strings.Builder
	b.WriteString("Procedures (run 'procedures <name>' for details):\n")
	for _, p := range procedures.All() {
		fmt.Fprintf(&b, "  %-13s %-15s %s\n", p.Name, "["+modeLabel(p)+"]", p.Description)
	}
	return b.String()
}

func procedureHelpText(p procedures.Procedure) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\n  %s\n\n", p.Name, p.Description)
	fmt.Fprintf(&b, "Execution mode: %s\n", modeDetail(p))
	b.WriteString("  " + directionNote(p) + "\n")
	if len(p.Params) == 0 {
		b.WriteString("\nParameters: none\n")
		return b.String()
	}
	b.WriteString("\nParameters:\n")
	for _, spec := range p.Params {
		fmt.Fprintf(&b, "  %-11s %-14s default: %-24s %s\n",
			spec.Name, typeName(spec.Default), defaultText(spec.Default), spec.Description)
	}
	return b.String()
}

func modeLabel(p procedures.Procedure) string {
	if p.Mode == procedures.OncePerRound {
		return "once-per-round"
	}
	return "per-direction"
}

func modeDetail(p procedures.Procedure) string {
	if p.Mode == procedures.OncePerRound {
		return "once-per-round: a single invocation every round"
	}
	return "per-direction: the procedure is invoked separately for each direction"
}

// directionNote describes direction handling, separate from ordinary
// defaults: direction has no default value, omission controls execution.
func directionNote(p procedures.Procedure) string {
	switch {
	case p.Mode == procedures.PerDirection:
		return "Direction: optional direction=ul|dl (case-insensitive). " +
			"If omitted, every round runs the procedure once for DL and once for UL."
	case p.SupportsDirection:
		return "Direction: optional direction=ul|dl (case-insensitive). " +
			"If omitted, one invocation per round covers both directions."
	default:
		return "Direction: not supported."
	}
}

// typeName reports the parameter type inferred from the typed default.
func typeName(defaultValue any) string {
	switch defaultValue.(type) {
	case uint:
		return "uint"
	case string:
		return "string"
	case []uint:
		return "[]uint"
	case []string:
		return "[]string"
	case []pkg.TCPCCA:
		return "[]pkg.TCPCCA"
	default:
		return fmt.Sprintf("%T", defaultValue)
	}
}

func defaultText(defaultValue any) string {
	switch v := defaultValue.(type) {
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case string:
		return v
	case []uint:
		parts := make([]string, len(v))
		for i, u := range v {
			parts[i] = strconv.FormatUint(uint64(u), 10)
		}
		return strings.Join(parts, ",")
	case []string:
		return strings.Join(v, ",")
	case []pkg.TCPCCA:
		parts := make([]string, len(v))
		for i, cca := range v {
			parts[i] = cca.String()
		}
		return strings.Join(parts, ",")
	default:
		return fmt.Sprintf("%v", defaultValue)
	}
}
