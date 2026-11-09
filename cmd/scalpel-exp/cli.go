package main

import (
	"github.com/alecthomas/kong"

	"github.com/hendrikcech/netscalpel/pkg"
)

var cli struct {
	Version     kong.VersionFlag `help:"Print running build version and provenance."`
	IP          string           `help:"Server IP (required for client)." default:"0.0.0.0"`
	Port        uint             `help:"Server port." default:"8500"`
	Log         string           `help:"Write all log output to this file."`
	LogLevel    int              `help:"Log level (-4: Debug, 0: Info, 4: Warn, 8: Error)" default:"0"`
	TimestampTx string           `help:"Preferred local TX timestamp source: hardware (hw), software (sw), or app." default:"software"`
	TimestampRx string           `help:"Preferred local RX timestamp source: hardware (hw), software (sw), or app." default:"software"`

	Client struct {
		Results   string    `help:"Path to the results folder." default:"results"`
		Batch     string    `help:"YAML batch file; mutually exclusive with --procedure, --params, and --rounds." xor:"batch-procedure,batch-params"`
		Rounds    uint      `help:"Number of procedure rounds to run; must be greater than zero." default:"1"`
		Procedure string    `help:"Procedure to run (required unless --batch is supplied). Use 'scalpel-exp procedures' to list supported procedures." xor:"batch-procedure"`
		Params    string    `help:"Semicolon-separated key=value pairs passed to procedure." xor:"batch-params"`
		StartMode StartMode `help:"Start anchor (case-insensitive): starlink aligns to reconfiguration intervals; immediate starts the intact schedule soon, with no exact start guarantee." default:"starlink"`
	} `cmd:"" help:"Run an experiment."`

	Server struct {
	} `cmd:"" help:"Serve remote experiment operations."`

	Procedures struct {
		Name string `arg:"" optional:"" help:"Show details for this procedure instead of the list."`
	} `cmd:"" help:"Output a list of supported procedures."`
}

func normalizedTimestampConfig() (pkg.TimestampConfig, error) {
	return pkg.ParseTimestampConfig(cli.TimestampTx, cli.TimestampRx)
}
