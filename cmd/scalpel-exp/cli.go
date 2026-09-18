//go:build !noserver

package main

var cli struct {
	IP       string `help:"Server IP." default:"0.0.0.0"`
	Port     uint   `help:"Server port." default:"8500"`
	Log      string `help:"Write all log output to this file."`
	LogLevel int    `help:"Log level (-4: Debug, 0: Info, 4: Warn, 8: Error)" default:"0"`

	Client struct {
		Results   string `help:"Path to the results folder." default:"results"`
		Rounds    uint   `help:"Number of procedure rounds to run; 0 = infinite." default:"1"`
		Procedure string `help:"Procedure to use for the experiment."`
		Params    string `help:"Semicolon-separated key=value pairs passed to procedure."`
	} `cmd:"" help:"Run an experiment."`

	Server struct {
	} `cmd:"" help:"Serve remote experiment operations."`

	Procedures struct {
		Name string `arg:"" optional:"" help:"Show details for this procedure instead of the list."`
	} `cmd:"" help:"Output a list of supported procedures."`
}
