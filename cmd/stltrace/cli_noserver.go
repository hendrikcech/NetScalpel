//go:build noserver

package main

var cli struct {
	IP   string `help:"Server IP." default:"0.0.0.0"`
	Port uint   `help:"Server port." default:"8500"`
	Log  string `help:"Write all log output to this file."`
	// Profile string `help:"Write pprof to file"`

	Client struct {
		Results   string `help:"Path to the results folder." default:"results"`
		Rounds    uint   `help:"number of measurement rounds to run; 0 = infinite" default:"1"`
		Procedure string `help:"Test procedure to run."`
		Params    string `help:"Semicolon-separated key=value pairs passed to procedure."`
	} `cmd:"" help:"Send a burst of UDP packets."`
}
