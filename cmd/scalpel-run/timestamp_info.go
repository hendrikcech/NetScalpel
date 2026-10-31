package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/hendrikcech/netscalpel/pkg"
)

type timestampInfo struct {
	SchemaVersion   int                 `json:"schema_version"`
	Status          string              `json:"status"`
	TimestampReport pkg.TimestampReport `json:"timestamp_report"`
	Error           string              `json:"error,omitempty"`
}

func writeTimestampInfo(path string, client *pkg.SenderClient, config pkg.TimestampConfig, runErr error) error {
	if path == "" {
		return nil
	}
	status := "complete"
	if runErr != nil {
		status = "failed"
	}
	var report pkg.TimestampReport
	if client != nil {
		report = client.TimestampReport()
	}
	report.LocalConfig = config
	info := timestampInfo{
		SchemaVersion:   1,
		Status:          status,
		TimestampReport: report,
	}
	if runErr != nil {
		info.Error = runErr.Error()
	}
	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal timestamp info: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write timestamp info: %w", err)
	}
	return nil
}
