package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/hendrikcech/netscalpel/pkg"
)

func TestSetupStandaloneLoggingRoutesConsoleAndFile(t *testing.T) {
	previous := slog.Default()
	defer slog.SetDefault(previous)

	var basicConsole bytes.Buffer
	setupStandaloneLogging(slog.LevelInfo, &basicConsole, nil)
	slog.Info("basic console marker")
	if !strings.Contains(basicConsole.String(), "basic console marker") {
		t.Fatalf("basic logger did not use its console writer: %q", basicConsole.String())
	}

	logFile, err := os.CreateTemp(t.TempDir(), "file-*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()

	var console bytes.Buffer
	setupStandaloneLogging(slog.LevelInfo, &console, logFile)
	slog.Info("multi console marker")
	slog.Debug("file debug marker")
	if !strings.Contains(console.String(), "multi console marker") || strings.Contains(console.String(), "file debug marker") {
		t.Fatalf("multi logger console level/output incorrect: %q", console.String())
	}
	if err := logFile.Sync(); err != nil {
		t.Fatal(err)
	}
	fileOutput, err := os.ReadFile(logFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(fileOutput), "multi console marker") || !strings.Contains(string(fileOutput), "file debug marker") {
		t.Fatalf("multi logger file output missing records: %q", fileOutput)
	}
}

func TestScalpelRunCLI(t *testing.T) {
	if os.Getenv("SCALPEL_RUN_TEST_CLI") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append(os.Args[:1], os.Args[i+1:]...)
				main()
				return
			}
		}
		t.Fatal("missing CLI arguments")
	}

	for _, tc := range []struct {
		name  string
		flags []string
		want  string
	}{
		{name: "info and csv", flags: []string{"--out"}, want: "--out and --info"},
		{name: "info and log", flags: []string{"--log"}, want: "--log and --info"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "shared-output")
			const original = "preserve existing output\n"
			if err := os.WriteFile(path, []byte(original), 0600); err != nil {
				t.Fatal(err)
			}
			args := []string{"-test.run=^TestScalpelRunCLI$", "--", "udp-burst", "--ip", "127.0.0.1"}
			args = append(args, tc.flags[0], path, "--info", path, "--timestamp-tx=software")
			cmd := exec.Command(os.Args[0], args...)
			cmd.Env = append(os.Environ(), "SCALPEL_RUN_TEST_CLI=1")
			var stdout, stderr bytes.Buffer
			cmd.Stdout, cmd.Stderr = &stdout, &stderr
			err := cmd.Run()
			if err == nil {
				t.Fatalf("expected colliding output paths to fail; stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("CLI diagnostic polluted CSV stdout: %q", stdout.String())
			}
			if !strings.Contains(stderr.String(), tc.want+" refer to the same output path") {
				t.Fatalf("collision diagnostic missing from stderr: %q", stderr.String())
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != original {
				t.Fatalf("collision modified existing output: got %q, want %q", got, original)
			}
		})
	}
}

func TestScalpelRunRejectsRemovedTimestampStrictFlag(t *testing.T) {
	if os.Getenv("SCALPEL_RUN_TEST_REMOVED_FLAG") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append(os.Args[:1], os.Args[i+1:]...)
				main()
				return
			}
		}
		t.Fatal("missing CLI arguments")
	}

	args := []string{
		"-test.run=^TestScalpelRunRejectsRemovedTimestampStrictFlag$", "--",
		"udp-burst", "--ip", "127.0.0.1", "--port", "0",
		"--timestamp-strict",
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "SCALPEL_RUN_TEST_REMOVED_FLAG=1")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "unknown flag") || !strings.Contains(string(output), "--timestamp-strict") {
		t.Fatalf("removed timestamp strict flag was not rejected as unknown: err=%v output=%s", err, output)
	}
}

func TestTimestampInfoPathIsUDPOnlyAndOptIn(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command string
		info    string
		want    string
	}{
		{name: "UDP has no default report", command: "udp-burst"},
		{name: "explicit path for UDP", command: "udp-periodic", info: "report.json", want: "report.json"},
		{name: "UDP rate has no default report", command: "udp-rate"},
		{name: "TCP has no report", command: "tcp", info: "report.json"},
		{name: "ICMP has no report", command: "icmp", info: "report.json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := timestampInfoPath(tc.command, tc.info); got != tc.want {
				t.Fatalf("timestampInfoPath(%q, %q) = %q, want %q", tc.command, tc.info, got, tc.want)
			}
		})
	}
}

func TestScalpelRunWritesInfoOnlyWhenRequested(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			dir := t.TempDir()
			args := []string{"-test.run=^TestScalpelRunCLI$", "--", "udp-burst",
				"--ip", "127.0.0.1", "--port", "0", "--out", "result.csv"}
			if explicit {
				args = append(args, "--info", "requested.json")
			}
			cmd := exec.Command(os.Args[0], args...)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "SCALPEL_RUN_TEST_CLI=1")
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), "rpc.Dial failed") {
				t.Fatalf("expected dial failure: %v, %s", err, output)
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.json"))
			if err != nil {
				t.Fatal(err)
			}
			if explicit {
				if len(files) != 1 || filepath.Base(files[0]) != "requested.json" {
					t.Fatalf("requested report not written: %v", files)
				}
			} else if len(files) != 0 {
				t.Fatalf("unrequested report files: %v", files)
			}
		})
	}
}

func TestScalpelRunDoesNotWriteInfoForInvalidTimestampConfig(t *testing.T) {
	if os.Getenv("SCALPEL_RUN_TEST_INVALID_CONFIG") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append(os.Args[:1], os.Args[i+1:]...)
				main()
				return
			}
		}
		t.Fatal("missing CLI arguments")
	}

	path := filepath.Join(t.TempDir(), "report.json")
	args := []string{
		"-test.run=^TestScalpelRunDoesNotWriteInfoForInvalidTimestampConfig$", "--",
		"udp-burst", "--ip", "127.0.0.1", "--info", path, "--timestamp-tx=invalid",
	}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "SCALPEL_RUN_TEST_INVALID_CONFIG=1")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "Invalid timestamp config") {
		t.Fatalf("invalid timestamp config was not rejected: err=%v output=%s", err, output)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("invalid CLI config wrote timestamp info: stat err=%v", err)
	}
}

func TestTimestampConfigFromCLIParsesSourcePreferences(t *testing.T) {
	previous := cli
	defer func() { cli = previous }()

	cli.TimestampTx = "hw"
	cli.TimestampRx = "software"
	config, err := timestampConfigFromCLI()
	if err != nil {
		t.Fatal(err)
	}
	if config.TX != pkg.TimestampHardware || config.RX != pkg.TimestampSoftware {
		t.Fatalf("hardware preference config = %+v", config)
	}
}

func TestWriteTimestampInfoPreservesFailureAndLocalConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "info.json")
	config, err := (pkg.TimestampConfig{TX: pkg.TimestampApp, RX: pkg.TimestampSoftware}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	client := &pkg.SenderClient{
		TimestampConfig: config,
		Sender:          &pkg.PeriodicSender{},
	}
	runErr := errors.New("requested timestamp source could not be selected")
	if err := writeTimestampInfo(path, client, config, runErr); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var info timestampInfo
	if err := json.Unmarshal(data, &info); err != nil {
		t.Fatal(err)
	}
	if info.SchemaVersion != 1 || info.Status != "failed" || info.Error != runErr.Error() {
		t.Fatalf("failure metadata lost: %+v", info)
	}
	if info.TimestampReport.LocalConfig != config {
		t.Fatalf("report local config = %+v, want %+v", info.TimestampReport.LocalConfig, config)
	}
}

func TestWriteTimestampInfoEmptyPathIsNoOp(t *testing.T) {
	if err := writeTimestampInfo("", nil, pkg.TimestampConfig{}, nil); err != nil {
		t.Fatalf("empty report path should be a no-op: %v", err)
	}
}

func TestWriteTimestampInfoPreservesRequestedConfigWithoutClient(t *testing.T) {
	path := filepath.Join(t.TempDir(), "info.json")
	config := pkg.TimestampConfig{TX: pkg.TimestampHardware, RX: pkg.TimestampSoftware}
	configErr := errors.New("requested timestamp source setup failed")
	if err := writeTimestampInfo(path, nil, config, configErr); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var info timestampInfo
	if err := json.Unmarshal(data, &info); err != nil {
		t.Fatal(err)
	}
	if info.Status != "failed" || info.Error != configErr.Error() || info.TimestampReport.LocalConfig != config {
		t.Fatalf("rejected config metadata lost: %+v", info)
	}
}
