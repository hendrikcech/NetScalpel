package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Run the real entry point in a subprocess to check help and exit status
// without starting a server or letting os.Exit terminate the test runner.
func TestCLIHelp(t *testing.T) {
	if os.Getenv("SCALPEL_EXP_TEST_CLI") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append(os.Args[:1], os.Args[i+1:]...)
				main()
				return
			}
		}
		t.Fatal("missing CLI arguments")
	}

	cases := []struct {
		name string
		args []string
		want []string
		ok   bool
	}{
		{"missing flags", []string{"client"}, []string{"Usage:", "missing required client flags: --ip, --procedure", "scalpel-exp procedures"}, false},
		{"missing procedure", []string{"client", "--ip", "127.0.0.1"}, []string{"Usage:", "missing required client flags: --procedure", "scalpel-exp procedures"}, false},
		{"missing IP", []string{"client", "--procedure", "burst"}, []string{"Usage:", "missing required client flags: --ip"}, false},
		{"unknown procedure", []string{"client", "--ip", "127.0.0.1", "--procedure", "no-such-procedure"}, []string{"Usage:", `unknown procedure "no-such-procedure"`, "scalpel-exp procedures"}, false},
		{"zero port", []string{"client", "--ip", "127.0.0.1", "--procedure", "burst", "--port", "0"}, []string{"Usage:", "--port must be greater than zero"}, false},
		{"zero rounds with equals", []string{"client", "--ip", "127.0.0.1", "--procedure", "burst", "--rounds=0"}, []string{"Usage:", "--rounds must be greater than zero"}, false},
		{"zero rounds as argument", []string{"client", "--ip", "127.0.0.1", "--procedure", "burst", "--rounds", "0"}, []string{"Usage:", "--rounds must be greater than zero"}, false},
		{"batch conflicts with procedure", []string{"client", "--ip", "127.0.0.1", "--batch", "batch.yaml", "--procedure", "burst"}, []string{"--batch and --procedure can't be used together"}, false},
		{"batch conflicts with params", []string{"client", "--ip", "127.0.0.1", "--batch", "batch.yaml", "--params="}, []string{"--batch and --params can't be used together"}, false},
		{"batch conflicts with explicitly default rounds", []string{"client", "--ip", "127.0.0.1", "--batch", "batch.yaml", "--rounds=1"}, []string{"--batch and --rounds can't be used together"}, false},
		{"client help", []string{"client", "--help"}, []string{"Usage:", "required for client", "required unless --batch is supplied", "scalpel-exp procedures"}, true},
		{"procedure list", []string{"procedures"}, []string{"burst"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := append([]string{"-test.run=^TestCLIHelp$", "--"}, tc.args...)
			cmd := exec.Command(os.Args[0], args...)
			cmd.Env = append(os.Environ(), "SCALPEL_EXP_TEST_CLI=1")
			output, err := cmd.CombinedOutput()
			if (err == nil) != tc.ok {
				t.Fatalf("unexpected exit: %v\n%s", err, output)
			}
			text := strings.Join(strings.Fields(string(output)), " ")
			for _, want := range tc.want {
				if !strings.Contains(text, want) {
					t.Errorf("output missing %q:\n%s", want, output)
				}
			}
			if strings.Contains(string(output), "Procedure setup failed") {
				t.Errorf("CLI validation reached runtime setup:\n%s", output)
			}
		})
	}
}

func TestCLIRejectsZeroRoundsBeforeSideEffects(t *testing.T) {
	for _, rounds := range [][]string{{"--rounds=0"}, {"--rounds", "0"}} {
		name := strings.Join(rounds, " ")
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			logPath := filepath.Join(root, "client.log")
			resultsPath := filepath.Join(root, "results")
			cliArgs := append([]string{"--log", logPath, "client", "--ip", "127.0.0.1", "--port", "1", "--procedure", "trace"}, rounds...)
			cliArgs = append(cliArgs, "--results", resultsPath)
			args := append([]string{"-test.run=^TestCLIHelp$", "--"}, cliArgs...)
			cmd := exec.Command(os.Args[0], args...)
			cmd.Env = append(os.Environ(), "SCALPEL_EXP_TEST_CLI=1")
			output, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatalf("expected zero rounds to be rejected\n%s", output)
			}
			if !strings.Contains(string(output), "--rounds must be greater than zero") {
				t.Fatalf("missing rounds validation error:\n%s", output)
			}
			if strings.Contains(string(output), "rpc dial") || strings.Contains(string(output), "Experiment failed") {
				t.Fatalf("CLI reached runtime after invalid rounds:\n%s", output)
			}
			for _, path := range []string{logPath, resultsPath} {
				if _, err := os.Stat(path); !os.IsNotExist(err) {
					t.Errorf("invalid rounds created %q: %v", path, err)
				}
			}
		})
	}
}

func TestValidateClientFlagsAcceptsPositiveRounds(t *testing.T) {
	previous := cli
	defer func() { cli = previous }()
	cli.IP = "127.0.0.1"
	cli.Port = 8500
	cli.Client.Batch = ""
	cli.Client.Procedure = "trace"

	for _, rounds := range []uint{1, 2} {
		t.Run(strconv.FormatUint(uint64(rounds), 10), func(t *testing.T) {
			cli.Client.Rounds = rounds
			if err := validateClientFlags([]string{"client"}); err != nil {
				t.Fatalf("positive rounds %d rejected: %v", rounds, err)
			}
		})
	}
}

func TestCLIBatchWithoutProcedure(t *testing.T) {
	if os.Getenv("SCALPEL_EXP_TEST_CLI_BATCH") == "1" {
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append(os.Args[:1], os.Args[i+1:]...)
				main()
				return
			}
		}
		t.Fatal("missing CLI arguments")
	}

	root := t.TempDir()
	batchPath := filepath.Join(root, "batch.yaml")
	results := filepath.Join(root, "results")
	if err := os.WriteFile(batchPath, []byte("mode: sequential\nsteps: [{id: one, procedure: trace, rounds: 1}]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"-test.run=^TestCLIBatchWithoutProcedure$", "--", "client", "--ip", "127.0.0.1", "--port", "1", "--batch", batchPath, "--results", results}
	cmd := exec.Command(os.Args[0], args...)
	cmd.Env = append(os.Environ(), "SCALPEL_EXP_TEST_CLI_BATCH=1")
	output, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected closed localhost RPC port to produce a failed batch\n%s", output)
	}
	if strings.Contains(string(output), "missing required client flags: --procedure") {
		t.Fatalf("batch mode still required --procedure:\n%s", output)
	}
	for _, want := range []string{"Batch ", "Total: rounds"} {
		if !strings.Contains(string(output), want) {
			t.Errorf("output missing %q:\n%s", want, output)
		}
	}
	batchDirs, err := os.ReadDir(results)
	if err != nil {
		t.Fatal(err)
	}
	if len(batchDirs) != 1 || !batchDirs[0].IsDir() {
		t.Fatalf("expected one isolated batch directory, got %v", batchDirs)
	}
	batchResults := filepath.Join(results, batchDirs[0].Name())
	if !strings.Contains(string(output), batchResults) {
		t.Fatalf("output omitted batch results directory %q:\n%s", batchResults, output)
	}
	for _, name := range []string{batchPlanFileName, batchSummaryFileName} {
		if _, err := os.Stat(filepath.Join(batchResults, name)); err != nil {
			t.Errorf("missing %s after ordinary failed invocation: %v", name, err)
		}
	}
	summary, err := os.ReadFile(filepath.Join(batchResults, batchSummaryFileName))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(summary), "rpc dial") {
		t.Fatalf("batch summary omitted the pre-result-path invocation error:\n%s", summary)
	}
}
