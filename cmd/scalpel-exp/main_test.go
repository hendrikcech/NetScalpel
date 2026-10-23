package main

import (
	"os"
	"os/exec"
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
		{"client help", []string{"client", "--help"}, []string{"Usage:", "required for client", "Required: procedure", "scalpel-exp procedures"}, true},
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
