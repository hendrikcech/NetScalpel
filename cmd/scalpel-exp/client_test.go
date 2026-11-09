package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
)

func TestStartModeCLI(t *testing.T) {
	for _, mode := range []string{"", "starlink", "STARLINK", "Immediate", "invalid"} {
		t.Run(mode, func(t *testing.T) {
			args := cli
			parser, err := kong.New(&args)
			if err != nil {
				t.Fatal(err)
			}
			argv := []string{"client", "--procedure", "burst"}
			if mode != "" {
				argv = append(argv, "--start-mode", mode)
			}
			_, err = parser.Parse(argv)
			if mode == "invalid" {
				if err == nil {
					t.Fatal("invalid mode accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := StartModeStarlink
			if strings.EqualFold(mode, string(StartModeImmediate)) {
				want = StartModeImmediate
			}
			if args.Client.StartMode != want {
				t.Fatalf("start mode = %q, want %q", args.Client.StartMode, want)
			}
		})
	}
	client := &Client{Procedure: "burst", StartMode: "invalid"}
	if _, _, err := client.prepareOverrides(); err == nil {
		t.Fatal("Go caller bypassed start-mode validation")
	}
}

// Immediate mode must keep every built-in schedule's shape and start its
// earliest event after the setup allowance.
func TestImmediateRebasesBuiltinSchedules(t *testing.T) {
	for _, proc := range procedures.All() {
		t.Run(proc.Name, func(t *testing.T) {
			params, err := procedures.PrepareParams(proc, proc.ScheduleTest.Params)
			if err != nil {
				t.Fatal(err)
			}
			e := experiment.NewExecutor(context.Background(), "127.0.0.1", nil)
			e.DeferStart = true
			anchor := experiment.NextRI(time.Now().Add(15 * time.Second))
			if err := proc.Run(e, anchor, "unused", params); err != nil {
				t.Fatal(err)
			}
			if len(e.Clients) == 0 {
				t.Fatal("schedule was empty")
			}
			original := make([]time.Time, len(e.Clients))
			for i, client := range e.Clients {
				original[i] = client.StartTime()
			}
			start := time.Now().Add(2 * time.Second)
			if err := e.Rebase(start); err != nil {
				t.Fatal(err)
			}
			if len(e.Clients) != len(original) {
				t.Fatal("schedule changed size")
			}
			shift := e.Clients[0].StartTime().Sub(original[0])
			earliest := start.Add(time.Hour)
			for i, client := range e.Clients {
				if client.StartTime().Sub(original[i]) != shift {
					t.Fatalf("client %d lost its relative timing", i)
				}
				if client.StartTime().Before(earliest) {
					earliest = client.StartTime()
				}
			}
			if !earliest.Equal(start) {
				t.Fatalf("earliest event starts at %v, want %v", earliest, start)
			}
		})
	}
}

// dialRpcClient must return an error on a failed dial. It used to os.Exit(1)
// from a helper goroutine — including after the caller had already returned
// on a cancelled context, killing the process during shutdown.
func TestDialRpcClientReturnsErrorOnRefusedConn(t *testing.T) {
	// Grab a port that is guaranteed to be closed.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()

	client, err := dialRpcClient(context.Background(), "127.0.0.1", port)
	if err == nil {
		client.Close()
		t.Fatal("expected an error dialing a closed port")
	}
}

func TestClientRunRejectsZeroRoundsBeforeSideEffects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	results := filepath.Join(root, "results")
	logger := slog.Default()
	client := &Client{
		IP:      "127.0.0.1",
		Port:    uint(listener.Addr().(*net.TCPAddr).Port),
		Results: results,
		Rounds:  0,
	}
	if err := client.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "rounds must be greater than zero") {
		t.Fatalf("Run error = %v, want positive-rounds validation error", err)
	}
	if client.experimentID != "" {
		t.Errorf("Run generated experiment ID before rejecting rounds: %q", client.experimentID)
	}
	if slog.Default() != logger {
		t.Error("Run configured logging before rejecting rounds")
	}
	if _, err := os.Stat(results); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("Run created results directory despite invalid rounds: %v", err)
	}
	if conn, err := listener.Accept(); err == nil {
		conn.Close()
		t.Error("Run dialed server despite invalid rounds")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("listener accept error = %v, want timeout", err)
	}
}

func TestDialRpcClientReturnsErrorOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client, err := dialRpcClient(ctx, "127.0.0.1", 1)
	if err == nil {
		client.Close()
		t.Fatal("expected an error dialing with a cancelled context")
	}
}

// Client preparation must reject unsupported or malformed parameters before
// Client.Run dials the server or creates measurement output.
func TestClientPrepareOverridesRejectsBeforeSideEffects(t *testing.T) {
	cases := []struct {
		name      string
		procedure string
		params    experiment.ParamMap
	}{
		{"unknown procedure", "no-such-procedure", nil},
		{"unknown parameter", "prograte", experiment.ParamMap{"nope": "1"}},
		{"malformed uint list", "prograte", experiment.ParamMap{"durations": "a,b"}},
		{"overflowing uint", "cdsf", experiment.ParamMap{"step": "4294967296"}},
		{"negative uint", "cdsf", experiment.ParamMap{"step": "-5"}},
		{"invalid direction", "burst", experiment.ParamMap{"direction": "sideways"}},
		{"direction for trace", "trace", experiment.ParamMap{"direction": "ul"}},
		{"unknown parameter on second invocation", "burst", experiment.ParamMap{"durations": "100"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			results := filepath.Join(t.TempDir(), "must-not-exist")
			client := &Client{Procedure: c.procedure, Params: c.params, Results: results}
			if _, _, err := client.prepareOverrides(); err == nil {
				t.Fatal("expected an error before any measurement side effects")
			}
			if _, err := os.Stat(results); !os.IsNotExist(err) {
				t.Errorf("measurement output was created despite the error: %v", err)
			}
		})
	}
}

func TestClientPrepareOverridesDirectionExpansion(t *testing.T) {
	t.Run("per-direction omitted runs DL then UL", func(t *testing.T) {
		client := &Client{Procedure: "burst", Params: experiment.ParamMap{}}
		proc, sets, err := client.prepareOverrides()
		if err != nil {
			t.Fatal(err)
		}
		if proc.Name != "burst" {
			t.Fatalf("resolved procedure %q", proc.Name)
		}
		if len(sets) != 2 {
			t.Fatalf("expected one invocation per direction, got %d", len(sets))
		}
		if sets[0]["direction"] != "DL" || sets[1]["direction"] != "UL" {
			t.Errorf("expected DL then UL, got %v and %v", sets[0], sets[1])
		}
		if _, ok := client.Params["direction"]; ok {
			t.Error("direction expansion leaked into the client parameters")
		}
	})

	t.Run("explicit direction is one invocation", func(t *testing.T) {
		client := &Client{Procedure: "burst", Params: experiment.ParamMap{"direction": "ul"}}
		_, sets, err := client.prepareOverrides()
		if err != nil {
			t.Fatal(err)
		}
		if len(sets) != 1 || sets[0]["direction"] != "ul" {
			t.Errorf("expected one UL invocation, got %v", sets)
		}
	})

	t.Run("once-per-round omits direction", func(t *testing.T) {
		for _, name := range []string{"trace", "owdbidir", "ratebidir"} {
			client := &Client{Procedure: name, Params: experiment.ParamMap{}}
			_, sets, err := client.prepareOverrides()
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if len(sets) != 1 {
				t.Errorf("%s: expected a single invocation, got %d", name, len(sets))
			}
			if _, ok := sets[0]["direction"]; ok {
				t.Errorf("%s: direction must stay absent when omitted", name)
			}
		}
	})

	// The prepared invocation maps for DL and UL must be independent, so a
	// later round never sees a mutated earlier invocation.
	t.Run("invocation parameters are not shared", func(t *testing.T) {
		params, err := parseParams("durations=1000,4000")
		if err != nil {
			t.Fatal(err)
		}
		client := &Client{Procedure: "multidurrate", Params: params}
		proc, sets, err := client.prepareOverrides()
		if err != nil {
			t.Fatal(err)
		}
		first, err := procedures.PrepareParams(proc, sets[0])
		if err != nil {
			t.Fatal(err)
		}
		second, err := procedures.PrepareParams(proc, sets[1])
		if err != nil {
			t.Fatal(err)
		}
		first["durations"].([]uint)[0] = 99999
		if second["durations"].([]uint)[0] == 99999 {
			t.Error("DL and UL invocations share parameter storage")
		}
		if !reflect.DeepEqual(client.Params["durations"], []string{"1000", "4000"}) {
			t.Error("invocation preparation modified the raw client parameters")
		}
	})
}
