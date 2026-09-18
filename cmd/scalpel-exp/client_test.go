package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
)

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
