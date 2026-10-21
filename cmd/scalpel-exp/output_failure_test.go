package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
)

func failureRPC(t *testing.T) (string, uint) {
	t.Helper()
	s := rpc.NewServer()
	if err := s.RegisterName("Server", &LegacyServer{}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.ServeConn(c)
		}
	}()
	t.Cleanup(func() { ln.Close(); <-done })
	return "127.0.0.1", uint(ln.Addr().(*net.TCPAddr).Port)
}

func TestClientRunOutputDirectoryFailure(t *testing.T) {
	ip, port := failureRPC(t)
	c := &Client{IP: ip, Port: port, Rounds: 1, Procedure: "burst", Results: "/dev/null/no-results"}
	if err := c.Run(context.Background()); err == nil {
		t.Fatal("output directory failure returned success")
	}
}

func TestCLIOutputFailureExitsNonzero(t *testing.T) {
	if testing.Short() {
		t.Skip("process test")
	}
	bin := filepath.Join(t.TempDir(), "scalpel-exp")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v %s", err, out)
	}
	ip, port := failureRPC(t)
	cmd := exec.Command(bin, "client", "--ip", ip, "--port", strconv.Itoa(int(port)), "--procedure", "burst", "--results", "/dev/null/no-results")
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("CLI returned success: %s", out)
	}
}

func TestInvocationOutputFailuresPropagate(t *testing.T) {
	for _, phase := range []string{"schedule", "initial", "final"} {
		t.Run(phase, func(t *testing.T) {
			ip, port := failureRPC(t)
			rpcClient, err := dialRpcClient(context.Background(), ip, port)
			if err != nil {
				t.Fatal(err)
			}
			defer rpcClient.Close()
			c := &Client{IP: ip, Port: port, Results: t.TempDir(), Procedure: "test", Rounds: 1}
			defer func() {
				if c.slogFile != nil {
					c.slogFile.Close()
				}
			}()
			e := experiment.NewExecutor(context.Background(), ip, rpcClient)
			proc := procedures.Procedure{Name: "test", Mode: procedures.OncePerRound, Run: func(_ *experiment.Executor, _ time.Time, path string, _ experiment.ParamMap) error {
				if phase == "schedule" {
					return errors.New("bad schedule")
				}
				if phase == "initial" {
					return os.Mkdir(filepath.Join(path, "info.json"), 0700)
				}
				return nil
			}}
			path, scheduleErr := c.scheduleProcedureInvocation(context.Background(), e, time.Now(), proc, nil)
			if phase == "final" {
				if scheduleErr != nil {
					t.Fatal(scheduleErr)
				}
				if err := os.Remove(filepath.Join(path, "info.json")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(path, "info.json"), 0700); err != nil {
					t.Fatal(err)
				}
			} else if scheduleErr == nil || e.Started {
				t.Fatal("setup failure did not stop launching")
			}
			if err := c.finishProcedureInvocation(context.Background(), e, rpcClient, path); err == nil {
				t.Fatal("finalization returned success")
			}
			if phase == "schedule" {
				b, err := os.ReadFile(filepath.Join(path, "info.json"))
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Contains(b, []byte(`"status": "failed"`)) {
					t.Fatalf("schedule failure not finalized: %s", b)
				}
			}
		})
	}
}

func TestServerLogBoundsUnreadTransport(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	rpcClient := rpc.NewClient(b)
	defer rpcClient.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	c := &Client{}
	if err := c.writeServerLog(ctx, t.TempDir(), rpcClient); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error: %v", err)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("server log transport write exceeded deadline")
	}
}

func TestMetadataTimeoutReconnectsBeforeScheduling(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := rpc.NewServer()
	if err := s.RegisterName("Server", &LegacyServer{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		first, err := ln.Accept()
		if err != nil {
			return
		}
		defer first.Close()
		// First connection intentionally never reads/responds. The reconnect
		// serves ordinary RPCs, including the unsupported metadata fallback.
		second, err := ln.Accept()
		if err != nil {
			return
		}
		defer second.Close()
		s.ServeConn(second)
	}()
	c := &Client{IP: "127.0.0.1", Port: uint(ln.Addr().(*net.TCPAddr).Port), Procedure: "test", Results: t.TempDir()}
	initial, err := dialRpcClient(ctx, c.IP, c.Port)
	if err != nil {
		t.Fatal(err)
	}
	defer initial.Close()
	e := experiment.NewExecutor(ctx, c.IP, initial)
	proc := procedures.Procedure{Name: "test", Mode: procedures.OncePerRound, Run: func(*experiment.Executor, time.Time, string, experiment.ParamMap) error { return nil }}
	path, err := c.scheduleProcedureInvocation(ctx, e, time.Now(), proc, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		e.RpcClient.Close()
		<-done
		if c.slogFile != nil {
			c.slogFile.Close()
		}
	}()
	if e.RpcClient == initial || !e.Started {
		t.Fatal("closed metadata channel reused for execution")
	}
	if err := c.finishProcedureInvocation(ctx, e, e.RpcClient, path); err != nil {
		t.Fatal(err)
	}
	if len(e.Manifest.Warnings) == 0 || e.Manifest.Status != "complete" {
		t.Fatalf("metadata timeout not optional: %+v", e.Manifest)
	}
}
