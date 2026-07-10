package main

// Red/green tests for orchestration-level termination issues around Ctrl+C
// handling. These are expected to FAIL until the fixes land.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/pkg"
)

const testIP = "127.0.0.1"

var testPort atomic.Uint32

func termServerPort() uint {
	testPort.CompareAndSwap(0, 16100)
	return uint(testPort.Add(1))
}

// returnsWithin runs fn and fails the test if it does not return within d.
func returnsWithin(t *testing.T, d time.Duration, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", name, d)
	}
}

// --- runRound performs blocking Gather RPCs after cancellation ---

// The umbrella test for client-side Ctrl+C latency: a cancelled round must
// finish promptly instead of (a) waiting for senders that ignore ctx and
// (b) gathering results from a server that runs the test to its natural end.
func TestRoundFinishesQuicklyAfterCancel(t *testing.T) {
	pkg.RegisterGob()

	port := termServerPort()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	server := pkg.RunServer(srvCtx, testIP, port, nil)
	// Cancel the server ctx before Stop(): a pending blocked RPC keeps
	// rpc.ServeCodec (and thus Stop's WaitGroup) from returning.
	defer func() {
		srvCancel()
		server.Stop()
	}()

	rpcClient, err := rpc.Dial("tcp", fmt.Sprintf("%s:%v", testIP, port))
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultPath := t.TempDir()
	c := &Client{
		IP:      testIP,
		Port:    port,
		Results: resultPath,
		Rounds:  1,
	}

	// What a procedure would schedule: one long UL test starting shortly.
	e := NewExecutor(ctx, testIP, rpcClient)
	sc := &pkg.SenderClient{
		IP:        testIP,
		Out:       filepath.Join(resultPath, "owd.csv"),
		Direction: pkg.UL,
		StartAt:   time.Now().Add(300 * time.Millisecond),
		Sender: &pkg.PeriodicSender{Params: pkg.PeriodicParams{
			Interval: 10 * time.Millisecond,
			Duration: 10 * time.Second,
		}},
	}
	e.RunClient(sc)

	// Simulated Ctrl+C half a second into the round.
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()

	returnsWithin(t, 2500*time.Millisecond, "runRound", func() {
		c.runRound(ctx, e, rpcClient, resultPath)
	})

	// The cancelled round must also have aborted the
	// server-side receiver; its (partial) result must be available promptly
	// instead of after the full test duration + 1s receive timeout.
	returnsWithin(t, 2*time.Second, "RequestServerResult after abort", func() {
		var res pkg.RequestServerResultReply
		server.RequestServerResult(pkg.RequestServerResultArgs{ID: sc.ID}, &res)
	})
}

// --- a second SIGINT must force-exit a hanging process ---

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", testIP+":0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func TestSecondSignalForcesExit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping process-level test in -short mode")
	}

	bin := filepath.Join(t.TempDir(), "scalpel-exp")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, out)
	}

	port := freePort(t)
	srv := exec.Command(bin, "server", "--ip", testIP, "--port", fmt.Sprint(port))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer srv.Process.Kill()

	// Wait for the RPC listener to come up.
	var conn net.Conn
	var err error
	for range 50 {
		conn, err = net.Dial("tcp", fmt.Sprintf("%s:%v", testIP, port))
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("server did not come up: %v", err)
	}
	// The open RPC connection is what makes Server.Stop() hang today,
	// giving the second SIGINT something to rescue the user from.
	defer conn.Close()

	waitC := make(chan error, 1)
	go func() { waitC <- srv.Wait() }()

	if err := srv.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	// Now the first signal already shuts the server down cleanly;
	// the second signal only needs to rescue a *hanging* shutdown.
	if err := srv.Process.Signal(syscall.SIGINT); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}

	select {
	case <-waitC:
		// Exited; a non-zero exit status is fine, prompt exit is the point.
	case <-time.After(2 * time.Second):
		t.Fatal("server still running after two SIGINTs")
	}
}
