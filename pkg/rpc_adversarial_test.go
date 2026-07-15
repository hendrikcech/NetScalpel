package pkg

// Adversarial RPC tests: retrieving a test result twice, concurrent
// retrievals racing each other, and retrievals racing server shutdown. The
// server must return errors and keep serving — never panic, hang, or leak
// (the latter checked by goleak in TestMain).

import (
	"context"
	"net/rpc"
	"strings"
	"sync"
	"testing"
	"time"
)

// requestReceiveUDP starts a ReceiveUDP test that ends after timeout even
// without any traffic (handleReceiver bounds the receiver with args.Timeout).
func requestReceiveUDP(t *testing.T, rpcClient *rpc.Client, id string, timeout time.Duration) {
	t.Helper()
	args := RequestServerArgs{
		ID:         id,
		Timeout:    timeout,
		ServerMode: ReceiveUDP,
		Params:     BurstParams{Timeout: timeout},
	}
	var reply RequestServerReply
	if err := rpcClient.Call("Server.RequestServer", args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}
}

func requestServerResult(rpcClient *rpc.Client, id string) error {
	var reply RequestServerResultReply
	return rpcClient.Call("Server.RequestServerResult", RequestServerResultArgs{ID: id}, &reply)
}

// A second RequestServerResult for the same ID must return an error — and
// the server must survive it. An earlier version of handleChanResult
// panicked here and took the whole server down.
func TestRequestServerResultTwice(t *testing.T) {
	RegisterGob()
	port := serverPort()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := RunServer(srvCtx, ip, port, nil)
	defer func() {
		srvCancel()
		s.Stop()
	}()
	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()

	requestReceiveUDP(t, rpcClient, "twice", 100*time.Millisecond)

	returnsWithin(t, 5*time.Second, "first RequestServerResult", func() {
		err = requestServerResult(rpcClient, "twice")
	})
	if err != nil {
		t.Fatalf("first retrieval failed: %v", err)
	}

	returnsWithin(t, 2*time.Second, "second RequestServerResult", func() {
		err = requestServerResult(rpcClient, "twice")
	})
	if err == nil {
		t.Fatalf("second retrieval for the same ID succeeded; want an error")
	}
	// It used to fall through to panic("result.Res == nil") because
	// handleChanResult misread the channel receive's ok value, killing the
	// whole server on a duplicate RequestServerResult RPC.
	if !strings.Contains(err.Error(), "already retrieved") {
		t.Errorf("expected an 'already retrieved' error, got: %v", err)
	}

	// The server must still be fully serviceable.
	requestReceiveUDP(t, rpcClient, "twice-after", 50*time.Millisecond)
	returnsWithin(t, 5*time.Second, "RequestServerResult after double retrieval", func() {
		err = requestServerResult(rpcClient, "twice-after")
	})
	if err != nil {
		t.Errorf("retrieval after the double-retrieval error failed: %v", err)
	}
}

// Concurrent RequestServerResult calls for one ID: exactly one receives the
// result, the rest get errors (from the closed channel or the removed
// entry), and nothing panics or blocks.
func TestRequestServerResultConcurrent(t *testing.T) {
	RegisterGob()
	port := serverPort()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := RunServer(srvCtx, ip, port, nil)
	defer func() {
		srvCancel()
		s.Stop()
	}()
	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()

	requestReceiveUDP(t, rpcClient, "race", 200*time.Millisecond)

	const n = 5
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = requestServerResult(rpcClient, "race")
		}()
	}
	returnsWithin(t, 5*time.Second, "concurrent RequestServerResult", wg.Wait)

	succeeded := 0
	for _, e := range errs {
		if e == nil {
			succeeded++
		}
	}
	if succeeded != 1 {
		t.Errorf("%d of %d concurrent retrievals succeeded; want exactly 1 (errors: %v)", succeeded, n, errs)
	}
}

// A result request blocked on a test that never delivers must unblock when
// the server context is cancelled. RequestServerResult and
// RequestRunCommandResult share this select via handleChanResult, so one
// test covers both paths.
func TestRequestServerResultUnblocksOnServerShutdown(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := NewServer(srvCtx, nil)
	s.resultC["x"] = make(chan *Result, 1) // never written to

	go func() {
		time.Sleep(100 * time.Millisecond)
		srvCancel()
	}()

	var err error
	returnsWithin(t, 2*time.Second, "RequestServerResult", func() {
		var reply RequestServerResultReply
		err = s.RequestServerResult(RequestServerResultArgs{ID: "x"}, &reply)
	})
	if err == nil {
		t.Errorf("expected an error after server shutdown")
	}
}

// Full shutdown race over the wire: while a result request for a
// long-running test is pending, cancel the server context and Stop().
// Cancelling ends the test, which delivers its result concurrently with the
// blocked handler observing ctx.Done(), so the pending call may return
// either a result or an error — the invariants are that it returns at all,
// that Stop() completes, and that no goroutine leaks (goleak).
func TestServerShutdownWithPendingResultRequest(t *testing.T) {
	RegisterGob()
	port := serverPort()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := RunServer(srvCtx, ip, port, nil)
	defer func() {
		srvCancel()
		s.Stop() // idempotent; also runs when the test fails before the explicit Stop
	}()
	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()

	requestReceiveUDP(t, rpcClient, "shutdown", 30*time.Second)

	pending := make(chan error, 1)
	go func() {
		pending <- requestServerResult(rpcClient, "shutdown")
	}()

	// Let the result request reach the server and block on the result chan.
	time.Sleep(100 * time.Millisecond)

	returnsWithin(t, 5*time.Second, "Server.Stop with pending result request", func() {
		srvCancel()
		s.Stop()
	})

	select {
	case <-pending:
	case <-time.After(2 * time.Second):
		t.Fatalf("RequestServerResult still blocked after server stop")
	}
}
