package pkg

// Red/green tests for termination issues: Ctrl+C latency, stuck-experiment
// deadlocks, and deadline bugs.
//
// These tests assert the *intended* cancellation behavior and are expected to
// FAIL until the corresponding fixes land. Every test that exercises a
// potential deadlock is wrapped in returnsWithin so it fails instead of
// hanging. Timing-sensitive tests deliberately do not run in parallel.

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os/exec"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/hendrikcech/netscalpel/internal/testutil"
)

// Aliases for the shared helpers (internal/testutil); kept as local names so
// the many call sites in this file and timing_test.go stay short.
var cancelAfter = testutil.CancelAfter
var localAddrOf = testutil.LocalAddrOf

func returnsWithin(t *testing.T, d time.Duration, name string, fn func()) {
	t.Helper()
	testutil.ReturnsWithin(t, d, name, fn)
}

// --- PeriodicSender ignores context cancellation (pkg/udp.go run loop) ---

func TestPeriodicSenderStopsOnCancel(t *testing.T) {
	conn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	s := &PeriodicSender{Params: PeriodicParams{
		Interval: 10 * time.Millisecond,
		Duration: 5 * time.Second,
	}}

	ctx := cancelAfter(100 * time.Millisecond)

	var runErr error
	returnsWithin(t, 1500*time.Millisecond, "PeriodicSender.Run", func() {
		_, runErr = s.Run(ctx, conn, localAddrOf(peer))
	})
	if runErr != nil {
		t.Errorf("expected nil error from cancelled PeriodicSender, got: %v", runErr)
	}
}

// --- MonitorCommand fixed 6s signal ladder, never reaps (pkg/commands.go) ---

func TestMonitorCommandFastKillOnCancel(t *testing.T) {
	cmd := exec.Command("sleep", "30")
	ctx := cancelAfter(100 * time.Millisecond)

	var err error
	returnsWithin(t, 2*time.Second, "MonitorCommand", func() {
		err = MonitorCommand(ctx, cmd, 30*time.Second)
	})
	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
	if cmd.ProcessState == nil {
		t.Errorf("child process was not reaped (cmd.Wait never called)")
	}
}

func TestMonitorCommandFastKillOnTimeout(t *testing.T) {
	// The natural-timeout path: every tcpdump round pays the ladder today.
	cmd := exec.Command("sleep", "30")

	var err error
	returnsWithin(t, 2*time.Second, "MonitorCommand", func() {
		err = MonitorCommand(context.Background(), cmd, 100*time.Millisecond)
	})
	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
	if cmd.ProcessState == nil {
		t.Errorf("child process was not reaped (cmd.Wait never called)")
	}
}

func TestMonitorCommandReturnsWhenProcessExitsEarly(t *testing.T) {
	cmd := exec.Command("true")

	var err error
	returnsWithin(t, 2*time.Second, "MonitorCommand", func() {
		err = MonitorCommand(context.Background(), cmd, 10*time.Second)
	})
	if err != nil {
		t.Errorf("expected nil error for a command that exited on its own, got: %v", err)
	}
}

// --- waitForUDPProbe blocks forever without probe (pkg/server.go) ---

func TestWaitForUDPProbeCancellable(t *testing.T) {
	conn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx := cancelAfter(100 * time.Millisecond)

	var probeErr error
	returnsWithin(t, 2*time.Second, "waitForUDPProbe", func() {
		_, probeErr = waitForUDPProbe(ctx, conn, time.Time{})
	})
	if probeErr == nil {
		t.Errorf("expected an error from an aborted probe wait")
	}
}

// End-to-end Symptom B chain: a server-side send test whose NAT probe never
// arrives must eventually feed an error into resultC so that
// RequestServerResult returns instead of deadlocking the client's Gather.
func TestStuckProbeProducesErrorResult(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	s := NewServer(srvCtx, nil)

	args := RequestServerArgs{
		ID:         "b1",
		Timeout:    500 * time.Millisecond,
		StartAt:    time.Now().Add(200 * time.Millisecond),
		ServerMode: SendRate,
		Params: RateParamsW{{
			Pps:         10,
			Duration:    300 * time.Millisecond,
			PayloadSize: 100,
		}},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}
	// Never send a NAT probe to reply.Port.

	var resErr error
	returnsWithin(t, 3*time.Second, "RequestServerResult", func() {
		var res RequestServerResultReply
		resErr = s.RequestServerResult(RequestServerResultArgs{ID: "b1"}, &res)
	})
	if resErr == nil {
		t.Errorf("expected an error result for a test whose probe never arrived")
	}
}

// --- TCP Accept is not cancellable (pkg/tcp.go, pkg/server.go SendTCP) ---

func TestTCPReceiverAcceptCancellable(t *testing.T) {
	ln, err := listenTCP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	ctx := cancelAfter(100 * time.Millisecond)

	r := &TCPReceiver{}
	returnsWithin(t, 2*time.Second, "TCPReceiver.Run", func() {
		r.Run(ctx, ln) // no client ever dials; error return expected
	})
}

func TestServerSendTCPAcceptProducesErrorResult(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	s := NewServer(srvCtx, nil)

	args := RequestServerArgs{
		ID:         "b2",
		Timeout:    500 * time.Millisecond,
		StartAt:    time.Now().Add(200 * time.Millisecond),
		ServerMode: SendTCP,
		Params:     TCPSenderParams{Duration_: 300 * time.Millisecond},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}
	// Never dial reply.Port.

	var resErr error
	returnsWithin(t, 3*time.Second, "RequestServerResult", func() {
		var res RequestServerResultReply
		resErr = s.RequestServerResult(RequestServerResultArgs{ID: "b2"}, &res)
	})
	if resErr == nil {
		t.Errorf("expected an error result for a test whose client never connected")
	}
}

// --- client RPCs are not context-aware (pkg/client.go Gather) ---

// Design decision encoded here: SenderClient.Gather must respect the ctx it
// already receives and return promptly (with an error) when it is cancelled,
// even if the server-side test never produces a result.
func TestGatherReturnsOnCanceledContext(t *testing.T) {
	RegisterGob()

	port := serverPort()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := RunServer(srvCtx, ip, port, nil)
	// Cleanup ordering matters: rpc.ServeCodec only returns once pending
	// calls finish, and s.Stop() waits for ServeConn. Cancel the
	// server ctx first so the blocked RequestServerResult handler unblocks
	// (via handleChanResult's s.ctx.Done select), then Stop() can complete.
	defer func() {
		srvCancel()
		s.Stop()
	}()

	// A server-side test that will not produce a result for a long time
	// (probe never sent; generous Timeout so even a fixed, bounded probe
	// wait does not release the result before the assertion below).
	args := RequestServerArgs{
		ID:         "b3",
		Timeout:    20 * time.Second,
		StartAt:    time.Now().Add(100 * time.Millisecond),
		ServerMode: SendRate,
		Params: RateParamsW{{
			Pps:         10,
			Duration:    300 * time.Millisecond,
			PayloadSize: 100,
		}},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}

	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()

	c := &SenderClient{
		IP:        ip,
		Direction: DL,
		ID:        "b3",
		Sender: &RateSender{Params: RateParamsW{{
			Pps:         10,
			Duration:    300 * time.Millisecond,
			PayloadSize: 100,
		}}},
	}

	ctx := cancelAfter(200 * time.Millisecond)

	var gatherErr error
	returnsWithin(t, 2*time.Second, "SenderClient.Gather", func() {
		gatherErr = c.Gather(ctx, rpcClient)
	})
	if gatherErr == nil {
		t.Errorf("expected an error from Gather aborted by context cancellation")
	}
}

// --- Server.Stop waits for connected RPC clients (pkg/server.go) ---

func TestServerStopWithConnectedClient(t *testing.T) {
	port := serverPort()
	s := RunServer(context.Background(), ip, port, nil)

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Let the accept loop register the connection in the WaitGroup.
	time.Sleep(200 * time.Millisecond)

	returnsWithin(t, 2*time.Second, "Server.Stop", s.Stop)
}

// --- RequestRunCommandResult ignores server shutdown (pkg/server.go) ---

func TestRunCommandResultUnblocksOnServerShutdown(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	s := NewServer(srvCtx, nil)
	s.resultC["x"] = make(chan *Result) // never written to

	go func() {
		time.Sleep(100 * time.Millisecond)
		srvCancel()
	}()

	var err error
	returnsWithin(t, 2*time.Second, "RequestRunCommandResult", func() {
		var res RequestRunCommandResultReply
		err = s.RequestRunCommandResult(RequestRunCommandResultArgs{ID: "x"}, &res)
	})
	if err == nil {
		t.Errorf("expected an error after server shutdown")
	}
}

// --- UDPReceiver grace loop must not apply on user abort (pkg/udp.go) ---

// sendEvery sends one small Msg packet from sendConn to raddr every interval,
// count times, and reports how many were sent. It stops early on stop or on a
// write error (e.g. closed socket during test cleanup).
func sendEvery(sendConn *net.UDPConn, raddr net.Addr, interval time.Duration, count int, stop chan struct{}, sent chan<- int) {
	n := 0
	buf := make([]byte, 16)
	var msg Msg
	for i := 0; i < count; i++ {
		select {
		case <-stop:
			sent <- n
			return
		case <-time.After(interval):
		}
		msg.Seq = uint64(i)
		enc, err := msg.Encode(buf)
		if err != nil {
			break
		}
		if _, err := sendConn.WriteTo(buf[:enc], raddr); err != nil {
			break
		}
		n++
	}
	sent <- n
}

func TestUDPReceiverAbortStopsDespiteTraffic(t *testing.T) {
	recvConn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()
	sendConn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sendConn.Close()

	// Traffic keeps flowing well past the abort.
	stop := make(chan struct{})
	defer close(stop)
	sent := make(chan int, 1)
	go sendEvery(sendConn, localAddrOf(recvConn), 20*time.Millisecond, 200 /* 4s */, stop, sent)

	// context.Canceled = user abort: the receiver must return promptly and
	// not keep extending its deadline while packets are still arriving.
	ctx := cancelAfter(200 * time.Millisecond)

	r := &UDPReceiver{}
	ln := NewDummyListener(recvConn, recvConn.LocalAddr())
	returnsWithin(t, 1500*time.Millisecond, "UDPReceiver.Run", func() {
		r.Run(ctx, ln)
	})
}

// Companion regression guard: on *natural* end (context deadline reached),
// the grace loop must keep collecting in-flight packets. This test passes
// today and must stay green while fixing the user-abort path.
func TestUDPReceiverGraceOnDeadline(t *testing.T) {
	recvConn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()
	sendConn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sendConn.Close()

	// The sender outlives the receiver deadline by 300 ms (like a DL sender
	// whose last packets are still in flight when the timeout fires).
	stop := make(chan struct{})
	defer close(stop)
	sent := make(chan int, 1)
	go sendEvery(sendConn, localAddrOf(recvConn), 20*time.Millisecond, 30 /* 600ms */, stop, sent)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	r := &UDPReceiver{}
	ln := NewDummyListener(recvConn, recvConn.LocalAddr())
	var res any
	var runErr error
	returnsWithin(t, 3*time.Second, "UDPReceiver.Run", func() {
		res, runErr = r.Run(ctx, ln)
	})
	if runErr != nil {
		t.Fatalf("UDPReceiver.Run failed: %v", runErr)
	}

	numSent := <-sent
	msgs, ok := res.([]MsgRcvd)
	if !ok {
		t.Fatalf("unexpected result type %T", res)
	}
	if len(msgs) < numSent-3 {
		t.Errorf("grace period lost packets: received %v of %v sent", len(msgs), numSent)
	}
}

// --- Server.Abort(ID) RPC ---

// A server-side sender blocked in its probe wait must deliver an error result
// promptly once the client aborts the test, instead of waiting out the
// first-contact deadline.
func TestAbortRPCEndsServerSenderEarly(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	s := NewServer(srvCtx, nil)

	// StartAt far enough away that the probe wait (bounded at StartAt+1s)
	// only ends early through the Abort below.
	args := RequestServerArgs{
		ID:         "abort-snd",
		Timeout:    20 * time.Second,
		StartAt:    time.Now().Add(9 * time.Second),
		ServerMode: SendRate,
		Params: RateParamsW{{
			Pps:         10,
			Duration:    300 * time.Millisecond,
			PayloadSize: 100,
		}},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}

	// Let the test goroutine reach the probe wait, then abort.
	time.Sleep(100 * time.Millisecond)
	var abortReply AbortReply
	if err := s.Abort(AbortArgs{ID: "abort-snd"}, &abortReply); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	var resErr error
	returnsWithin(t, 2*time.Second, "RequestServerResult", func() {
		var res RequestServerResultReply
		resErr = s.RequestServerResult(RequestServerResultArgs{ID: "abort-snd"}, &res)
	})
	if resErr == nil {
		t.Errorf("expected an error result from an aborted sender test")
	}

	// Aborting a finished or unknown ID is a no-op, not an error: the client
	// fires Abort for every test it scheduled, finished or not.
	if err := s.Abort(AbortArgs{ID: "abort-snd"}, &abortReply); err != nil {
		t.Errorf("Abort of a finished test should be a no-op, got: %v", err)
	}
	if err := s.Abort(AbortArgs{ID: "unknown"}, &abortReply); err != nil {
		t.Errorf("Abort of an unknown ID should be a no-op, got: %v", err)
	}
}

// A server-side receiver aborts with the packets received so far (no error:
// partial data from an aborted round is still delivered if asked for).
func TestAbortRPCEndsServerReceiverEarly(t *testing.T) {
	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	s := NewServer(srvCtx, nil)

	args := RequestServerArgs{
		ID:         "abort-rcv",
		Timeout:    20 * time.Second,
		ServerMode: ReceiveUDP,
		Params:     PeriodicParams{Interval: 10 * time.Millisecond, Duration: 20 * time.Second},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	var abortReply AbortReply
	if err := s.Abort(AbortArgs{ID: "abort-rcv"}, &abortReply); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	var resErr error
	returnsWithin(t, 2*time.Second, "RequestServerResult", func() {
		var res RequestServerResultReply
		resErr = s.RequestServerResult(RequestServerResultArgs{ID: "abort-rcv"}, &res)
	})
	if resErr != nil {
		t.Errorf("expected the partial result of an aborted receiver, got error: %v", resErr)
	}
}

// --- QUIC receive relies on the ~30s idle timeout (pkg/quic.go) ---

// The QUIC receiver's stream read must be woken by ctx cancellation even
// while the peer connection stays open (a vanished/stalled sender otherwise
// keeps it blocked until quic-go's idle timeout).
func TestQUICReceiverReturnsOnCancel(t *testing.T) {
	recvConn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer recvConn.Close()
	sendConn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sendConn.Close()

	r := &QUICReceiver{}
	r.Init()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	var res any
	var runErr error
	go func() {
		defer close(done)
		res, runErr = r.Run(ctx, NewDummyListener(recvConn, recvConn.LocalAddr()))
	}()

	// A sender that connects, writes some data, and then stalls without
	// closing the stream or the connection.
	tr := &quic.Transport{Conn: sendConn}
	defer tr.Close()
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	quicConn, err := tr.Dial(dialCtx, localAddrOf(recvConn),
		&tls.Config{InsecureSkipVerify: true, NextProtos: []string{quicProto}}, nil)
	if err != nil {
		t.Fatalf("QUIC dial failed: %v", err)
	}
	defer quicConn.CloseWithError(0, "")
	stream, err := quicConn.OpenStreamSync(dialCtx)
	if err != nil {
		t.Fatalf("OpenStreamSync failed: %v", err)
	}
	if _, err := stream.Write(make([]byte, 1024)); err != nil {
		t.Fatalf("stream.Write failed: %v", err)
	}

	// Give the receiver time to accept the stream and read the data, then
	// simulate the user abort.
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("QUICReceiver.Run did not return within 2s of cancellation (blocked until the QUIC idle timeout)")
	}
	if runErr != nil {
		t.Fatalf("expected the partial result of an aborted QUIC receiver, got error: %v", runErr)
	}
	if msgs, ok := res.([]MsgRcvd); !ok || len(msgs) == 0 {
		t.Errorf("expected the packets received before the abort, got %T with %v", res, res)
	}
}

// --- RateSender cancelled mid-segment returns promptly ---

func TestRateSenderCancelMidSegment(t *testing.T) {
	conn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	peer, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	s := &RateSender{Params: RateParamsW{
		{Pps: 50, Duration: 400 * time.Millisecond, PayloadSize: 100},
		{Pps: 50, Duration: 400 * time.Millisecond, PayloadSize: 100},
		{Pps: 50, Duration: 400 * time.Millisecond, PayloadSize: 100},
	}}

	// Cancel during segment 2 of 3.
	ctx := cancelAfter(600 * time.Millisecond)

	var res any
	var runErr error
	returnsWithin(t, 2*time.Second, "RateSender.Run", func() {
		res, runErr = s.Run(ctx, conn, localAddrOf(peer))
	})
	if runErr != nil {
		t.Fatalf("expected nil error from a cancelled RateSender, got: %v", runErr)
	}
	if msgs, ok := res.([]MsgSent); !ok || len(msgs) == 0 {
		t.Errorf("expected partial results from a cancelled RateSender, got %T with %v", res, res)
	}
}

// --- handleSender applies a fixed 60s deadline (pkg/server.go) ---

type fakeConn struct {
	deadline time.Time
}

func (c *fakeConn) Read(b []byte) (int, error)         { return 0, io.EOF }
func (c *fakeConn) Write(b []byte) (int, error)        { return len(b), nil }
func (c *fakeConn) Close() error                       { return nil }
func (c *fakeConn) LocalAddr() net.Addr                { return nil }
func (c *fakeConn) RemoteAddr() net.Addr               { return nil }
func (c *fakeConn) SetDeadline(t time.Time) error      { c.deadline = t; return nil }
func (c *fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *fakeConn) SetWriteDeadline(t time.Time) error { return nil }

var _ net.Conn = (*fakeConn)(nil)

type stubParams struct {
	d time.Duration
}

func (p stubParams) GetDuration() time.Duration { return p.d }

type stubSender struct {
	params stubParams
}

func (s *stubSender) GetParams() SenderParams { return s.params }
func (s *stubSender) SenderMode() Mode        { return SendRate }
func (s *stubSender) ReceiverMode() Mode      { return ReceiveUDP }
func (s *stubSender) Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error) {
	return nil, nil
}

func TestHandleSenderAppliesComputedDeadline(t *testing.T) {
	fc := &fakeConn{}
	sender := &stubSender{params: stubParams{d: 2 * time.Minute}}
	args := RequestServerArgs{Params: sender.params}

	before := time.Now()
	if _, err := handleSender(context.Background(), fc, args, sender, nil); err != nil {
		t.Fatalf("handleSender failed: %v", err)
	}

	// Intended behavior: the socket deadline covers the
	// whole test (2x duration => now+4min); today a fixed now+60s is applied.
	minDeadline := before.Add(3 * time.Minute)
	if fc.deadline.Before(minDeadline) {
		t.Errorf("sender socket deadline %v too early for a 2min test; want >= %v",
			fc.deadline, minDeadline)
	}
}
