package pkg

// Tests for the positive timing contract: tests
// must start at StartAt, end at StartAt+Duration, and fail instead of running
// late. The abort/fault half of the termination story lives in
// termination_test.go.
//
// Tolerances come from internal/testutil (StartTol/EndTol); the assertions
// deliberately stay loose because the suite runs on a loaded loopback.
// Timing-sensitive tests here do not run t.Parallel(); the assertions wired
// into the parallel testSender matrix (assertClientTiming) use the same
// loose tolerances.

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/internal/testutil"
)

// assertSenderTiming checks recorded send timestamps against the scheduled
// window: no packet before startAt (a hard bound: starting early corrupts
// the schedule of neighboring tests), the first packet by firstBy, and the
// last packet by endBy.
func assertSenderTiming(t *testing.T, msgs []MsgSent, startAt, firstBy, endBy time.Time) {
	t.Helper()
	if len(msgs) == 0 {
		t.Errorf("no packets recorded for a sender scheduled at %v", startAt)
		return
	}
	first := msgs[0].TsSent
	last := msgs[len(msgs)-1].TsSent
	if first.Before(startAt) {
		t.Errorf("first packet sent at %v, before StartAt %v", first, startAt)
	}
	if first.After(firstBy) {
		t.Errorf("first packet sent at %v, later than %v (StartAt %v)", first, firstBy, startAt)
	}
	if last.After(endBy) {
		t.Errorf("last packet sent at %v, later than %v (StartAt %v)", last, endBy, startAt)
	}
}

// assertClientTiming applies the timing assertions to the send data of a
// finished testSender run. For DL the messages come from the server-side
// sender via Gather, so the same assertions pin the client-vs-server StartAt
// contract (client and server share a clock in these tests).
func assertClientTiming(t *testing.T, c *SenderClient) {
	t.Helper()
	startAt := c.StartAt
	duration := c.Sender.GetParams().GetDuration()
	endBy := startAt.Add(duration + testutil.EndTol)

	switch c.Sender.SenderMode() {
	case SendBurst:
		// BurstParams.Timeout is the receiver window, not a send duration:
		// the whole burst must go out right at StartAt, inside the window.
		assertSenderTiming(t, c.UDPMsgsSent, startAt,
			startAt.Add(testutil.StartTol), startAt.Add(duration))
	case SendPeriodic:
		// The ticker delivers the first packet one interval after StartAt.
		interval := c.Sender.GetParams().(PeriodicParams).Interval
		assertSenderTiming(t, c.UDPMsgsSent, startAt,
			startAt.Add(interval+testutil.StartTol), endBy)
	case SendICMP:
		interval := c.Sender.GetParams().(ICMPParams).Interval
		assertSenderTiming(t, c.UDPMsgsSent, startAt,
			startAt.Add(interval+testutil.StartTol), endBy)
	case SendRate:
		params := c.Sender.GetParams().(RateParamsW)
		if params.NumPackets() == 0 {
			// A pure-cooldown schedule (Pps 0) sends nothing by design.
			return
		}
		// The packet goal accrues with elapsed time: the first packet goes
		// out once elapsed*Pps reaches 1.
		firstDelay := time.Duration(0)
		if params[0].Pps > 0 {
			firstDelay = time.Second / time.Duration(params[0].Pps)
		}
		assertSenderTiming(t, c.UDPMsgsSent, startAt,
			startAt.Add(firstDelay+testutil.StartTol), endBy)
	case SendQUIC:
		// Seqs are QUIC packet numbers but TsSent is still a send timestamp;
		// only 1-RTT packets are traced, so the first one includes the
		// handshake time.
		assertSenderTiming(t, c.UDPMsgsSent, startAt,
			startAt.Add(testutil.StartTol), endBy)
	case SendTCP:
		// No per-packet timestamps; the TCP monitor samples span the send.
		m := c.TCPMetricsSndr
		if len(m) == 0 {
			t.Errorf("no TCP metrics recorded for a sender scheduled at %v", startAt)
			return
		}
		if m[0].Time.Before(startAt) {
			t.Errorf("first TCP metric at %v, before StartAt %v", m[0].Time, startAt)
		}
		if last := m[len(m)-1].Time; last.After(endBy) {
			t.Errorf("last TCP metric at %v, later than %v (StartAt %v)", last, endBy, startAt)
		}
	default:
		t.Fatalf("assertClientTiming: unhandled mode %v", c.Sender.SenderMode())
	}
}

// --- the receive side runs until it is supposed to end, and not longer ---

func TestTimingReceiverEndBound(t *testing.T) {
	RegisterGob()

	port := serverPort()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	server := RunServer(srvCtx, ip, port, nil)
	defer func() {
		srvCancel()
		server.Stop()
	}()

	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()

	duration := 500 * time.Millisecond
	c := &SenderClient{
		IP:        ip,
		Direction: UL,
		StartAt:   time.Now().Add(300 * time.Millisecond),
		Sender: &PeriodicSender{Params: PeriodicParams{
			Interval: time.Millisecond,
			Duration: duration,
		}},
	}

	before := time.Now()
	if err := c.Run(context.Background(), rpcClient); err != nil {
		t.Fatalf("client.Run failed: %v", err)
	}
	runElapsed := time.Since(before)
	if err := c.Gather(context.Background(), rpcClient); err != nil {
		t.Fatalf("client.Gather failed: %v", err)
	}
	gatherElapsed := time.Since(before)

	// The server-side receiver window is Duration + 1s (the Timeout margin
	// set in runUL) and the UDPReceiver grace loop may extend it by 250ms.
	graceBudget := 1*time.Second + 250*time.Millisecond
	bound := c.StartAt.Add(duration + graceBudget + testutil.EndTol)

	if len(c.UDPMsgsRcvd) == 0 {
		t.Fatalf("no packets received")
	}
	if last := c.UDPMsgsRcvd[len(c.UDPMsgsRcvd)-1].TsRcvd; last.After(bound) {
		t.Errorf("last packet received at %v, later than %v", last, bound)
	}

	// The client calls must not outlive the scheduled window either.
	maxWall := c.StartAt.Sub(before) + duration + graceBudget + testutil.EndTol
	if runElapsed > maxWall {
		t.Errorf("c.Run took %v, longer than %v", runElapsed, maxWall)
	}
	if gatherElapsed > maxWall {
		t.Errorf("c.Run+Gather took %v, longer than %v", gatherElapsed, maxWall)
	}
}

// --- a StartAt already in the past must fail, not run late ---

func TestStartAtInPastErrors(t *testing.T) {
	RegisterGob()

	port := serverPort()
	srvCtx, srvCancel := context.WithCancel(context.Background())
	server := RunServer(srvCtx, ip, port, nil)
	defer func() {
		srvCancel()
		server.Stop()
	}()

	rpcClient, err := dialRpcClient(ip, port)
	if err != nil {
		t.Fatal(err)
	}
	defer rpcClient.Close()

	c := &SenderClient{
		IP:        ip,
		Direction: UL,
		StartAt:   time.Now().Add(-1 * time.Second),
		Sender: &PeriodicSender{Params: PeriodicParams{
			Interval: time.Millisecond,
			Duration: 200 * time.Millisecond,
		}},
	}

	var runErr error
	returnsWithin(t, 5*time.Second, "SenderClient.Run", func() {
		runErr = c.Run(context.Background(), rpcClient)
	})
	if runErr == nil {
		t.Errorf("expected an error from a StartAt in the past")
	}
	if len(c.UDPMsgsSent) != 0 {
		t.Errorf("expected zero packets sent, got %v", len(c.UDPMsgsSent))
	}
}

func TestWaitUntilPastErrors(t *testing.T) {
	if err := waitUntil(context.Background(), time.Now().Add(-time.Second)); err == nil {
		t.Errorf("expected an error for a startAt in the past")
	}
}

func TestWaitUntilZeroReturnsImmediately(t *testing.T) {
	before := time.Now()
	if err := waitUntil(context.Background(), time.Time{}); err != nil {
		t.Errorf("expected nil error for a zero startAt, got: %v", err)
	}
	if elapsed := time.Since(before); elapsed > 50*time.Millisecond {
		t.Errorf("waitUntil with zero startAt took %v", elapsed)
	}
}

func TestWaitUntilFutureWakesOnTime(t *testing.T) {
	startAt := time.Now().Add(200 * time.Millisecond)
	if err := waitUntil(context.Background(), startAt); err != nil {
		t.Fatalf("expected nil error, got: %v", err)
	}
	now := time.Now()
	if now.Before(startAt) {
		t.Errorf("waitUntil returned at %v, before startAt %v", now, startAt)
	}
	if late := startAt.Add(testutil.StartTol); now.After(late) {
		t.Errorf("waitUntil returned at %v, later than %v", now, late)
	}
}

func TestWaitUntilCancelledDuringWait(t *testing.T) {
	ctx := cancelAfter(100 * time.Millisecond)
	var err error
	returnsWithin(t, time.Second, "waitUntil", func() {
		err = waitUntil(ctx, time.Now().Add(10*time.Second))
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

// --- a late probe reply must abort the DL test, not start it late ---

// A probe reply arriving only after StartAt means the measurement window is
// already compromised; punchHole must return the "StartAt already passed"
// error instead of proceeding (the dangerous silent failure for OWD data).
func TestPunchHoleLateReplyFails(t *testing.T) {
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

	startAt := time.Now().Add(300 * time.Millisecond)
	c := &SenderClient{StartAt: startAt}

	// The "server" deliberately replies only after StartAt.
	go func() {
		var buf [64]byte
		_, raddr, err := peer.ReadFrom(buf[:])
		if err != nil {
			return
		}
		time.Sleep(time.Until(startAt.Add(100 * time.Millisecond)))
		peer.WriteTo([]byte{}, raddr)
	}()

	var punchErr error
	returnsWithin(t, 2*time.Second, "punchHole", func() {
		punchErr = c.punchHole(context.Background(), conn, localAddrOf(peer), []byte{})
	})
	if punchErr == nil {
		t.Errorf("expected an error from a probe reply arriving after StartAt")
	}
}

// Happy path: with a prompt replier the probing must finish before StartAt
// (it must never eat into the measurement window).
func TestPunchHolePromptReplyFinishesBeforeStartAt(t *testing.T) {
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

	startAt := time.Now().Add(300 * time.Millisecond)
	c := &SenderClient{StartAt: startAt}

	go func() {
		var buf [64]byte
		_, raddr, err := peer.ReadFrom(buf[:])
		if err != nil {
			return
		}
		peer.WriteTo([]byte{}, raddr)
	}()

	var punchErr error
	returnsWithin(t, 2*time.Second, "punchHole", func() {
		punchErr = c.punchHole(context.Background(), conn, localAddrOf(peer), []byte{})
	})
	if punchErr != nil {
		t.Fatalf("expected nil error from a prompt probe reply, got: %v", punchErr)
	}
	if now := time.Now(); !now.Before(startAt) {
		t.Errorf("punchHole returned at %v, at or after StartAt %v", now, startAt)
	}
}

// --- the probe retry loop must respect the StartAt deadline cap ---

// punchHole caps each probe deadline at StartAt. With a server that never
// replies it must give up at StartAt instead of retrying 5x1s.
func TestPunchHoleDeadlineCappedByStartAt(t *testing.T) {
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
	// peer never replies

	startAt := time.Now().Add(200 * time.Millisecond)
	c := &SenderClient{StartAt: startAt}

	var punchErr error
	returnsWithin(t, 2*time.Second, "punchHole", func() {
		punchErr = c.punchHole(context.Background(), conn, localAddrOf(peer), []byte{})
	})
	if punchErr == nil {
		t.Errorf("expected an error when no probe reply ever arrives")
	}
	if now, late := time.Now(), startAt.Add(testutil.StartTol); now.After(late) {
		t.Errorf("punchHole gave up at %v, later than %v: the retry loop must cap its deadline at StartAt", now, late)
	}
}

// --- firstContactDeadline (pure function of time, three branches) ---

func TestFirstContactDeadline(t *testing.T) {
	// Bands account for the time.Now() calls inside firstContactDeadline
	// happening slightly after the ones here.
	within := func(name string, got, lo, hi time.Time) {
		t.Helper()
		if got.Before(lo) || got.After(hi) {
			t.Errorf("%s: deadline %v outside [%v, %v]", name, got, lo, hi)
		}
	}

	// Zero startAt: ~now+10s.
	now := time.Now()
	d := firstContactDeadline(time.Time{})
	within("zero", d, now.Add(10*time.Second), time.Now().Add(10*time.Second+100*time.Millisecond))

	// Normal future startAt: exactly startAt+1s.
	startAt := time.Now().Add(5 * time.Second)
	if d := firstContactDeadline(startAt); !d.Equal(startAt.Add(time.Second)) {
		t.Errorf("future: deadline %v, want %v", d, startAt.Add(time.Second))
	}

	// startAt near now (startAt+1s below the floor): floor of now+2s.
	now = time.Now()
	d = firstContactDeadline(now.Add(500 * time.Millisecond))
	within("near", d, now.Add(2*time.Second), time.Now().Add(2*time.Second+100*time.Millisecond))

	// startAt behind now: also the now+2s floor.
	now = time.Now()
	d = firstContactDeadline(now.Add(-5 * time.Second))
	within("past", d, now.Add(2*time.Second), time.Now().Add(2*time.Second+100*time.Millisecond))
}

// --- server-side scheduling is honored without a client-side wait ---

// The duration clock of a server-side sender must start at StartAt, not at
// probe arrival: RequestServer directly, probe immediately, and check when
// the first data packet arrives (handleSender's waitUntil ->
// WithTimeout(GetDuration()) sequencing).
func TestTimingServerHonorsStartAt(t *testing.T) {
	RegisterGob()

	srvCtx, srvCancel := context.WithCancel(context.Background())
	defer srvCancel()
	s := NewServer(srvCtx, nil)

	pps := uint(200)
	startAt := time.Now().Add(400 * time.Millisecond)
	args := RequestServerArgs{
		ID:         "t6",
		Timeout:    2 * time.Second,
		StartAt:    startAt,
		ServerMode: SendRate,
		Params: RateParamsW{{
			Pps:         pps,
			Duration:    300 * time.Millisecond,
			PayloadSize: 100,
		}},
	}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatalf("RequestServer failed: %v", err)
	}

	conn, err := listenUDP(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	raddr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: int(reply.Port)}

	// Send the NAT probe right away, well before StartAt.
	if _, err := conn.WriteTo([]byte{}, raddr); err != nil {
		t.Fatal(err)
	}

	// The first non-empty packet is the first data packet (the empty one is
	// the server's probe reply).
	if err := conn.SetReadDeadline(startAt.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var firstData time.Time
	var buf [2048]byte
	for {
		n, _, err := conn.ReadFrom(buf[:])
		if err != nil {
			break // read deadline: no data packet at all
		}
		if n == 0 {
			continue
		}
		firstData = time.Now()
		break
	}
	if firstData.IsZero() {
		t.Fatalf("no data packet received from the server-side sender")
	}
	if firstData.Before(startAt) {
		t.Errorf("first data packet at %v, before StartAt %v", firstData, startAt)
	}
	// The rate sender emits its first packet once elapsed*Pps reaches 1.
	firstDelay := time.Second / time.Duration(pps)
	if late := startAt.Add(firstDelay + testutil.StartTol); firstData.After(late) {
		t.Errorf("first data packet at %v, later than %v: the send window must start at StartAt, not at probe arrival", firstData, late)
	}

	// Drain the result so the server goroutine finishes cleanly.
	returnsWithin(t, 5*time.Second, "RequestServerResult", func() {
		var res RequestServerResultReply
		s.RequestServerResult(RequestServerResultArgs{ID: "t6"}, &res)
	})
}
