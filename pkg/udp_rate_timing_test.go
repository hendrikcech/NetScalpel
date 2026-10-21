package pkg

import (
	"context"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
)

type rateTimingBatch struct {
	at      time.Time
	n       int
	payload int
}

// No real sockets: both a blocked write and its deadline are driven by the
// synctest clock, including cancellation's concurrent deadline update.
type rateTimingSocket struct {
	mu         sync.Mutex
	deadline   time.Time
	changed    chan struct{}
	batches    []rateTimingBatch
	blockFirst bool
	stallFirst time.Duration
}

func (s *rateTimingSocket) SetWriteDeadline(d time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadline = d
	if s.changed != nil {
		close(s.changed)
	}
	s.changed = make(chan struct{})
	return nil
}

func (s *rateTimingSocket) SendMsgs(msgs []mmsg.Message, _ int) (int, error) {
	first := len(s.batches) == 0
	s.batches = append(s.batches, rateTimingBatch{time.Now(), len(msgs), len(msgs[0].Buffers[0]) - 8})
	if first && s.blockFirst {
		for {
			s.mu.Lock()
			d, changed := s.deadline, s.changed
			s.mu.Unlock()
			if !d.IsZero() && !time.Now().Before(d) {
				return 0, &net.OpError{Op: "write", Net: "udp", Err: context.DeadlineExceeded}
			}
			timer := time.NewTimer(time.Until(d))
			select {
			case <-timer.C:
			case <-changed:
			}
			timer.Stop()
		}
	}
	if first && s.stallFirst > 0 {
		time.Sleep(s.stallFirst)
	}
	return len(msgs), nil
}

func newRateTimingSender(params RateParamsW) *RateSender {
	r := &RateSender{Params: params, tx: make([]mmsg.Message, 1024)}
	for i := range r.tx {
		r.tx[i].Buffers = [][]byte{make([]byte, 1500)}
	}
	return r
}

func TestRateTimingNominalBoundaries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		r := newRateTimingSender(RateParamsW{
			{Pps: 1000, Duration: 10 * time.Millisecond, PayloadSize: 1},
			{Pps: 1000, Duration: 10 * time.Millisecond, PayloadSize: 2},
			{Pps: 1000, Duration: 20 * time.Millisecond, PayloadSize: 3},
		})
		s := &rateTimingSocket{stallFirst: 25 * time.Millisecond}
		if err := r.runSegments(context.Background(), s, s, nil, start); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 40*time.Millisecond {
			t.Fatalf("schedule extended to %v", elapsed)
		}
		for _, b := range s.batches {
			if b.payload == 2 {
				t.Fatal("fully expired segment was replayed")
			}
			if b.n != 1 {
				t.Fatalf("late segment entry caught up missed demand: %+v", b)
			}
			if b.payload == 3 && (!b.at.Before(start.Add(40*time.Millisecond)) || b.at.Before(start.Add(27*time.Millisecond))) {
				t.Fatalf("bad live-segment send time: %+v", b)
			}
		}
		if r.seq != 14 {
			t.Fatalf("sent %d, want first packet plus 13 live ticks", r.seq)
		}
	})
}

func TestRateTimingSkipExpiredAndNonpositive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		r := newRateTimingSender(RateParamsW{
			{Pps: 1000, Duration: 10 * time.Millisecond},
			{Duration: 0}, {Pps: 1000, Duration: -time.Second},
			{Duration: 10 * time.Millisecond}, // already expired cooldown
			{Pps: 1000, Duration: 20 * time.Millisecond},
		})
		s := &rateTimingSocket{}
		if err := r.runSegments(context.Background(), s, s, nil, now.Add(-25*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(now); elapsed != 15*time.Millisecond {
			t.Fatalf("remaining schedule = %v", elapsed)
		}
		if r.seq != 14 {
			t.Fatalf("sent %d, want 14 (missed demand dropped)", r.seq)
		}
		for _, b := range s.batches {
			if b.n != 1 {
				t.Fatalf("entry burst: %+v", b)
			}
		}
		if got := (RateParams{Pps: 1000, Duration: -time.Second}).NumPackets(); got != 0 {
			t.Fatalf("negative duration budget = %d", got)
		}
	})
}

func TestRateTimingEqualSegmentsStayIndependent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRateTimingSender(RateParamsW{
			{Pps: 750, Duration: 3 * time.Millisecond},
			{Pps: 750, Duration: 3 * time.Millisecond},
			{Duration: 3 * time.Millisecond},
		})
		s := &rateTimingSocket{}
		start := time.Now()
		if err := r.runSegments(context.Background(), s, s, nil, start); err != nil {
			t.Fatal(err)
		}
		// Each independent segment sends once, at its 2ms tick. A merged
		// 6ms segment instead sends three packets, at 2ms, 3ms and 4ms.
		if r.seq != 2 || len(s.batches) != 2 {
			t.Fatalf("independent segments sent %d packets in %d batches, want 2", r.seq, len(s.batches))
		}
		for i, elapsed := range []time.Duration{2 * time.Millisecond, 5 * time.Millisecond} {
			if b := s.batches[i]; b.n != 1 || !b.at.Equal(start.Add(elapsed)) {
				t.Fatalf("batch %d = %+v, want one packet at %v", i, b, elapsed)
			}
		}
		if time.Since(start) != 9*time.Millisecond {
			t.Fatal("cooldown did not retain its nominal duration")
		}
	})
}

// Gate only the first segment's cancellation wakeup, before it can mutate
// the deadline. Recording outside the underlying socket also detects an
// attempted clear or next-segment setup while that callback is blocked.
type rateGatedDeadliner struct {
	socket  *rateTimingSocket
	end     time.Time
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	calls   []time.Time
}

func (s *rateGatedDeadliner) SetWriteDeadline(d time.Time) error {
	if d.Equal(s.end) && !d.After(time.Now()) {
		close(s.entered)
		<-s.release
	}
	s.mu.Lock()
	s.calls = append(s.calls, d)
	s.mu.Unlock()
	return s.socket.SetWriteDeadline(d)
}

func (s *rateGatedDeadliner) deadlines() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Time(nil), s.calls...)
}

func (s *rateGatedDeadliner) SendMsgs([]mmsg.Message, int) (int, error) {
	// Keep the first segment inside a write until its cancellation callback
	// has started. Otherwise cleanup may legitimately stop the callback
	// before it runs, leaving the test waiting forever for entered.
	<-s.entered
	return 0, &net.OpError{Op: "write", Net: "udp", Err: context.DeadlineExceeded}
}

func TestRateTimingCancellationCallbackJoinedBeforeReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		r := newRateTimingSender(RateParamsW{{Pps: 1000, Duration: time.Second}, {Duration: time.Second}})
		socket := &rateTimingSocket{}
		s := &rateGatedDeadliner{socket: socket, end: start.Add(time.Second), entered: make(chan struct{}), release: make(chan struct{})}
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(s.release) }) }
		defer release()
		done := make(chan error, 1)
		go func() { done <- r.runSegments(context.Background(), s, s, nil, start) }()
		synctest.Wait()
		time.Sleep(time.Second)
		<-s.entered
		// The callback is held inside SetWriteDeadline. With ctx.Done ready,
		// Wait lets segment cleanup run until it blocks joining that callback.
		synctest.Wait()
		if calls := s.deadlines(); len(calls) != 1 || !calls[0].Equal(s.end) {
			t.Fatalf("deadline cleared or next segment started before callback completed: %v", calls)
		}
		select {
		case err := <-done:
			t.Fatalf("sender returned before cancellation callback completed: %v", err)
		default:
		}
		release()
		synctest.Wait()
		want := []time.Time{s.end, s.end, {}, start.Add(2 * time.Second)}
		calls := s.deadlines()
		if len(calls) != len(want) {
			t.Fatalf("deadline sequence after release: %v", calls)
		}
		for i := range want {
			if !calls[i].Equal(want[i]) {
				t.Fatalf("deadline call %d = %v, want %v", i, calls[i], want[i])
			}
		}
		time.Sleep(time.Second)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !socket.deadline.IsZero() {
			t.Fatal("final deadline not cleared")
		}
	})
}

func TestRateTimingFractionalLateEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entry := time.Now()
		r := newRateTimingSender(RateParamsW{{Pps: 750, Duration: 10 * time.Millisecond}})
		s := &rateTimingSocket{}
		if err := r.runSegments(context.Background(), s, s, nil, entry.Add(-2500*time.Microsecond)); err != nil {
			t.Fatal(err)
		}
		// Nominal entry demand is 1.875 packets: discard the one missed
		// whole packet, retain the segment's phase, and never replay it.
		want := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 5 * time.Millisecond, 6 * time.Millisecond, 7 * time.Millisecond}
		if len(s.batches) != len(want) || r.seq != uint64(len(want)) {
			t.Fatalf("late entry sent %d packets in %d batches, want %d", r.seq, len(s.batches), len(want))
		}
		for i, elapsed := range want {
			if b := s.batches[i]; b.n != 1 || !b.at.Equal(entry.Add(elapsed)) {
				t.Fatalf("batch %d = %+v, want one packet at %v", i, b, elapsed)
			}
		}
		if time.Since(entry) != 7500*time.Microsecond {
			t.Fatalf("fractional entry extended schedule: %v", time.Since(entry))
		}
	})
}

func TestRateTimingLiveMultiBatchCatchUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		r := newRateTimingSender(RateParamsW{{Pps: 100_000, Duration: 30 * time.Millisecond}})
		s := &rateTimingSocket{stallFirst: 25 * time.Millisecond}
		if err := r.runSegments(context.Background(), s, s, nil, start); err != nil {
			t.Fatal(err)
		}
		// The first 5-packet send stalls while its segment remains live.
		// Preserve all 2500 packets of live debt, split only by batch size.
		for i, n := range []int{5, 1024, 1024, 452} {
			if len(s.batches) <= i {
				t.Fatalf("missing catch-up batch %d", i)
			}
			wantAt := start.Add(50 * time.Microsecond)
			if i > 0 {
				wantAt = wantAt.Add(25 * time.Millisecond)
			}
			if b := s.batches[i]; b.n != n || !b.at.Equal(wantAt) {
				t.Fatalf("batch %d = %+v, want %d packets at %v", i, b, n, wantAt)
			}
		}
		if r.seq != 2995 || time.Since(start) != 30*time.Millisecond {
			t.Fatalf("live catch-up sent=%d elapsed=%v", r.seq, time.Since(start))
		}
		for i, msg := range r.msgs {
			if msg.Seq != uint64(i) || !msg.TsSent.Before(start.Add(30*time.Millisecond)) {
				t.Fatalf("invalid recorded packet %d: %+v", i, msg)
			}
		}
	})
}

func TestRateTimingBlockedWriteAndDeadlineReset(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRateTimingSender(RateParamsW{
			{Pps: 1000, Duration: 10 * time.Millisecond, PayloadSize: 1},
			{Pps: 1000, Duration: 10 * time.Millisecond, PayloadSize: 2},
		})
		s := &rateTimingSocket{blockFirst: true}
		start := time.Now()
		if err := r.runSegments(context.Background(), s, s, nil, start); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 20*time.Millisecond || r.seq != 9 {
			t.Fatalf("elapsed=%v sent=%d", time.Since(start), r.seq)
		}
		if !s.deadline.IsZero() {
			t.Fatal("write deadline leaked after return")
		}
		for _, b := range s.batches[1:] {
			if b.payload != 2 || b.n != 1 {
				t.Fatalf("bad second segment: %+v", b)
			}
		}
	})
}

func TestRateTimingOuterStop(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "deadline"}[timeout], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				ctx, cancel := context.WithCancel(context.Background())
				if timeout {
					cancel()
					ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
				} else {
					go func() { time.Sleep(5 * time.Millisecond); cancel() }()
				}
				defer cancel()
				r := newRateTimingSender(RateParamsW{{Pps: 1000, Duration: time.Second}, {Pps: 1000, Duration: time.Second}})
				s := &rateTimingSocket{blockFirst: true}
				if err := r.runSegments(ctx, s, s, nil, start); err != nil {
					t.Fatal(err)
				}
				if time.Since(start) != 5*time.Millisecond || r.seq != 0 || len(s.batches) != 1 {
					t.Fatalf("elapsed=%v sent=%d attempts=%d", time.Since(start), r.seq, len(s.batches))
				}
				if !s.deadline.IsZero() {
					t.Fatal("cancellation callback left stale write deadline")
				}
			})
		})
	}
}

// Models a deadline whose cancellation callback has not run yet.
type rateLateDeadline struct {
	context.Context
	end time.Time
}

func (c rateLateDeadline) Deadline() (time.Time, bool) { return c.end, true }

func TestRateTimingDeadlineWithoutCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx := rateLateDeadline{context.Background(), start.Add(time.Millisecond)}
		r := newRateTimingSender(nil)
		s := &rateTimingSocket{stallFirst: time.Millisecond}
		n, err := r.sendRound(ctx, s, nil, 3000, 0)
		if err != nil || n != 1024 || len(s.batches) != 1 {
			t.Fatalf("sent=%d attempts=%d err=%v", n, len(s.batches), err)
		}
		n, err = r.sendRound(ctx, s, nil, 1, 0)
		if err != nil || n != 0 || ctx.Err() != nil {
			t.Fatalf("expired but uncanceled context: sent=%d err=%v ctx=%v", n, err, ctx.Err())
		}
	})
}

func TestRateTimingGoalBudget(t *testing.T) {
	start := time.Unix(0, 0)
	end := start.Add(10 * time.Millisecond)
	for _, elapsed := range []time.Duration{10 * time.Millisecond, time.Second} {
		if got := rateSegmentGoal(start.Add(elapsed), start, end, 1000, 3); got != 7 {
			t.Fatalf("goal at %v = %d, want capped remaining budget 7", elapsed, got)
		}
	}
	if got := rateSegmentGoal(start.Add(5*time.Millisecond), start, end, 1000, 3); got != 2 {
		t.Fatalf("live cumulative catch-up goal = %d, want 2", got)
	}
}

func TestRateTimingInactiveSchedule(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		params := RateParamsW{{Pps: 1000, Duration: time.Second}, {Duration: -time.Second}, {Duration: 0}}
		if got := params.GetDuration(); got != time.Second {
			t.Fatalf("nonpositive segment changed total duration: %v", got)
		}
		for _, ctx := range []context.Context{context.Background(), rateLateDeadline{context.Background(), start}} {
			r := newRateTimingSender(params)
			s := &rateTimingSocket{}
			if err := r.runSegments(ctx, s, s, nil, start.Add(-2*time.Second)); err != nil {
				t.Fatal(err)
			}
			if len(s.batches) != 0 || s.changed != nil || time.Now() != start {
				t.Fatal("inactive schedule touched socket or advanced time")
			}
		}
		r := newRateTimingSender(nil)
		for _, duration := range []time.Duration{0, -time.Second} {
			if err := r.runParams(context.Background(), nil, nil, RateParams{Duration: duration}); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestRateTimingCooldownCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() { time.Sleep(5 * time.Millisecond); cancel() }()
		r := newRateTimingSender(RateParamsW{{Duration: time.Second}, {Pps: 1000, Duration: time.Second}})
		s := &rateTimingSocket{}
		if err := r.runSegments(ctx, s, s, nil, start); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) != 5*time.Millisecond || len(s.batches) != 0 || !s.deadline.IsZero() {
			t.Fatalf("cooldown abort: elapsed=%v batches=%d deadline=%v", time.Since(start), len(s.batches), s.deadline)
		}
	})
}
