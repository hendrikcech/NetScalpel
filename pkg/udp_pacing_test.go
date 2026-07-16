package pkg

// Tests for the RateSender pacing math (rateInterval, numPacketsGoal,
// packetsDue), extracted into pure functions because the timing behavior of
// the real send loop cannot be asserted reliably against sockets.

import (
	"math/big"
	"math/rand"
	"testing"
	"time"
)

func TestRateInterval(t *testing.T) {
	cases := []struct {
		pps  uint
		want time.Duration
	}{
		{pps: 1, want: time.Millisecond},    // 5s calculated, capped to 1ms
		{pps: 5, want: time.Millisecond},    // 1s calculated, capped
		{pps: 5000, want: time.Millisecond}, // exactly at the cap
		{pps: 50000, want: 100 * time.Microsecond},
		{pps: 500000, want: 10 * time.Microsecond},
	}
	for _, c := range cases {
		if got := rateInterval(c.pps); got != c.want {
			t.Errorf("rateInterval(%d) = %v; want %v", c.pps, got, c.want)
		}
	}
}

func TestNumPacketsGoal(t *testing.T) {
	cases := []struct {
		elapsed time.Duration
		pps     uint
		want    uint
	}{
		{elapsed: 0, pps: 100, want: 0},
		{elapsed: -time.Second, pps: 100, want: 0},
		{elapsed: time.Second, pps: 100, want: 100},
		{elapsed: 100 * time.Millisecond, pps: 3, want: 0}, // 0.3
		{elapsed: 333 * time.Millisecond, pps: 3, want: 0}, // 0.999
		{elapsed: 334 * time.Millisecond, pps: 3, want: 1}, // 1.002
		{elapsed: time.Nanosecond, pps: 1_000_000_000, want: 1},
		{elapsed: time.Hour, pps: 123456, want: 123456 * 3600},
	}
	for _, c := range cases {
		if got := numPacketsGoal(c.elapsed, c.pps); got != c.want {
			t.Errorf("numPacketsGoal(%v, %d) = %d; want %d", c.elapsed, c.pps, got, c.want)
		}
	}
}

func TestPacketsDue(t *testing.T) {
	// 500ms at 10pps: goal is 5.
	if due, ahead := packetsDue(500*time.Millisecond, 10, 2); due != 3 || ahead {
		t.Errorf("packetsDue = (%d, %v); want (3, false)", due, ahead)
	}
	if due, ahead := packetsDue(500*time.Millisecond, 10, 5); due != 0 || ahead {
		t.Errorf("packetsDue at goal = (%d, %v); want (0, false)", due, ahead)
	}
	// Sent ahead of the goal (the observed-in-the-field anomaly): nothing is
	// due and the caller is told to warn, not panic.
	if due, ahead := packetsDue(500*time.Millisecond, 10, 7); due != 0 || !ahead {
		t.Errorf("packetsDue ahead of goal = (%d, %v); want (0, true)", due, ahead)
	}
}

// Simulates a segment tick by tick: at every tick the loop sends what
// packetsDue asks for. After simulated time T at rate P, exactly ⌊T·P⌋
// packets must have been requested (checked against a big.Int oracle so the
// test does not share rounding behavior with the implementation), and the
// running total must never exceed the goal. Ticks get random lateness, as
// real tickers deliver late under load.
func TestRatePacingSimulatedRun(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	for _, pps := range []uint{1, 7, 100, 4999, 5000, 12345, 250000} {
		for _, total := range []time.Duration{time.Second, 2500 * time.Millisecond} {
			interval := rateInterval(pps)
			sent := uint(0)
			var elapsed time.Duration
			for elapsed < total {
				elapsed += interval + time.Duration(rng.Int63n(int64(interval/4)+1))
				if elapsed > total {
					elapsed = total // final tick lands on the segment end
				}
				due, ahead := packetsDue(elapsed, pps, sent)
				if ahead {
					t.Fatalf("pps=%d: sent %d ran ahead of the goal at %v", pps, sent, elapsed)
				}
				sent += due
			}

			want := new(big.Int).Mul(big.NewInt(int64(total)), new(big.Int).SetUint64(uint64(pps)))
			want.Div(want, big.NewInt(int64(time.Second)))
			if want.Uint64() != uint64(sent) {
				t.Errorf("pps=%d T=%v: %d packets requested; want exactly %v", pps, total, sent, want)
			}
		}
	}
}
