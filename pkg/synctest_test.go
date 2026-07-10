package pkg

// Exact-time variants of the waitUntil tests under the
// testing/synctest fake clock — zero flakiness, exact assertions instead of
// tolerance bands. Only pure timer logic runs here; the senders touch real
// sockets and are exercised more honestly by the integration tests.

import (
	"context"
	"testing"
	"testing/synctest"
	"time"
)

func TestWaitUntilSynctestExact(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		startAt := time.Now().Add(3 * time.Second)
		if err := waitUntil(context.Background(), startAt); err != nil {
			t.Fatalf("expected nil error, got: %v", err)
		}
		if now := time.Now(); !now.Equal(startAt) {
			t.Errorf("waitUntil returned at %v, want exactly %v", now, startAt)
		}
	})
}

func TestWaitUntilSynctestZero(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := time.Now()
		if err := waitUntil(context.Background(), time.Time{}); err != nil {
			t.Fatalf("expected nil error for a zero startAt, got: %v", err)
		}
		if now := time.Now(); !now.Equal(before) {
			t.Errorf("waitUntil with zero startAt advanced the clock: %v -> %v", before, now)
		}
	})
}

func TestWaitUntilSynctestPast(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if err := waitUntil(context.Background(), time.Now().Add(-time.Nanosecond)); err == nil {
			t.Errorf("expected an error for a startAt in the past")
		}
	})
}

func TestWaitUntilSynctestCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()

		before := time.Now()
		err := waitUntil(ctx, before.Add(10*time.Second))
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
		if elapsed := time.Since(before); elapsed != time.Second {
			t.Errorf("waitUntil returned after %v, want exactly 1s (the cancellation time)", elapsed)
		}
	})
}
