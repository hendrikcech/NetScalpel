// Package testutil holds helpers shared by the test trees under pkg and
// cmd/scalpel-exp. It is a regular package (not _test)
// so both trees can import it; internal/ keeps it out of the public API.
package testutil

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Tolerances for real-time timing assertions. Loopback plus parallel test
// load means tens of ms of scheduling noise; never assert single-digit-ms
// precision.
const (
	// StartTol bounds how late after StartAt the first packet may appear.
	StartTol = 150 * time.Millisecond
	// EndTol bounds how late after the nominal end the last packet may appear.
	EndTol = 300 * time.Millisecond
)

// ReturnsWithin runs fn and fails the test if it does not return within d.
func ReturnsWithin(t *testing.T, d time.Duration, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", name, d)
	}
}

// CancelAfter returns a context that is cancelled after d (context.Canceled,
// i.e. a user abort, as opposed to a deadline expiry).
func CancelAfter(d time.Duration) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(d)
		cancel()
	}()
	return ctx
}

// PortCounter hands out sequential ports for test RPC servers. Every test
// binary uses its own base so that parallel `go test ./...` runs of the
// different packages cannot collide.
type PortCounter struct {
	c atomic.Uint32
}

func NewPortCounter(base uint32) *PortCounter {
	p := &PortCounter{}
	p.c.Store(base)
	return p
}

func (p *PortCounter) Next() uint {
	return uint(p.c.Add(1))
}

// LocalAddrOf returns a dialable loopback address for a wildcard-bound UDP socket.
func LocalAddrOf(conn *net.UDPConn) *net.UDPAddr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: conn.LocalAddr().(*net.UDPAddr).Port}
}
