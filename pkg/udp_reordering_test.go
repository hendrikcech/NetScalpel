package pkg

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// Both cases use real loopback UDP sockets and the production receiver. The
// queued case puts every datagram in the socket before Run starts, exercising
// batched reception without relying on scheduler timing to form a batch.
func TestUDPReceiverReordering(t *testing.T) {
	for _, queued := range []bool{false, true} {
		name := "live"
		if queued {
			name = "queued_batch"
		}
		t.Run(name, func(t *testing.T) {
			recv, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer recv.Close()
			send, err := net.DialUDP("udp4", nil, recv.LocalAddr().(*net.UDPAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer send.Close()
			if err := send.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
				t.Fatal(err)
			}

			// Enable before enqueueing: Linux cannot timestamp packets
			// retroactively. Verify the option rather than trusting the setup
			// helper's return value; no userspace fallback is allowed here.
			requireUDPReceiveTimestamping(t, recv)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			type outcome struct {
				value any
				err   error
			}
			done := make(chan outcome, 1)
			start := func() {
				go func() {
					r := &UDPReceiver{}
					value, err := r.Run(ctx, NewDummyListener(recv, recv.LocalAddr()))
					done <- outcome{value, err}
				}()
			}
			if !queued {
				// Run enables timestamping again. Wait until it has consumed
				// a zero-length probe before sending measured packets, so no
				// write races with that socket-option initialization.
				if _, err := send.Write(nil); err != nil {
					t.Fatal(err)
				}
				start()
				waitUDPQueueDrained(t, recv)
			}

			order := []uint64{0, 1, 2, 3, 5, 4}
			sent := make([]MsgSent, len(order))
			before := make(map[uint64]time.Time)
			after := make(map[uint64]time.Time)
			for _, seq := range order {
				buf := make([]byte, 64)
				msg := Msg{Seq: seq, PadN: uint(seq)}
				n, err := msg.Encode(buf)
				if err != nil {
					t.Fatal(err)
				}
				before[seq] = time.Now()
				if _, err := send.Write(buf[:n]); err != nil {
					t.Fatal(err)
				}
				sent[seq] = MsgSent{Seq: seq, TsSent: before[seq], Len: uint(n)}
				// Separate kernel arrival times, even on coarse clocks. The
				// upper bound is before the next write, not receiver read time.
				time.Sleep(2 * time.Millisecond)
				after[seq] = time.Now()
				if !queued {
					waitUDPQueueDrained(t, recv)
				}
			}
			if queued {
				start()
			}
			// Wait for actual consumption rather than sleeping for an
			// assumed scheduling delay. User cancellation avoids grace.
			waitUDPQueueDrained(t, recv)
			cancel()
			var out outcome
			select {
			case out = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("UDPReceiver.Run did not stop")
			}
			if out.err != nil {
				t.Fatal(out.err)
			}
			received, ok := out.value.([]MsgRcvd)
			if !ok {
				t.Fatalf("unexpected receiver result %T", out.value)
			}
			if len(received) != len(order) {
				t.Fatalf("received %d packets, want %d", len(received), len(order))
			}
			for i, r := range received {
				if r.Seq != order[i] {
					t.Fatalf("arrival %d has seq %d, want %d", i, r.Seq, order[i])
				}
				if r.Len != sent[r.Seq].Len {
					t.Errorf("seq %d length = %d, want %d", r.Seq, r.Len, sent[r.Seq].Len)
				}
				if r.TsRcvd.Before(before[r.Seq]) || r.TsRcvd.After(after[r.Seq]) {
					t.Errorf("seq %d receive timestamp %v outside send window [%v, %v]; missing or misassigned kernel timestamp",
						r.Seq, r.TsRcvd, before[r.Seq], after[r.Seq])
				}
				if i > 0 && !received[i-1].TsRcvd.Before(r.TsRcvd) {
					t.Errorf("arrival timestamps not strictly increasing: %v then %v", received[i-1], r)
				}
			}
			assertUDPReorderingCSV(t, sent, received)
		})
	}
}

// MSG_PEEK observes readiness without consuming a datagram that belongs to the
// production receiver. Even a zero-length probe is distinguishable from EAGAIN.
func waitUDPQueueDrained(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var peekErr error
		if err := raw.Control(func(fd uintptr) {
			var buf [1]byte
			_, _, peekErr = unix.Recvfrom(int(fd), buf[:], unix.MSG_PEEK|unix.MSG_DONTWAIT)
		}); err != nil {
			t.Fatal(err)
		}
		if errors.Is(peekErr, unix.EAGAIN) {
			return
		}
		if peekErr != nil {
			t.Fatal(peekErr)
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("UDPReceiver did not drain the socket queue")
}

func requireUDPReceiveTimestamping(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	if err := enableRxTimestamping(conn); err != nil {
		t.Skipf("kernel receive timestamping unavailable: %v", err)
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var flags int
	var optionErr error
	if err := raw.Control(func(fd uintptr) {
		flags, optionErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPING)
	}); err != nil {
		t.Fatal(err)
	}
	if optionErr != nil {
		t.Skipf("cannot query kernel receive timestamping: %v", optionErr)
	}
	const required = unix.SOF_TIMESTAMPING_RX_SOFTWARE | unix.SOF_TIMESTAMPING_SOFTWARE
	if flags&required != required {
		t.Skipf("kernel receive timestamping unavailable: flags %#x lack %#x", flags, required)
	}
}
