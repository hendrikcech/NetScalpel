package pkg

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// Unlike the sender integration tests, these tests never call selectMsgsSent:
// missing/unparseable kernel timestamps must fail, not use userspace fallback.
// SO_TIMESTAMPING software TX timestamps need no special privileges on Linux.
func TestTxTsReaderSocket(t *testing.T) {
	for _, network := range []struct {
		name string
		ip   string
	}{
		{"udp4", "127.0.0.1"},
		{"udp6", "::1"},
	} {
		t.Run(network.name, func(t *testing.T) {
			for _, mode := range []struct {
				name               string
				packets            int
				cancelledBeforeRun bool
			}{
				{"live", 32, false},
				// All notifications are already queued when Run starts with a
				// cancelled context. It must drain them rather than exit early.
				{"cancelled_with_queued_timestamps", 32, true},
				{"cancel_idle_reader", 0, false},
			} {
				t.Run(mode.name, func(t *testing.T) {
					peer, err := net.ListenUDP(network.name, &net.UDPAddr{IP: net.ParseIP(network.ip)})
					if err != nil {
						if network.name == "udp6" && os.Getenv("NETSCALPEL_REQUIRE_IPV6") != "1" &&
							(errors.Is(err, syscall.EAFNOSUPPORT) || errors.Is(err, syscall.EADDRNOTAVAIL)) {
							t.Skipf("IPv6 loopback unavailable (required in CI): %v", err)
						}
						t.Fatal(err)
					}
					defer peer.Close()
					sender, err := net.ListenUDP(network.name, &net.UDPAddr{IP: net.ParseIP(network.ip)})
					if err != nil {
						t.Fatal(err)
					}
					defer sender.Close()
					if err := enableTxTimestamping(sender); err != nil {
						// Do not skip option errors: an option regression must fail
						// just like a decoder regression or missing notification.
						t.Fatalf("enable TX timestamping: %v", err)
					}
					if err := sender.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
						t.Fatal(err)
					}

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					r := newTxTsReader(uint(mode.packets))
					finished := make(chan struct{})
					var runErr error
					start := func() {
						go func() {
							runErr = r.Run(ctx, sender)
							close(finished)
						}()
						t.Cleanup(func() {
							cancel()
							sender.Close() // also unblock Run if a test assertion fails
							select {
							case <-finished:
							case <-time.After(3 * time.Second):
								t.Error("TxTsReader.Run did not stop during cleanup")
							}
						})
					}
					if !mode.cancelledBeforeRun {
						start()
					}

					type bounds struct{ before, after time.Time }
					windows := make([]bounds, mode.packets)
					for seq := range mode.packets {
						payload := make([]byte, 64)
						binary.BigEndian.PutUint64(payload, uint64(seq))
						windows[seq].before = time.Now()
						if n, err := sender.WriteToUDP(payload, peer.LocalAddr().(*net.UDPAddr)); err != nil || n != len(payload) {
							t.Fatalf("send packet %d: n=%d err=%v", seq, n, err)
						}
						// Consume every datagram so socket-buffer loss cannot hide
						// the behavior under test. Receipt bounds the TX timestamp.
						if err := peer.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
							t.Fatal(err)
						}
						var received [64]byte
						n, _, err := peer.ReadFromUDP(received[:])
						windows[seq].after = time.Now()
						if err != nil || n != len(payload) || binary.BigEndian.Uint64(received[:]) != uint64(seq) {
							t.Fatalf("receive packet %d: n=%d err=%v data=%x", seq, n, err, received)
						}
					}

					if !mode.cancelledBeforeRun {
						select {
						case <-r.C:
							t.Fatal("reader returned before cancellation")
						default:
						}
					}
					cancel()
					if mode.cancelledBeforeRun {
						start()
					}

					var msgs []MsgSent
					select {
					case got, ok := <-r.C:
						if !ok {
							t.Fatal("reader closed results without delivering them")
						}
						msgs = got
					case <-time.After(3 * time.Second):
						t.Fatal("reader did not deliver results after cancellation")
					}
					select {
					case <-finished:
					case <-time.After(3 * time.Second):
						t.Fatal("Run did not return after delivering results")
					}
					if runErr != nil {
						t.Fatalf("Run failed: %v", runErr)
					}
					select {
					case _, ok := <-r.C:
						if ok {
							t.Fatal("reader delivered results more than once")
						}
					default:
						t.Fatal("results channel not closed after Run returned")
					}
					if len(msgs) != mode.packets {
						t.Fatalf("got %d kernel timestamps, want %d (no fallback allowed)", len(msgs), mode.packets)
					}
					for seq, msg := range msgs {
						if msg.Seq != uint64(seq) {
							t.Errorf("timestamp %d: packet ID %d, want %d", seq, msg.Seq, seq)
						}
						w := windows[seq]
						if msg.TsSent.IsZero() || msg.TsSent.Before(w.before) || msg.TsSent.After(w.after) {
							t.Errorf("packet %d: kernel timestamp %v outside send/receive window [%v, %v]", seq, msg.TsSent, w.before, w.after)
						}
						if msg.Len != 0 {
							t.Errorf("packet %d: TSONLY notification length %d, want 0", seq, msg.Len)
						}
					}
				})
			}
		})
	}
}
