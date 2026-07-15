package pkg

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// icmpQueueConn feeds pre-marshaled ICMP messages to ICMPReceiver.receive and
// reports os.ErrDeadlineExceeded once drained, which is receive's normal stop
// condition.
type icmpQueueConn struct {
	msgs [][]byte
}

func (c *icmpQueueConn) ReadFrom(b []byte) (int, net.Addr, error) {
	if len(c.msgs) == 0 {
		return 0, nil, os.ErrDeadlineExceeded
	}
	n := copy(b, c.msgs[0])
	c.msgs = c.msgs[1:]
	return n, &net.IPAddr{IP: net.IPv4(127, 0, 0, 1)}, nil
}

func (c *icmpQueueConn) Read(b []byte) (int, error)                   { panic("receive reads via ReadFrom") }
func (c *icmpQueueConn) Write(b []byte) (int, error)                  { panic("receive does not write") }
func (c *icmpQueueConn) WriteTo(b []byte, addr net.Addr) (int, error) { panic("receive does not write") }
func (c *icmpQueueConn) Close() error                                 { return nil }
func (c *icmpQueueConn) LocalAddr() net.Addr                          { return nil }
func (c *icmpQueueConn) RemoteAddr() net.Addr                         { return nil }
func (c *icmpQueueConn) SetDeadline(t time.Time) error                { return nil }
func (c *icmpQueueConn) SetReadDeadline(t time.Time) error            { return nil }
func (c *icmpQueueConn) SetWriteDeadline(t time.Time) error           { return nil }

func marshalEchoReply(t *testing.T, echoID uint16, seq int, punch bool) []byte {
	t.Helper()
	m := icmp.Message{
		Type: ipv4.ICMPTypeEchoReply,
		Code: 0,
		Body: &icmp.Echo{
			ID:   int(echoID),
			Seq:  seq,
			Data: makeICMPData(echoID, punch),
		},
	}
	buf, err := m.Marshal(nil)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	return buf
}

// The ICMP Echo seq field is 16 bits; receive extends it to a uint64 via
// baseSeq/receivedSinceWrap. This drives the wrap with the documented subtle
// cases: out-of-order packets just before the wrap point and a late
// duplicate just after it (which must not trigger a second wrap).
func TestICMPReceiveSeqWrap(t *testing.T) {
	const echoID = 7
	conn := &icmpQueueConn{}

	// First epoch, with the two final seqs swapped: an out-of-order packet
	// straddling the wrap point must not disturb the wrap detection.
	for seq := 0; seq <= 65533; seq++ {
		conn.msgs = append(conn.msgs, marshalEchoReply(t, echoID, seq, false))
	}
	conn.msgs = append(conn.msgs, marshalEchoReply(t, echoID, 65535, false))
	conn.msgs = append(conn.msgs, marshalEchoReply(t, echoID, 65534, false))

	// Wrap into the second epoch, then a late duplicate of seq 0: with more
	// than half the seq space received since the wrap counter was reset, a
	// small seq triggers the wrap once; the reset must prevent the duplicate
	// from triggering it a second time.
	for _, seq := range []int{0, 1, 0, 2} {
		conn.msgs = append(conn.msgs, marshalEchoReply(t, echoID, seq, false))
	}

	// Filtered packets: a hole punch and a foreign echo ID. Both must be
	// dropped and must not advance the wrap bookkeeping.
	conn.msgs = append(conn.msgs, marshalEchoReply(t, echoID, 3, true))
	conn.msgs = append(conn.msgs, marshalEchoReply(t, echoID+1, 4, false))

	r := &ICMPReceiver{ClientEchoID: echoID, ICMPType: ipv4.ICMPTypeEchoReply}
	got, err := r.receive(context.Background(), conn)
	if err != nil {
		t.Fatalf("receive failed: %v", err)
	}

	var want []uint64
	for seq := 0; seq <= 65533; seq++ {
		want = append(want, uint64(seq))
	}
	want = append(want, 65535, 65534)
	want = append(want, 65536, 65537, 65536, 65538)

	if len(got) != len(want) {
		t.Fatalf("received %d messages; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Seq != want[i] {
			t.Fatalf("message %d has seq %d; want %d", i, got[i].Seq, want[i])
		}
	}
}
