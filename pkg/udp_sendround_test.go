package pkg

import (
	"context"
	"net"
	"testing"

	mmsg "github.com/anacrolix/mmsg/socket"
)

type rateSendMsgsStub struct {
	t      *testing.T
	counts []int
	seqs   []uint64
	lens   []int
}

func (s *rateSendMsgsStub) SendMsgs(msgs []mmsg.Message, _ int) (int, error) {
	n := len(msgs)
	if len(s.counts) > 0 {
		n, s.counts = s.counts[0], s.counts[1:]
	}
	if n < 0 || n > len(msgs) {
		s.t.Fatalf("invalid stub count %d for batch of %d", n, len(msgs))
	}
	for _, packet := range msgs[:n] {
		var msg Msg
		msg.Decode(packet.Buffers[0])
		s.seqs = append(s.seqs, msg.Seq)
		s.lens = append(s.lens, len(packet.Buffers[0]))
	}
	return n, nil
}

func TestRateSenderPayloadSizeChanges(t *testing.T) {
	r := RateSender{tx: make([]mmsg.Message, 3)}
	for i := range r.tx {
		r.tx[i].Buffers = [][]byte{make([]byte, 1500)}
	}
	conn := &rateSendMsgsStub{t: t}
	addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
	// Reuse every buffer across growing and shrinking payloads, including
	// an empty payload and one that fills the entire backing array.
	for _, payload := range []uint{0, 16, 1492, 64, 1492} {
		first := len(conn.seqs)
		sent, err := r.sendRound(context.Background(), conn, addr, 3, payload)
		if err != nil || sent != 3 {
			t.Fatalf("sendRound(payload=%d) = (%d, %v), want (3, nil)", payload, sent, err)
		}
		for i := first; i < len(conn.seqs); i++ {
			if conn.seqs[i] != uint64(i) || r.msgs[i].Seq != uint64(i) {
				t.Errorf("packet %d: wire sequence %d, recorded sequence %d", i, conn.seqs[i], r.msgs[i].Seq)
			}
			if conn.lens[i] != int(8+payload) || r.msgs[i].Len != 8+payload {
				t.Errorf("packet %d: wire length %d, recorded length %d, want %d", i, conn.lens[i], r.msgs[i].Len, 8+payload)
			}
		}
	}
	// Capacity is still a hard limit; an oversized payload must not be sent.
	before := len(conn.seqs)
	sent, err := r.sendRound(context.Background(), conn, addr, 3, 1493)
	if err == nil || sent != 0 {
		t.Fatalf("oversized sendRound = (%d, %v), want (0, error)", sent, err)
	}
	if len(conn.seqs) != before || len(r.msgs) != before || r.seq != uint64(before) {
		t.Fatal("oversized payload changed sent messages or sequence")
	}
}

func TestRateSenderPartialWriteSequence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		initial uint64
		packets uint
		counts  []int
		want    uint
	}{
		{name: "partial_from_zero", packets: 5, counts: []int{2}, want: 2},
		{name: "partial_after_previous_packets", initial: 100, packets: 5, counts: []int{3}, want: 3},
		{name: "zero_written", packets: 5, counts: []int{0}},
		{name: "partial_second_batch", packets: 1030, counts: []int{1024, 2}, want: 1026},
		{name: "full_write", packets: 5, want: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := RateSender{seq: tc.initial, tx: make([]mmsg.Message, 1024)}
			for i := range r.tx {
				r.tx[i].Buffers = [][]byte{make([]byte, 1500)}
			}
			conn := &rateSendMsgsStub{t: t, counts: append([]int(nil), tc.counts...)}
			addr := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
			sent, err := r.sendRound(context.Background(), conn, addr, tc.packets, 16)
			if err != nil || sent != tc.want {
				t.Fatalf("sendRound = (%d, %v), want (%d, nil)", sent, err, tc.want)
			}
			if r.seq != tc.initial+uint64(tc.want) {
				t.Errorf("next sequence = %d, want %d", r.seq, tc.initial+uint64(tc.want))
			}
			if len(r.msgs) != int(tc.want) {
				t.Fatalf("recorded %d messages, want %d", len(r.msgs), tc.want)
			}

			// A later tick must resume at the first unsent sequence, without
			// reusing an accepted sequence or wrapping below zero.
			sent, err = r.sendRound(context.Background(), conn, addr, 3, 16)
			if err != nil || sent != 3 {
				t.Fatalf("follow-up sendRound = (%d, %v), want (3, nil)", sent, err)
			}
			if r.seq != tc.initial+uint64(tc.want)+3 {
				t.Errorf("follow-up sequence = %d, want %d", r.seq, tc.initial+uint64(tc.want)+3)
			}
			if len(r.msgs) != int(tc.want)+3 || len(conn.seqs) != len(r.msgs) {
				t.Fatalf("recorded %d messages, accepted %d, want %d", len(r.msgs), len(conn.seqs), tc.want+3)
			}
			for i, seq := range conn.seqs {
				want := tc.initial + uint64(i)
				if seq != want || r.msgs[i].Seq != want {
					t.Errorf("packet %d: wire sequence %d, recorded sequence %d, want %d", i, seq, r.msgs[i].Seq, want)
				}
			}
		})
	}
}
