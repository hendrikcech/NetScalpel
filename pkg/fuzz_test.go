package pkg

// Fuzz/property tests for the pure codec and result-processing functions.
// `go test` runs only the seed corpus; explore with e.g.
// `go test -fuzz=FuzzProcessUDP -fuzztime=30s ./pkg/`.

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
	"time"
)

// FuzzProcessUDP compares the two-pointer algorithm against a trivially
// correct map-based oracle. rcvdSeqs is interpreted as one received packet
// per byte, so duplication, reordering, and foreign seqs (>= numSent) all
// occur naturally. Timestamps are a function of the seq, so duplicates of a
// seq are identical and it does not matter which one processUDP picks.
func FuzzProcessUDP(f *testing.F) {
	f.Add(uint8(6), []byte{0, 1, 2, 3, 4, 5})          // all received
	f.Add(uint8(6), []byte{0, 1, 3, 4, 5})             // loss
	f.Add(uint8(6), []byte{0, 1, 1, 2, 3, 3, 3, 4, 5}) // duplicates
	f.Add(uint8(4), []byte{3, 3, 3})                   // duplicated last seq
	f.Add(uint8(4), []byte{3, 0, 2, 1})                // reordered
	f.Add(uint8(3), []byte{0, 1, 5})                   // foreign seq
	f.Add(uint8(0), []byte{7})                         // nothing sent

	f.Fuzz(func(t *testing.T, numSent uint8, rcvdSeqs []byte) {
		base := time.Unix(1700000000, 0)
		tsSent := func(seq uint64) time.Time {
			return base.Add(time.Duration(seq) * time.Millisecond)
		}
		tsRcvd := func(seq uint64) time.Time {
			return tsSent(seq).Add(20 * time.Millisecond)
		}

		sent := make([]MsgSent, numSent)
		for i := range sent {
			seq := uint64(i)
			sent[i] = MsgSent{Seq: seq, TsSent: tsSent(seq), Len: 8 + uint(seq%100)}
		}
		rcvd := make([]MsgRcvd, len(rcvdSeqs))
		for i, b := range rcvdSeqs {
			seq := uint64(b)
			rcvd[i] = MsgRcvd{Seq: seq, TsRcvd: tsRcvd(seq), Len: 28}
		}

		res := processUDP(sent, rcvd)

		rcvdBySeq := make(map[uint64]MsgRcvd, len(rcvd))
		for _, r := range rcvd {
			rcvdBySeq[r.Seq] = r
		}
		exp := make([]MsgResult, len(sent))
		for i, s := range sent {
			if r, ok := rcvdBySeq[s.Seq]; ok {
				exp[i] = MsgResult{
					Seq:    s.Seq,
					TsSent: s.TsSent,
					TsRcvd: r.TsRcvd,
					Owd:    r.TsRcvd.Sub(s.TsSent),
					Len:    s.Len,
					Lost:   false,
				}
			} else {
				exp[i] = MsgResult{Seq: s.Seq, TsSent: s.TsSent, Len: s.Len, Lost: true}
			}
		}

		if len(res) != len(exp) {
			t.Fatalf("got %d results for %d sent packets", len(res), len(exp))
		}
		for i := range exp {
			if res[i].Seq != uint64(i) {
				t.Fatalf("result %d has seq %d; every seq 0..n-1 must appear exactly once in order", i, res[i].Seq)
			}
			if !reflect.DeepEqual(res[i], exp[i]) {
				t.Fatalf("result for seq %d = %+v; oracle says %+v", i, res[i], exp[i])
			}
		}
	})
}

func FuzzMsgEncodeDecode(f *testing.F) {
	f.Add(uint64(0), uint16(0), uint16(1500))
	f.Add(uint64(42), uint16(200), uint16(1500))
	f.Add(uint64(1)<<63, uint16(1500), uint16(8)) // too small for the padding
	f.Add(uint64(7), uint16(0), uint16(3))        // too small for the seq

	f.Fuzz(func(t *testing.T, seq uint64, padN uint16, bufLen uint16) {
		m := Msg{Seq: seq, PadN: uint(padN)}
		buf := make([]byte, bufLen)
		n, err := m.Encode(buf)
		if int(bufLen) < 8+int(padN) {
			if err == nil {
				t.Fatalf("Encode into %d-byte buffer with %d padding bytes returned no error", bufLen, padN)
			}
			return
		}
		if err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
		if n != 8+int(padN) {
			t.Fatalf("Encode wrote %d bytes; want %d", n, 8+int(padN))
		}
		var d Msg
		d.Decode(buf[:n])
		if d.Seq != seq {
			t.Fatalf("Seq %d round-tripped to %d", seq, d.Seq)
		}
	})
}

func FuzzICMPDataRoundTrip(f *testing.F) {
	f.Add(uint16(0), false)
	f.Add(uint16(4242), true)

	f.Fuzz(func(t *testing.T, echoID uint16, punch bool) {
		data := makeICMPData(echoID, punch)
		gotID, gotPunch, err := parseICMPData(data)
		if err != nil {
			t.Fatalf("parseICMPData(makeICMPData(%d, %v)) failed: %v", echoID, punch, err)
		}
		if gotID != echoID || gotPunch != punch {
			t.Fatalf("round trip of (%d, %v) returned (%d, %v)", echoID, punch, gotID, gotPunch)
		}
	})
}

func FuzzParseICMPData(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add(makeICMPData(99, true))
	f.Add(makeICMPData(99, false))

	f.Fuzz(func(t *testing.T, data []byte) {
		echoID, punch, err := parseICMPData(data)
		if len(data) < 2 {
			if err == nil {
				t.Fatalf("no error for %d-byte data", len(data))
			}
			return
		}
		if err != nil {
			t.Fatalf("unexpected error for %d-byte data: %v", len(data), err)
		}
		if want := binary.LittleEndian.Uint16(data[:2]); echoID != want {
			t.Fatalf("echoID = %d; want %d", echoID, want)
		}
		if punch && !bytes.Equal(data[2:], ICMPProbePayload) {
			t.Fatalf("punch reported for data %v that does not carry the probe payload", data)
		}
	})
}
