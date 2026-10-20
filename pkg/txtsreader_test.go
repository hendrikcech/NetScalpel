package pkg

import (
	"context"
	"testing"
	"time"
)

// makeReader returns a TxTsReader whose Run already delivered tsMsgs.
func makeReader(tsMsgs []MsgSent) *TxTsReader {
	r := NewTxTsReader()
	r.C <- tsMsgs
	close(r.C)
	return r
}

func TestTxTsReaderCapacity(t *testing.T) {
	for _, count := range []uint{0, 1, 1024, 100_000} {
		r := newTxTsReader(count)
		if len(r.sentMsgs) != 0 || cap(r.sentMsgs) != int(count) {
			t.Fatalf("count %d: len=%d cap=%d", count, len(r.sentMsgs), cap(r.sentMsgs))
		}
		allocs := testing.AllocsPerRun(1, func() {
			r.sentMsgs = r.sentMsgs[:0]
			for seq := uint(0); seq < count; seq++ {
				r.sentMsgs = append(r.sentMsgs, MsgSent{Seq: uint64(seq)})
			}
		})
		if allocs != 0 {
			t.Fatalf("count %d: appending timestamps allocated %v times", count, allocs)
		}
		// Estimates may be low; no timestamp should be dropped at capacity.
		r.sentMsgs = append(r.sentMsgs, MsgSent{Seq: uint64(count)})
		if len(r.sentMsgs) != int(count)+1 {
			t.Fatal("capacity hint limited stored timestamps")
		}
	}
}

func TestSelectMsgsSent(t *testing.T) {
	ctx := context.Background()
	noop := func() {}
	ts := time.Now()

	t.Run("uniform sizes use the reader's timestamps", func(t *testing.T) {
		sent := []MsgSent{{Seq: 0, Len: 100}, {Seq: 1, Len: 100}}
		tsMsgs := []MsgSent{{Seq: 0, TsSent: ts}, {Seq: 1, TsSent: ts}}
		got := selectMsgsSent(ctx, makeReader(tsMsgs), noop, sent)
		if len(got) != 2 || !got[0].TsSent.Equal(ts) {
			t.Fatalf("expected reader timestamps, got %+v", got)
		}
		for i := range got {
			if got[i].Len != 100 {
				t.Errorf("msg %v: Len = %v, want 100", i, got[i].Len)
			}
		}
	})

	t.Run("non-uniform sizes fall back to the run function's records", func(t *testing.T) {
		sent := []MsgSent{{Seq: 0, Len: 100}, {Seq: 1, Len: 200}}
		tsMsgs := []MsgSent{{Seq: 0, TsSent: ts}, {Seq: 1, TsSent: ts}}
		got := selectMsgsSent(ctx, makeReader(tsMsgs), noop, sent)
		if len(got) != 2 || got[0].Len != 100 || got[1].Len != 200 {
			t.Fatalf("expected sentMsgs to be returned unchanged, got %+v", got)
		}
	})

	t.Run("count mismatch falls back to the run function's records", func(t *testing.T) {
		sent := []MsgSent{{Seq: 0, Len: 100}, {Seq: 1, Len: 100}}
		tsMsgs := []MsgSent{{Seq: 0, TsSent: ts}}
		got := selectMsgsSent(ctx, makeReader(tsMsgs), noop, sent)
		if len(got) != 2 || !got[0].TsSent.IsZero() {
			t.Fatalf("expected sentMsgs to be returned unchanged, got %+v", got)
		}
	})

	t.Run("nil reader returns the run function's records", func(t *testing.T) {
		sent := []MsgSent{{Seq: 0, Len: 100}}
		got := selectMsgsSent(ctx, nil, noop, sent)
		if len(got) != 1 || got[0].Len != 100 {
			t.Fatalf("expected sentMsgs to be returned unchanged, got %+v", got)
		}
	})
}
