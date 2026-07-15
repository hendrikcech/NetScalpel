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
