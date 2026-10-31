package pkg

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

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

func TestMatchTxTimestampsRequiresCompleteUniqueIDs(t *testing.T) {
	ts := time.Unix(1700000000, 1)
	tests := []struct {
		name    string
		sent    []MsgSent
		times   []MsgSent
		wantErr bool
	}{
		{
			name: "ID zero and reordered complete IDs",
			sent: []MsgSent{{Seq: 0, Len: 100}, {Seq: 1, Len: 200}},
			times: []MsgSent{{Seq: 1, TsSent: ts.Add(time.Second)},
				{Seq: 0, TsSent: ts}},
		},
		{
			name:    "missing ID",
			sent:    []MsgSent{{Seq: 0}, {Seq: 1}},
			times:   []MsgSent{{Seq: 0, TsSent: ts}, {Seq: 2, TsSent: ts}},
			wantErr: true,
		},
		{
			name:    "duplicate timestamp ID",
			sent:    []MsgSent{{Seq: 0}, {Seq: 1}},
			times:   []MsgSent{{Seq: 0, TsSent: ts}, {Seq: 0, TsSent: ts}},
			wantErr: true,
		},
		{
			name:    "duplicate sent ID",
			sent:    []MsgSent{{Seq: 0}, {Seq: 0}},
			times:   []MsgSent{{Seq: 0, TsSent: ts}, {Seq: 0, TsSent: ts}},
			wantErr: true,
		},
		{
			name:    "zero timestamp",
			sent:    []MsgSent{{Seq: 0}},
			times:   []MsgSent{{Seq: 0}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := matchTxTimestamps(tt.sent, tt.times)
			if (err != nil) != tt.wantErr {
				t.Fatalf("matchTxTimestamps error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				if len(got) != len(tt.sent) || got[0].Seq != 0 || got[0].Len != tt.sent[0].Len || !got[0].TsSent.Equal(ts) {
					t.Fatalf("unexpected matched records: %+v", got)
				}
			}
		})
	}
}

func TestBeginTxTimestampCollectionReturnsSetupError(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}

	reader, cancel, selection, err := beginTxTimestampCollection(context.Background(), conn, 1, TimestampHardware)
	cancel()
	if err == nil || reader != nil || selection.Requested != TimestampHardware || selection.Source != TimestampSoftware {
		t.Fatalf("setup = (%v, %v, %+v, %v), want direct error and software downgrade", reader, cancel, selection, err)
	}
}

func TestSelectTxTimestampsReportsMissingAndInvalidEvidence(t *testing.T) {
	selection, err := selectTimestampSource(context.Background(), TimestampSoftware, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		invalid uint64
		wantErr bool
	}{
		{name: "missing", wantErr: true},
		{name: "invalid notification", invalid: 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := &TxTsReader{C: make(chan []MsgSent, 1), invalid: tc.invalid}
			reader.C <- nil
			_, report, err := selectTxTimestamps(context.Background(), reader, func() {}, []MsgSent{{Seq: 4}}, selection)
			if (err != nil) != tc.wantErr || report.PacketCount != 1 || report.MissingCount != 1 || report.InvalidCount != tc.invalid {
				t.Fatalf("report=%+v err=%v", report, err)
			}
		})
	}
}

func TestAppTxSelectionDoesNotStartKernelReader(t *testing.T) {
	reader, cancel, selection, err := beginTxTimestampCollection(context.Background(), nil, 1, TimestampApp)
	defer cancel()
	if err != nil || reader != nil || selection.Source != TimestampApp {
		t.Fatalf("app selection = (%v, %+v, %v)", reader, selection, err)
	}
	selected, report, err := selectTxTimestamps(context.Background(), reader, cancel,
		[]MsgSent{{Seq: 1, TsSent: time.Unix(1700000000, 1)}}, selection)
	if err != nil || len(selected) != 1 || report.PacketCount != 1 || report.MissingCount != 0 {
		t.Fatalf("app result=%+v report=%+v err=%v", selected, report, err)
	}
	_, report, err = selectTxTimestamps(context.Background(), reader, cancel, []MsgSent{{Seq: 1}}, selection)
	if err == nil || report.MissingCount != 1 {
		t.Fatalf("missing app stamp report=%+v err=%v", report, err)
	}
}

func TestSelectTxTimestampsPreservesReaderRunError(t *testing.T) {
	selection, err := selectTimestampSource(context.Background(), TimestampSoftware, true)
	if err != nil {
		t.Fatal(err)
	}
	runErr := errors.New("reader failed")
	reader := &TxTsReader{C: make(chan []MsgSent, 1), runErr: runErr}
	reader.C <- nil
	_, report, err := selectTxTimestamps(context.Background(), reader, func() {}, nil, selection)
	if !errors.Is(err, runErr) || report.PacketCount != 0 || report.InvalidCount != 0 {
		t.Fatalf("reader failure report=%+v err=%v", report, err)
	}
}
