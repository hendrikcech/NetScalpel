package pkg

// Unit tests for the result-processing logic: processUDP computes the headline loss metric and the CSV writers
// feed the downstream analysis scripts, which parse by column position. The
// tests assert the *current intended* behavior.

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/mikioh/tcpinfo"
)

// --- processUDP edge cases ---

func TestProcessUDPRcvdEmpty(t *testing.T) {
	sent := []MsgSent{{Seq: 0}, {Seq: 1}, {Seq: 2}}
	exp := []MsgResult{
		{Seq: 0, Lost: true},
		{Seq: 1, Lost: true},
		{Seq: 2, Lost: true},
	}
	res := processUDP(sent, nil)
	checkProcessUDP(t, exp, res)
}

func TestProcessUDPSentEmpty(t *testing.T) {
	// No sent packets: empty result, and no panic even when rcvd is
	// non-empty (corrupt or foreign packets).
	rcvd := []MsgRcvd{{Seq: 0}, {Seq: 1}}
	res := processUDP(nil, rcvd)
	if len(res) != 0 {
		t.Errorf("expected empty result, got %v entries", len(res))
	}
}

func TestProcessUDPDupLastSeq(t *testing.T) {
	// The existing dup test only covers interior dups; duplicates of the
	// final seq must be ignored, not looped over or appended.
	sent := []MsgSent{{Seq: 0}, {Seq: 1}, {Seq: 2}, {Seq: 3}}
	rcvd := []MsgRcvd{
		{Seq: 0}, {Seq: 1}, {Seq: 2}, {Seq: 3}, {Seq: 3}, {Seq: 3},
	}
	exp := []MsgResult{
		{Seq: 0}, {Seq: 1}, {Seq: 2}, {Seq: 3},
	}
	res := processUDP(sent, rcvd)
	checkProcessUDP(t, exp, res)
}

func TestProcessUDPForeignSeq(t *testing.T) {
	// A received seq >= len(sent) (corrupt/foreign packet) must not panic.
	// Current behavior: the foreign seq is never matched, so every sent seq
	// below it that was not received counts as lost.
	sent := []MsgSent{{Seq: 0}, {Seq: 1}, {Seq: 2}}
	rcvd := []MsgRcvd{{Seq: 0}, {Seq: 1}, {Seq: 5}}
	exp := []MsgResult{
		{Seq: 0}, {Seq: 1}, {Seq: 2, Lost: true},
	}
	res := processUDP(sent, rcvd)
	checkProcessUDP(t, exp, res)
}

func TestProcessUDPOnlyForeignSeq(t *testing.T) {
	sent := []MsgSent{{Seq: 0}, {Seq: 1}}
	rcvd := []MsgRcvd{{Seq: 7}}
	exp := []MsgResult{
		{Seq: 0, Lost: true}, {Seq: 1, Lost: true},
	}
	res := processUDP(sent, rcvd)
	checkProcessUDP(t, exp, res)
}

func TestProcessUDPOutOfOrderRcvd(t *testing.T) {
	// The sort at the top of processUDP must handle reordered input.
	sent := []MsgSent{{Seq: 0}, {Seq: 1}, {Seq: 2}, {Seq: 3}}
	rcvd := []MsgRcvd{{Seq: 3}, {Seq: 0}, {Seq: 2}, {Seq: 1}}
	exp := []MsgResult{
		{Seq: 0}, {Seq: 1}, {Seq: 2}, {Seq: 3},
	}
	res := processUDP(sent, rcvd)
	checkProcessUDP(t, exp, res)
}

func TestProcessUDPLossHeadAndTail(t *testing.T) {
	sent := []MsgSent{{Seq: 0}, {Seq: 1}, {Seq: 2}, {Seq: 3}, {Seq: 4}}
	rcvd := []MsgRcvd{{Seq: 1}, {Seq: 2}, {Seq: 3}}
	exp := []MsgResult{
		{Seq: 0, Lost: true},
		{Seq: 1}, {Seq: 2}, {Seq: 3},
		{Seq: 4, Lost: true},
	}
	res := processUDP(sent, rcvd)
	checkProcessUDP(t, exp, res)
}

func TestProcessUDPOwd(t *testing.T) {
	tsSent := time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)
	tsRcvd := tsSent.Add(12345 * time.Microsecond)
	sent := []MsgSent{{Seq: 0, TsSent: tsSent, Len: 1400}}
	rcvd := []MsgRcvd{{Seq: 0, TsRcvd: tsRcvd, Len: 1400}}

	res := processUDP(sent, rcvd)
	if len(res) != 1 {
		t.Fatalf("expected 1 result, got %v", len(res))
	}
	r := res[0]
	if r.Lost {
		t.Errorf("packet marked lost")
	}
	if !r.TsSent.Equal(tsSent) || !r.TsRcvd.Equal(tsRcvd) {
		t.Errorf("timestamps not carried through: %v / %v", r.TsSent, r.TsRcvd)
	}
	if want := tsRcvd.Sub(tsSent); r.Owd != want {
		t.Errorf("Owd = %v, want %v", r.Owd, want)
	}
	if r.Len != 1400 {
		t.Errorf("Len = %v, want 1400", r.Len)
	}
}

// --- CSV generation ---

func TestGenerateUDPResultRowsGolden(t *testing.T) {
	tsSent := time.Date(2026, 1, 2, 15, 4, 5, 123456789, time.UTC)
	tsRcvd := tsSent.Add(12345678 * time.Nanosecond)

	results := []MsgResult{
		// Lost: empty ts_rcvd and owd_ms
		{Seq: 0, TsSent: tsSent, Len: 1400, Lost: true},
		// Received
		{Seq: 1, TsSent: tsSent, TsRcvd: tsRcvd, Owd: tsRcvd.Sub(tsSent), Len: 1400},
		// Zero-time formatting (e.g. a msg without a kernel timestamp)
		{Seq: 2, TsSent: time.Time{}, TsRcvd: tsRcvd, Owd: 0, Len: 8},
	}

	exp := [][]string{
		{"seq", "ts_sent", "ts_rcvd", "owd_ms", "size", "lost"},
		{"0", "2026-01-02T15:04:05.123456789Z", "", "", "1400", "true"},
		{"1", "2026-01-02T15:04:05.123456789Z", "2026-01-02T15:04:05.135802467Z", "12.345678", "1400", "false"},
		{"2", "0001-01-01T00:00:00Z", "2026-01-02T15:04:05.135802467Z", "0", "8", "false"},
	}

	rows := generateUDPResultRows(results)
	if !reflect.DeepEqual(exp, rows) {
		t.Errorf("UDP result rows changed:\ngot  %v\nwant %v", rows, exp)
	}
}

// The exact header row the analysis scripts rely on (column positions are
// the contract; reordering is a silent corruption downstream).
var expTCPHeader = []string{
	"Time",
	"State", "SenderMSS", "ReceiverMSS", "RTT", "RTTVar", "RTO", "ATO",
	"LastDataSent", "LastDataReceived", "LastAckReceived",
	"ReceiverWindow",
	"SenderSSThreshold", "ReceiverSSThreshold", "SenderWindowBytes", "SenderWindowSegs",
	"PathMTU", "CAState", "Retransmissions", "Backoffs", "WindowOrKeepAliveProbes",
	"UnackedSegs", "SackedSegs", "LostSegs", "RetransSegs", "ForwardAckSegs",
	"ReorderedSegs", "ReceiverRTT", "TotalRetransSegs", "PacingRate",
	"ThruBytesAcked", "ThruBytesReceived", "SegsOut", "SegsIn", "NotSentBytes",
	"MinRTT", "DataSegsOut", "DataSegsIn",
	"BBRMaxBW", "BBRMinRTT", "BBRPacingGain", "BBRCongWindowGain",
}

func TestGenerateTCPResultRows(t *testing.T) {
	info := &tcpinfo.Info{
		FlowControl:       &tcpinfo.FlowControl{ReceiverWindow: 65535},
		CongestionControl: &tcpinfo.CongestionControl{SenderWindowSegs: 10},
		Sys:               &tcpinfo.SysInfo{PathMTU: 1500},
	}
	ts := time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)

	metrics := []TCPMetric{
		{Time: ts, Info: info, BBRInfo: nil},
		{Time: ts.Add(5 * time.Millisecond), Info: info, BBRInfo: &tcpinfo.BBRInfo{
			MaxBW:          123456,
			MinRTT:         20 * time.Millisecond,
			PacingGain:     256,
			CongWindowGain: 512,
		}},
	}

	rows := generateTCPResultRows(metrics)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %v", len(rows))
	}

	if !reflect.DeepEqual(expTCPHeader, rows[0]) {
		t.Errorf("TCP header row changed:\ngot  %v\nwant %v", rows[0], expTCPHeader)
	}

	// Header length == data row length is the property that the runtime
	// panic in generateTCPResultRows enforces; pin it here.
	for i := 1; i < len(rows); i++ {
		if len(rows[i]) != len(rows[0]) {
			t.Errorf("row %v has %v columns, header has %v", i, len(rows[i]), len(rows[0]))
		}
	}

	// A nil BBRInfo yields empty BBR cells; a set one yields its values.
	n := len(rows[0])
	if got := rows[1][n-4:]; !reflect.DeepEqual(got, []string{"", "", "", ""}) {
		t.Errorf("BBR cells for nil BBRInfo: %v", got)
	}
	if got, want := rows[2][n-4:], []string{"123456", "20", "256", "512"}; !reflect.DeepEqual(got, want) {
		t.Errorf("BBR cells = %v, want %v", got, want)
	}
	if got, want := rows[1][0], "2026-01-02T15:04:05Z"; got != want {
		t.Errorf("Time cell = %v, want %v", got, want)
	}
}

func TestFmtDurationMs(t *testing.T) {
	cases := []struct {
		d    time.Duration
		want string
	}{
		{123456 * time.Nanosecond, "0.123456"}, // sub-ms
		{0, "0"},
		{-1500 * time.Microsecond, "-1.5"}, // negative
		{12345678 * time.Nanosecond, "12.345678"},
		{time.Second, "1000"},
	}
	for _, c := range cases {
		if got := fmtDurationMs(c.d); got != c.want {
			t.Errorf("fmtDurationMs(%v) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestWriteCSVFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.csv")
	rows := [][]string{
		{"seq", "lost"},
		{"0", "false"},
		{"1", "true"},
	}
	if err := writeCSV(path, rows); err != nil {
		t.Fatalf("writeCSV failed: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := "seq,lost\n0,false\n1,true\n"
	if string(got) != want {
		t.Errorf("CSV content = %q, want %q", got, want)
	}
}
