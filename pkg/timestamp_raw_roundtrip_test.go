package pkg

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

func TestRawHardwareTimestampPreservedThroughResultAndCSV(t *testing.T) {
	RegisterGob()
	software := time.Unix(1700000000, 123456789)
	for _, raw := range []time.Time{time.Unix(10, 987654321), time.Unix(-1, 42)} {
		var times unix.ScmTimestamping
		times.Ts[0] = unix.NsecToTimespec(software.UnixNano())
		times.Ts[2] = unix.NsecToTimespec(raw.UnixNano())
		oob := timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, times)
		parsed, err := parseRxTimestampOOB(mmsg.Message{OOB: oob, NN: len(oob)}, TimestampHardware)
		if err != nil {
			t.Fatal(err)
		}
		report, err := selectTimestampSource(context.Background(), TimestampHardware, false)
		if err != nil {
			t.Fatal(err)
		}
		report.PacketCount = 1
		messages := []MsgRcvd{{Seq: 0, TsRcvd: parsed, Len: 8}}
		response := RequestServerResultReply{
			Complete: true,
			Result:   messages,
			TimestampReport: TimestampReport{
				LocalConfig: TimestampConfig{TX: TimestampSoftware, RX: TimestampHardware}, RX: report,
			},
		}
		var wire bytes.Buffer
		if err := gob.NewEncoder(&wire).Encode(response); err != nil {
			t.Fatal(err)
		}
		var decoded RequestServerResultReply
		if err := gob.NewDecoder(&wire).Decode(&decoded); err != nil {
			t.Fatal(err)
		}
		received := decoded.Result.([]MsgRcvd)
		rows := generateUDPResultRows(processUDP([]MsgSent{{Seq: 0, TsSent: software, Len: 8}}, received))
		persisted, err := time.Parse(time.RFC3339Nano, rows[1][2])
		if err != nil {
			t.Fatal(err)
		}
		if persisted.Unix() != raw.Unix() || persisted.Nanosecond() != raw.Nanosecond() {
			t.Fatalf("hardware timestamp changed: got %v, want raw %v", persisted, raw)
		}
	}
}

func TestTimestampParsersRejectPartialTrailingHeaders(t *testing.T) {
	tx := timestampMessage(false)
	var times unix.ScmTimestamping
	times.Ts[0] = unix.NsecToTimespec(time.Unix(1700000000, 1).UnixNano())
	times.Ts[2] = times.Ts[0]
	rx := timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, times)
	for trailing := 1; trailing < unix.CmsgLen(0); trailing++ {
		txOOB := append(append([]byte(nil), tx.OOB...), make([]byte, trailing)...)
		if _, err := NewTxTsReader().parseOOB(context.Background(), mmsg.Message{OOB: txOOB, NN: len(txOOB)}); err == nil {
			t.Fatalf("TX accepted %d trailing ancillary bytes", trailing)
		}
		for _, source := range []TimestampSource{TimestampSoftware, TimestampHardware} {
			rxOOB := append(append([]byte(nil), rx...), make([]byte, trailing)...)
			if _, err := parseRxTimestampOOB(mmsg.Message{OOB: rxOOB, NN: len(rxOOB)}, source); err == nil {
				t.Fatalf("%s RX accepted %d trailing ancillary bytes", source, trailing)
			}
		}
	}
}
