package pkg

// Completeness check for RegisterGob: every concrete type that travels
// behind an interface field of an RPC arg or reply struct must be
// registered, or the failure only shows up at runtime on the wire. The
// round trips here use the same encoding net/rpc uses.

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"
	"time"

	"github.com/mikioh/tcpinfo"
)

func gobRoundTrip(t *testing.T, in any, out any) {
	t.Helper()
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(in); err != nil {
		t.Fatalf("gob encode of %T failed (missing RegisterGob entry?): %v", in, err)
	}
	if err := gob.NewDecoder(&buf).Decode(out); err != nil {
		t.Fatalf("gob decode into %T failed (missing RegisterGob entry?): %v", out, err)
	}
}

// One Sender per send mode. Each entry also names its paired receive mode
// via ReceiverMode, and the completeness check over NetModes below forces a
// new entry (and thereby a params round trip) when a mode is added.
func TestGobRegistrationSenderParams(t *testing.T) {
	RegisterGob()

	senders := []Sender{
		&BurstSender{Params: BurstParams{Timeout: time.Second, Num: 3, Pad: 5}},
		&RateSender{Params: RateParamsW{{Pps: 100, Duration: time.Second, PayloadSize: 200}}},
		&PeriodicSender{Params: PeriodicParams{Interval: time.Millisecond, Duration: time.Second, Pad: 10}},
		&QUICSender{Params: QUICParams{Duration_: time.Second, Bytes: 1000}},
		&TCPSender{Params: TCPSenderParams{Duration_: time.Second, Bytes: 1000, CCA: TCPCCA(1)}},
		// Nonzero ClientEchoID so GetParams does not generate one
		&ICMPSender{Params: ICMPParams{Duration_: time.Second, Interval: time.Millisecond, ClientEchoID: 42, SenderEchoID: 42}},
	}

	covered := make(map[Mode]bool)
	for _, s := range senders {
		covered[s.SenderMode()] = true
		covered[s.ReceiverMode()] = true

		args := RequestServerArgs{ID: "id", ServerMode: s.SenderMode(), Params: s.GetParams()}
		var got RequestServerArgs
		gobRoundTrip(t, args, &got)
		if !reflect.DeepEqual(args.Params, got.Params) {
			t.Errorf("%T did not survive the gob round trip: sent %+v, got %+v", args.Params, args.Params, got.Params)
		}
	}

	for _, m := range NetModes {
		if !covered[m] {
			t.Errorf("no Sender in this test covers mode %v; add one so its params type gets a gob round trip", m)
		}
	}
}

func TestGobRegistrationResults(t *testing.T) {
	RegisterGob()

	// time.Unix carries no monotonic reading, which gob would drop and
	// DeepEqual would notice.
	ts := time.Unix(1700000000, 0)

	// Every value handleSender/handleReceiver can put into Result.Res. The
	// TCP metrics carry tcpinfo option values behind the tcpinfo.Option
	// interface, which is why RegisterGob lists them.
	results := []any{
		[]MsgSent{{Seq: 1, TsSent: ts, Len: 108}},
		[]MsgRcvd{{Seq: 1, TsRcvd: ts, Len: 108}},
		[]TCPMetric{{
			Time: ts,
			Info: &tcpinfo.Info{
				Options: []tcpinfo.Option{
					tcpinfo.WindowScale(7),
					tcpinfo.SACKPermitted(true),
					tcpinfo.Timestamps(true),
				},
				PeerOptions: []tcpinfo.Option{tcpinfo.WindowScale(8)},
			},
			BBRInfo: &tcpinfo.BBRInfo{MaxBW: 1},
		}},
	}

	for _, res := range results {
		reply := RequestServerResultReply{Result: res}
		var got RequestServerResultReply
		gobRoundTrip(t, reply, &got)
		if !reflect.DeepEqual(reply.Result, got.Result) {
			t.Errorf("%T did not survive the gob round trip: sent %+v, got %+v", res, reply.Result, got.Result)
		}
	}
}

func TestGobRegistrationCommandParams(t *testing.T) {
	RegisterGob()

	// Keyed by RunCommandMode as the checklist: a new command mode needs an
	// entry here.
	params := map[RunCommandMode]CommandParams{
		Tcpdump: TcpdumpParams{Name_: "tcpdump", Timeout_: time.Second, Filter: "udp"},
	}

	for mode, p := range params {
		args := RunCommandArgs{ID: "id", Params: p}
		var got RunCommandArgs
		gobRoundTrip(t, args, &got)
		if !reflect.DeepEqual(args.Params, got.Params) {
			t.Errorf("mode %v: %T did not survive the gob round trip: sent %+v, got %+v", mode, p, args.Params, got.Params)
		}
	}
}
