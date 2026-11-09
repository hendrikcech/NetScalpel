package pkg

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRequestArgsDoNotTransportTimestampChoices(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeOf(RequestServerArgs{}), reflect.TypeOf(RequestServerReply{})} {
		for i := 0; i < typ.NumField(); i++ {
			if strings.Contains(strings.ToLower(typ.Field(i).Name), "timestamp") {
				t.Fatalf("timestamp negotiation field in %s", typ)
			}
		}
	}
}

func TestServerUsesOnlyItsLocalTimestampConfig(t *testing.T) {
	config := TimestampConfig{TX: TimestampSoftware, RX: TimestampHardware}
	s := NewServer(context.Background(), nil, config)
	args := RequestServerArgs{ID: "local-config", Timeout: time.Second, ServerMode: ReceiveUDP}
	var reply RequestServerReply
	if err := s.RequestServer(args, &reply); err != nil {
		t.Fatal(err)
	}
	var abort AbortReply
	if err := s.Abort(AbortArgs{ID: args.ID}, &abort); err != nil {
		t.Fatal(err)
	}
	var result RequestServerResultReply
	if err := s.RequestServerResult(RequestServerResultArgs{ID: args.ID}, &result); err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.TimestampReport.LocalConfig != config || result.TimestampReport.RX.Source != TimestampHardware {
		t.Fatalf("server lost local config/selection: %+v", result)
	}
}

func TestRequestServerResultPreservesTimestampReportOnFailure(t *testing.T) {
	s := NewServer(context.Background(), nil)
	config, _ := (TimestampConfig{}).Normalize()
	report := TimestampReport{LocalConfig: config, RX: TimestampDirectionReport{
		Source: TimestampSoftware, PacketCount: 3, MissingCount: 1,
	}}
	resultC := make(chan *Result, 1)
	resultC <- &Result{Err: errors.New("collection failed"), Report: report}
	close(resultC)
	s.resultC["failed"] = resultC
	var reply RequestServerResultReply
	if err := s.RequestServerResult(RequestServerResultArgs{ID: "failed"}, &reply); err != nil {
		t.Fatal(err)
	}
	if !reply.Complete || reply.Error == "" || !reflect.DeepEqual(reply.TimestampReport, report) {
		t.Fatalf("failure report was lost: %+v", reply)
	}
}

func TestGatherPreservesPeerReportWithoutReapplyingSelectionPolicy(t *testing.T) {
	RegisterGob()
	for _, remoteError := range []string{"", "collection failed"} {
		t.Run(remoteError, func(t *testing.T) {
			// Deliberately omit explanatory metadata and leave the inactive
			// preference defaulted. Collection correctness belongs to the peer.
			report := TimestampReport{LocalConfig: TimestampConfig{RX: TimestampHardware}, RX: TimestampDirectionReport{
				Requested: TimestampHardware, Source: TimestampHardware, PacketCount: 1, ClockDomain: "nic_phc",
			}}
			service := &resultRPC{reply: RequestServerResultReply{Complete: true, Error: remoteError,
				Result: []MsgRcvd{{Seq: 0, TsRcvd: time.Unix(10, 1), Len: 8}}, TimestampReport: report}}
			client := &SenderClient{Direction: UL, Sender: &BurstSender{}, ID: "report",
				Out:             filepath.Join(t.TempDir(), "result.csv"),
				TimestampConfig: TimestampConfig{TX: TimestampApp, RX: TimestampSoftware},
				UDPMsgsSent:     []MsgSent{{Seq: 0, TsSent: time.Unix(10, 0), Len: 8}}}
			err := client.Gather(context.Background(), rpcTestClient(t, service))
			if (err != nil) != (remoteError != "") {
				t.Fatalf("Gather error = %v", err)
			}
			got := client.TimestampReport()
			if got.RX != report.RX || got.RemoteConfig == nil || *got.RemoteConfig != report.LocalConfig {
				t.Fatalf("remote report changed: %+v", got)
			}
			_, statErr := os.Stat(client.Out)
			if remoteError != "" && !os.IsNotExist(statErr) {
				t.Fatalf("failed run produced CSV: %v", statErr)
			}
		})
	}
}

func TestServerTxHardwarePreferenceIsReportedAsSoftwareForDL(t *testing.T) {
	serverConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer serverConn.Close()
	peerConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer peerConn.Close()
	config := TimestampConfig{TX: TimestampHardware, RX: TimestampApp}
	sender := &BurstSender{Params: BurstParams{Timeout: time.Second, Num: 1}}
	configureUDPSender(sender, config.TX)
	if _, err := handleSender(context.Background(), serverConn, RequestServerArgs{Params: sender.Params}, sender, peerConn.LocalAddr()); err != nil {
		t.Fatal(err)
	}
	report := makeTimestampReport(config, SendBurst, sender)
	if report.LocalConfig != config || report.TX.Requested != TimestampHardware || report.TX.Source != TimestampSoftware ||
		report.TX.SelectionReason == "" || report.TX.PacketCount != 1 || report.TX.MissingCount != 0 {
		t.Fatalf("server TX selection = %+v", report)
	}
}
