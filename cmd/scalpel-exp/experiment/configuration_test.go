package experiment

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/pkg"
)

func TestConfigurationJSONUsesExplicitUnitsAndEnumNames(t *testing.T) {
	cases := []struct {
		params pkg.SenderParams
		want   string
	}{
		{pkg.TCPSenderParams{Duration_: time.Second, Bytes: 123, CCA: pkg.CUBIC}, `"congestion_control":"cubic"`},
		{pkg.PeriodicParams{Duration: time.Second, Interval: time.Millisecond, Pad: 7}, `"interval_ns":1000000`},
		{pkg.RateParamsW{{Duration: time.Second, Pps: 25, PayloadSize: 1400}}, `"packets_per_second":25`},
		{pkg.BurstParams{Timeout: time.Second, Num: 5, Pad: 7}, `"packet_count":5`},
		{pkg.QUICParams{Duration_: time.Second, Bytes: 7}, `"bytes":7`},
		{pkg.ICMPParams{Duration_: time.Second, Interval: time.Millisecond, ClientEchoID: 42}, `"client_echo_id":42`},
	}
	for _, tc := range cases {
		b, err := json.Marshal(senderConfiguration(tc.params))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(b), `"duration_ns":1000000000`) || !strings.Contains(string(b), tc.want) || strings.Contains(string(b), "Duration_") || strings.Contains(string(b), `"CCA"`) {
			t.Fatalf("unstable config: %s", b)
		}
	}
	b, _ := json.Marshal(commandConfiguration(pkg.TcpdumpParams{Name_: "capture", Timeout_: time.Second, Filter: "udp"}))
	if string(b) != `{"duration_ns":1000000000,"name":"capture","filter":"udp"}` {
		t.Fatalf("command config: %s", b)
	}
	params := ParamMap{"ccas": []pkg.TCPCCA{pkg.CUBIC}}
	encoded, _ := json.Marshal(ManifestParameters(params))
	if string(encoded) != `{"ccas":["cubic"]}` {
		t.Fatal(string(encoded))
	}
	if _, ok := params["ccas"].([]pkg.TCPCCA); !ok {
		t.Fatal("mutated execution parameters")
	}
}
