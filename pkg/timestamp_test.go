package pkg

import (
	"context"
	"encoding/json"
	"testing"
)

func TestTimestampConfigNormalize(t *testing.T) {
	config, err := (TimestampConfig{}).Normalize()
	if err != nil || config != (TimestampConfig{TX: TimestampSoftware, RX: TimestampSoftware}) {
		t.Fatalf("default config = %+v, %v", config, err)
	}
	for _, config := range []TimestampConfig{{TX: 255}, {RX: 255}} {
		if _, err := config.Normalize(); err == nil {
			t.Fatalf("accepted invalid config %+v", config)
		}
	}
}

func TestParseTimestampSource(t *testing.T) {
	for input, want := range map[string]TimestampSource{
		"": TimestampDefault, " sw ": TimestampSoftware, "software": TimestampSoftware,
		" HW ": TimestampHardware, "hardware": TimestampHardware, "app": TimestampApp,
	} {
		got, err := ParseTimestampSource(input)
		if err != nil || got != want {
			t.Fatalf("ParseTimestampSource(%q) = %v, %v", input, got, err)
		}
		encoded, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		var decoded TimestampSource
		if err := json.Unmarshal(encoded, &decoded); err != nil || decoded != got {
			t.Fatalf("JSON roundtrip %s = %v, %v", encoded, decoded, err)
		}
	}
	for _, input := range []string{"auto", "kernel", "unknown", "not_applicable"} {
		if _, err := ParseTimestampSource(input); err == nil {
			t.Fatalf("accepted %q", input)
		}
	}
	if _, err := json.Marshal(TimestampSource(255)); err == nil {
		t.Fatal("serialized invalid source")
	}
}

func TestParseTimestampConfigAliases(t *testing.T) {
	got, err := ParseTimestampConfig("hw", "sw")
	if err != nil || got != (TimestampConfig{TX: TimestampHardware, RX: TimestampSoftware}) {
		t.Fatalf("config = %+v, %v", got, err)
	}
	for _, args := range [][2]string{{"bad", "sw"}, {"sw", "bad"}} {
		if _, err := ParseTimestampConfig(args[0], args[1]); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}

func TestUDPSourceSelection(t *testing.T) {
	for _, tx := range []bool{false, true} {
		for _, requested := range []TimestampSource{TimestampDefault, TimestampSoftware, TimestampHardware, TimestampApp} {
			report, err := selectTimestampSource(context.Background(), requested, tx)
			if requested == TimestampDefault {
				requested = TimestampSoftware
			}
			want := requested
			if tx && requested == TimestampHardware {
				want = TimestampSoftware
			}
			if err != nil || report.Source != want || report.Requested != requested {
				t.Fatalf("selection tx=%v requested=%s: %+v, %v", tx, requested, report, err)
			}
			if (report.SelectionReason != "") != (want != requested) {
				t.Fatalf("only downgrades need a reason: %+v", report)
			}
			clock := "system_realtime"
			if want == TimestampHardware {
				clock = "nic_phc"
			}
			if report.ClockDomain != clock {
				t.Fatalf("clock domain = %q, want %q", report.ClockDomain, clock)
			}
		}
	}
	if _, err := selectTimestampSource(context.Background(), 255, true); err == nil {
		t.Fatal("accepted invalid source")
	}
}

func TestNonUDPModesHaveNoTimestampDirections(t *testing.T) {
	for _, mode := range []Mode{SendICMP, ReceiveICMP, SendQUIC, ReceiveQUIC, SendTCP, ReceiveTCP} {
		if packetTimestampMode(mode) {
			t.Fatalf("%s participates in UDP timestamp policy", mode)
		}
		report := makeTimestampReport(TimestampConfig{}, mode, &BurstSender{})
		if report.TX != (TimestampDirectionReport{}) || report.RX != (TimestampDirectionReport{}) {
			t.Fatalf("%s fabricated timestamp evidence: %+v", mode, report)
		}
	}
}

func TestTimestampReportJSONContainsOnlyCollectionMetadata(t *testing.T) {
	report, err := selectTimestampSource(context.Background(), TimestampSoftware, true)
	if err != nil {
		t.Fatal(err)
	}
	report.PacketCount = 4
	report.MissingCount = 1
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 6 || fields["source"] != "software" || fields["requested"] != "software" ||
		fields["clock_domain"] != "system_realtime" || fields["packet_count"] != float64(4) ||
		fields["missing_count"] != float64(1) || fields["invalid_count"] != float64(0) {
		t.Fatalf("unexpected report schema: %s", data)
	}
}
