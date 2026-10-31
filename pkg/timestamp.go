package pkg

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

// TimestampConfig selects host-local UDP timestamp sources.
type TimestampConfig struct {
	TX TimestampSource `json:"tx"`
	RX TimestampSource `json:"rx"`
}

type TimestampSource uint8

const (
	TimestampDefault TimestampSource = iota
	TimestampSoftware
	TimestampHardware
	TimestampApp
)

func (s TimestampSource) String() string {
	switch s {
	case TimestampDefault:
		return ""
	case TimestampSoftware:
		return "software"
	case TimestampHardware:
		return "hardware"
	case TimestampApp:
		return "app"
	default:
		return fmt.Sprintf("TimestampSource(%d)", s)
	}
}

func ParseTimestampSource(value string) (TimestampSource, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return TimestampDefault, nil
	case "software", "sw":
		return TimestampSoftware, nil
	case "hardware", "hw":
		return TimestampHardware, nil
	case "app":
		return TimestampApp, nil
	default:
		return TimestampDefault, fmt.Errorf("timestamp source %q must be hardware (hw), software (sw), or app", value)
	}
}

func (s TimestampSource) MarshalText() ([]byte, error) {
	if s > TimestampApp {
		return nil, fmt.Errorf("invalid timestamp source %d", s)
	}
	return []byte(s.String()), nil
}

func (s *TimestampSource) UnmarshalText(text []byte) error {
	parsed, err := ParseTimestampSource(string(text))
	if err == nil {
		*s = parsed
	}
	return err
}

func ParseTimestampConfig(tx, rx string) (TimestampConfig, error) {
	var config TimestampConfig
	var err error
	if config.TX, err = ParseTimestampSource(tx); err != nil {
		return config, fmt.Errorf("TX %w", err)
	}
	if config.RX, err = ParseTimestampSource(rx); err != nil {
		return config, fmt.Errorf("RX %w", err)
	}
	return config.Normalize()
}

func (c TimestampConfig) Normalize() (TimestampConfig, error) {
	if c.TX == TimestampDefault {
		c.TX = TimestampSoftware
	}
	if c.RX == TimestampDefault {
		c.RX = TimestampSoftware
	}
	if c.TX < TimestampSoftware || c.TX > TimestampApp {
		return TimestampConfig{}, fmt.Errorf("TX timestamp source %q must be hardware, software, or app", c.TX)
	}
	if c.RX < TimestampSoftware || c.RX > TimestampApp {
		return TimestampConfig{}, fmt.Errorf("RX timestamp source %q must be hardware, software, or app", c.RX)
	}
	return c, nil
}

func packetTimestampMode(mode Mode) bool {
	return mode == ReceiveUDP || mode == SendBurst || mode == SendRate || mode == SendPeriodic
}

// TimestampDirectionReport is produced by UDP collection, not reconstructed by
// the RPC layer. Hardware timestamps remain in the NIC's unconverted PHC clock.
type TimestampDirectionReport struct {
	Requested       TimestampSource `json:"requested"`
	Source          TimestampSource `json:"source"`
	SelectionReason string          `json:"selection_reason,omitempty"`
	PacketCount     uint64          `json:"packet_count"`
	MissingCount    uint64          `json:"missing_count"`
	InvalidCount    uint64          `json:"invalid_count"`
	ClockDomain     string          `json:"clock_domain,omitempty"`
}

type TimestampReport struct {
	LocalConfig  TimestampConfig          `json:"local_config"`
	RemoteConfig *TimestampConfig         `json:"remote_config,omitempty"`
	TX           TimestampDirectionReport `json:"tx"`
	RX           TimestampDirectionReport `json:"rx"`
}

// selectTimestampSource is called once at UDP setup. Hardware TX is the only
// implementation-only downgrade; collection failures never switch sources.
func selectTimestampSource(ctx context.Context, requested TimestampSource, tx bool) (TimestampDirectionReport, error) {
	if requested == TimestampDefault {
		requested = TimestampSoftware
	}
	report := TimestampDirectionReport{Requested: requested, Source: requested}
	if requested < TimestampSoftware || requested > TimestampApp {
		return report, fmt.Errorf("invalid timestamp source %q", requested)
	}
	if tx && requested == TimestampHardware {
		report.Source = TimestampSoftware
		report.SelectionReason = "hardware TX is not implemented; selected software"
		slog.InfoContext(ctx, "Timestamp source downgraded", "requested_source", requested, "selected_source", report.Source)
	}
	report.ClockDomain = "system_realtime"
	if report.Source == TimestampHardware {
		report.ClockDomain = "nic_phc"
	}
	return report, nil
}

type timestampReporter interface {
	TimestampReport() TimestampDirectionReport
}

func makeTimestampReport(config TimestampConfig, mode Mode, reporter any) TimestampReport {
	report := TimestampReport{LocalConfig: config}
	if source, ok := reporter.(timestampReporter); ok && packetTimestampMode(mode) {
		if mode == ReceiveUDP {
			report.RX = source.TimestampReport()
		} else {
			report.TX = source.TimestampReport()
		}
	}
	return report
}

func configureUDPSender(sender Sender, source TimestampSource) {
	switch sender := sender.(type) {
	case *BurstSender:
		sender.TimestampSource = source
	case *PeriodicSender:
		sender.TimestampSource = source
	case *RateSender:
		sender.TimestampSource = source
	}
}
