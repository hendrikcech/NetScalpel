package experiment

import "github.com/hendrikcech/netscalpel/pkg"

// Configuration is the stable JSON boundary for existing measurement parameters,
// not a new set of runtime parameters. It is independent of Go field names and
// enum ordinals. All durations use nanoseconds, byte counts use bytes, and pacing
// configuration is not a claim about runtime socket activation.
type Configuration struct {
	DurationNS        int64           `json:"duration_ns"`
	PacketCount       *uint           `json:"packet_count,omitempty"`
	PaddingBytes      *uint           `json:"padding_bytes,omitempty"`
	IntervalNS        *int64          `json:"interval_ns,omitempty"`
	PacketsPerSecond  *uint           `json:"packets_per_second,omitempty"`
	PayloadSizeBytes  *uint           `json:"payload_size_bytes,omitempty"`
	Bytes             *uint           `json:"bytes,omitempty"`
	CongestionControl string          `json:"congestion_control,omitempty"`
	ClientEchoID      *uint16         `json:"client_echo_id,omitempty"`
	SenderEchoID      *uint16         `json:"sender_echo_id,omitempty"`
	Windows           []Configuration `json:"windows,omitempty"`
	Name              string          `json:"name,omitempty"`
	Filter            string          `json:"filter,omitempty"`
}

func ptr[T any](v T) *T { return &v }

func senderConfiguration(params pkg.SenderParams) Configuration {
	c := Configuration{DurationNS: int64(params.GetDuration())}
	switch p := params.(type) {
	case pkg.BurstParams:
		c.PacketCount = ptr(p.Num)
		c.PaddingBytes = ptr(p.Pad)
	case pkg.PeriodicParams:
		c.IntervalNS = ptr(int64(p.Interval))
		c.PaddingBytes = ptr(p.Pad)
	case pkg.RateParams:
		c.IntervalNS = ptr(int64(p.Interval))
		c.PacketsPerSecond = ptr(p.Pps)
		c.PayloadSizeBytes = ptr(p.PayloadSize)
	case pkg.RateParamsW:
		for _, window := range p {
			c.Windows = append(c.Windows, senderConfiguration(window))
		}
	case pkg.TCPSenderParams:
		c.Bytes = ptr(p.Bytes)
		c.CongestionControl = p.CCA.String()
	case pkg.QUICParams:
		c.Bytes = ptr(p.Bytes)
	case pkg.ICMPParams:
		c.IntervalNS = ptr(int64(p.Interval))
		c.ClientEchoID = ptr(p.ClientEchoID)
		c.SenderEchoID = ptr(p.SenderEchoID)
	}
	return c
}

func commandConfiguration(params pkg.CommandParams) Configuration {
	c := Configuration{DurationNS: int64(params.Timeout()), Name: params.Name()}
	if p, ok := params.(pkg.TcpdumpParams); ok {
		c.Filter = p.Filter
	}
	return c
}

// ManifestParameters preserves procedure-defined names/units, but serializes
// congestion control choices as names rather than Go enum ordinals.
func ManifestParameters(params ParamMap) ParamMap {
	result := make(ParamMap, len(params))
	for name, value := range params {
		switch v := value.(type) {
		case pkg.TCPCCA:
			result[name] = v.String()
		case []pkg.TCPCCA:
			names := make([]string, len(v))
			for i, cca := range v {
				names[i] = cca.String()
			}
			result[name] = names
		default:
			result[name] = value
		}
	}
	return result
}
