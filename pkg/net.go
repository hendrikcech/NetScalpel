package pkg

import (
	"context"
	"fmt"
	"net"
	"time"
)

type Sender interface {
	GetParams() SenderParams
	SenderMode() Mode
	ReceiverMode() Mode
	Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error)
}

type SenderParams interface {
	NumPackets() uint
	GetDuration() time.Duration
}

type Receiver interface {
	Init()
	Run(ctx context.Context, conn net.Conn, expectedNumPackets uint) (any, error)
}

type Mode int

const (
	ReceiveUDP Mode = iota
	SendBurst
	SendRate
	SendPeriodic
	ReceiveQUIC
	SendQUIC
	ReceiveTCP
	SendTCP
)

func (m Mode) String() string {
	switch m {
	case ReceiveUDP:
		return "receive"
	case SendBurst:
		return "burst"
	case SendRate:
		return "rate"
	case SendPeriodic:
		return "periodic"
	case ReceiveQUIC:
		return "receiveQUIC"
	case SendQUIC:
		return "sendQUIC"
	case ReceiveTCP:
		return "receiveTCP"
	case SendTCP:
		return "sendTCP"
	default:
		panic(fmt.Sprintf("Unknown Mode '%d'", m))
	}
}
