package pkg

import (
	"context"
	"net"
	"time"
)

type TCPReceiver struct {
}

func (r *TCPReceiver) Init() {}
func (r *TCPReceiver) Run(ctx context.Context, listener net.Listener) (any, error) {
	return nil, nil
}

type TCPSenderParams struct {
}

func (p TCPSenderParams) GetDuration() time.Duration {
	// TODO
	return time.Duration(0)
}

type TCPSender struct {
	Params TCPSenderParams
}

func (s *TCPSender) GetParams() SenderParams {
	return s.Params
}

func (s *TCPSender) SenderMode() Mode {
	return SendTCP
}

func (s *TCPSender) ReceiverMode() Mode {
	return ReceiveTCP
}

func (s *TCPSender) Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error) {
	return nil, nil
}
