package pkg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mathRand "math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/mikioh/tcp"
	"github.com/mikioh/tcpinfo"
)

type TCPMetric struct {
	Time time.Time
	Info *tcpinfo.Info
	// Only set if BBR was used
	BBRInfo *tcpinfo.BBRInfo
}

// Returns on ctx.Done()
func monitorTCP(ctx context.Context, conn net.Conn) ([]TCPMetric, error) {
	tc, err := tcp.NewConn(conn)
	if err != nil {
		return nil, fmt.Errorf("Failed opening tcp info conn: %v", err.Error())
	}

	ccName, err := tcpCCName(ctx, tc)
	if err != nil {
		return nil, fmt.Errorf("Failed getCCName: %v", err.Error())
	}
	bbrUsed := strings.HasPrefix(ccName, "bbr")

	var metrics []TCPMetric

	ticker := time.Tick(5 * time.Millisecond)
	var b [256]byte
	for {
		select {
		case <-ticker:
			m := TCPMetric{Time: time.Now()}
			var err error
			if m.Info, err = queryTCPInfo(ctx, tc, b[:]); err != nil {
				return nil, err
			}
			if bbrUsed {
				if m.BBRInfo, err = queryBBRInfo(ctx, tc, b[:]); err != nil {
					return nil, err
				}
			}
			metrics = append(metrics, m)
			// slog.InfoContext(ctx, "TCPInfo", "info", m, "type", fmt.Sprintf("%T", info), "cc", info.CongestionControl)
			// slog.InfoContext(ctx, "BBRInfo", "info", info, "type", fmt.Sprintf("%T", info))
		case <-ctx.Done():
			return metrics, nil
		}
	}

}

func queryTCPInfo(ctx context.Context, tc *tcp.Conn, b []byte) (*tcpinfo.Info, error) {
	var o tcpinfo.Info
	for {
		opt, err := tc.Option(o.Level(), o.Name(), b)
		if err != nil {
			return nil, err
		}
		return opt.(*tcpinfo.Info), nil
	}
}

func queryBBRInfo(ctx context.Context, tc *tcp.Conn, b []byte) (*tcpinfo.BBRInfo, error) {
	var o tcpinfo.CCInfo
	_, err := tc.Option(o.Level(), o.Name(), b)
	if err != nil {
		return nil, err
	}
	info, err := tcpinfo.ParseCCAlgorithmInfo("bbr", b)
	if err != nil {
		return nil, err
	}
	return info.(*tcpinfo.BBRInfo), nil
}

func tcpCCName(ctx context.Context, tc *tcp.Conn) (string, error) {
	var o tcpinfo.CCAlgorithm
	var b [256]byte
	opt, err := tc.Option(o.Level(), o.Name(), b[:])
	if err != nil {
		return "", err
	}
	info := opt.(tcpinfo.CCAlgorithm)
	// slog.InfoContext(ctx, "CCAlgorithm", "info", info, "type", fmt.Sprintf("%T", info))
	return string(info), nil
}

type TCPReceiver struct {
}

func (r *TCPReceiver) Init() {}
func (r *TCPReceiver) Run(ctx context.Context, ln net.Listener) (any, error) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			// TODO: what to do here?
			slog.DebugContext(ctx, "TCPReceiver accept", "error", err.Error())
			return nil, err
		}

		// TODO: introduce support for handling multiple TCP clients

		go func() {
			<-ctx.Done()
			conn.SetWriteDeadline(time.Now())
		}()

		recvErrC := make(chan error, 1)
		go func() {
			defer close(recvErrC)
			if err := r.run(ctx, conn); err != nil {
				recvErrC <- err
			}
		}()

		metrics, err := monitorTCP(ctx, conn)
		if err != nil {
			return nil, err
		}

		recvErr := <-recvErrC
		if recvErr != nil {
			return nil, recvErr
		}

		return metrics, nil
	}
}

func (r *TCPReceiver) run(ctx context.Context, conn net.Conn) error {
	slog.DebugContext(ctx, "Reading from TCP conn")
	n, err := io.Copy(droppingWriter{}, conn)
	slog.DebugContext(ctx, "TCPReceiver: io.Copy returned", "n", n)
	if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("Unexpected TCPReceiver error: %v", err)
	}
	return nil
}

type TCPCCA int

const (
	CUBIC TCPCCA = iota
	BBR
)

func (c TCPCCA) String() string {
	switch c {
	case CUBIC:
		return "cubic"
	case BBR:
		return "bbr"
	default:
		panic("Unknown TCPCC")
	}
}

type TCPSenderParams struct {
	Duration_ time.Duration
	Bytes     uint
	CCA       TCPCCA
}

func (p TCPSenderParams) GetDuration() time.Duration {
	return p.Duration_
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
	if err := setTCPCC(ctx, conn.(*net.TCPConn), s.Params.CCA.String()); err != nil {
		return nil, err
	}

	go func() {
		<-ctx.Done()
		conn.SetWriteDeadline(time.Now())
	}()

	sendErrC := make(chan error, 1)
	go func() {
		defer close(sendErrC)
		n, err := io.CopyN(conn, mathRand.New(mathRand.NewSource(0)), int64(s.Params.Bytes))
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			// Actual error
			slog.ErrorContext(ctx, "io.copyN returned error", "error", err)
			sendErrC <- err
		} else {
			// Expected timeout error (SetWriteDeadline) to break send
			slog.DebugContext(ctx, "io.copyN returned", "n", n)
		}
	}()

	metrics, err := monitorTCP(ctx, conn)
	if err != nil {
		return nil, err
	}

	sendErr := <-sendErrC
	if sendErr != nil {
		return nil, sendErr
	}

	return metrics, nil
}
