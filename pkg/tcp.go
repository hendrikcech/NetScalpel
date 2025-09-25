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

type TCPMonitor struct {
	C   chan bool // Closed when TCPMonitor has terminated
	Res []TCPMetric
	Err error
}

func NewTCPMonitor() *TCPMonitor {
	return &TCPMonitor{
		C: make(chan bool),
	}
}

// Returns until ctx.Done()
func (t *TCPMonitor) Run(ctx context.Context, conn net.Conn) {
	defer func() {
		close(t.C)
	}()

	tc, err := tcp.NewConn(conn)
	if err != nil {
		t.Err = fmt.Errorf("Failed opening tcp info conn: %v", err.Error())
	}

	ccName, err := tcpCCName(ctx, tc)
	if err != nil {
		t.Err = fmt.Errorf("Failed getCCName: %v", err.Error())
	}
	bbrUsed := strings.HasPrefix(ccName, "bbr")

	var b [256]byte

	queryTCPOnce := func() error {
		m := TCPMetric{Time: time.Now()}
		var err error
		if m.Info, err = queryTCPInfo(ctx, tc, b[:]); err != nil {
			return err
		}
		if bbrUsed {
			if m.BBRInfo, err = queryBBRInfo(ctx, tc, b[:]); err != nil {
				return err
			}
		}
		t.Res = append(t.Res, m)
		// slog.InfoContext(ctx, "TCPInfo", "info", m, "type", fmt.Sprintf("%T", info), "cc", info.CongestionControl)
		// slog.InfoContext(ctx, "BBRInfo", "info", info, "type", fmt.Sprintf("%T", info))
		return nil
	}

	// Call it at least once even for very short transfer
	if err := queryTCPOnce(); err != nil {
		t.Err = err
		return
	}
	ticker := time.Tick(5 * time.Millisecond)
outer:
	for {
		select {
		case <-ticker:
			if err := queryTCPOnce(); err != nil {
				slog.ErrorContext(ctx, "TCPMonitor Run", "error", err)
				t.Err = err
				return
			}
		case <-ctx.Done():
			if err := queryTCPOnce(); err != nil {
				t.Err = err
				return
			}
			break outer
		}
	}

	timeoutDuration := 10 * time.Second
	timeout := time.NewTimer(timeoutDuration)
	slog.DebugContext(ctx, "Wait for TCP send queue to drain", "timeout", timeoutDuration)

	for {
		select {
		case <-ticker:
			if err := queryTCPOnce(); err != nil {
				t.Err = err
				return
			}
			// Terminate once send queue has been drained and all bytes have been received
			if len(t.Res) > 0 {
				info := t.Res[len(t.Res)-1].Info.Sys
				if info.UnackedSegs == 0 && info.NotSentBytes == 0 {
					return
				}
			}
		case <-timeout.C:
			slog.WarnContext(ctx, "TCP send queue did not drain in-time", "timeout", timeoutDuration)
			return
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

func runTCPMonitorAndIO(ctx context.Context, conn net.Conn, ioFn func(chan error)) ([]TCPMetric, error) {
	tcpMonitor := NewTCPMonitor()
	monitorCtx, monitorCtxCancel := context.WithCancel(ctx)
	defer monitorCtxCancel() // satisfy vet
	go tcpMonitor.Run(monitorCtx, conn)

	ioErrC := make(chan error, 1)
	go ioFn(ioErrC)

	// Wait for either TCPMonitor or IO to terminate
	select {
	case <-tcpMonitor.C: // fires when tcpMonitor has terminated
		if tcpMonitor.Err != nil {
			return nil, tcpMonitor.Err
		}
	case ioErr := <-ioErrC:
		// Also terminate tcpMonitor
		monitorCtxCancel()
		if ioErr != nil {
			return nil, ioErr
		}
	case <-ctx.Done():
	}

	// Terminate sender
	conn.SetWriteDeadline(time.Now())

	// TCPMonitor has terminated without error because of tcp.Done()
	// -> IO also terminates because of WriteDeadline
	//
	// IO has terminated (because all bytes were sent / received) without error
	// -> TCPMonitor will also terminate because of monitorCtxCancel()

	// Make sure TCPMonitor has terminated
	<-tcpMonitor.C
	return tcpMonitor.Res, tcpMonitor.Err
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

		recvFn := func(c chan error) {
			defer close(c)
			if err := r.run(ctx, conn); err != nil {
				c <- err
			}
			slog.DebugContext(ctx, "TCPReceiver run returned")
		}

		return runTCPMonitorAndIO(ctx, conn, recvFn)
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
	CUBIC            TCPCCA = iota
	CUBIC_NO_HYSTART TCPCCA = iota
	BBR
)

// Used to set CCA with sysctl
func (c TCPCCA) KernelName() string {
	switch c {
	case CUBIC, CUBIC_NO_HYSTART:
		return "cubic"
	case BBR:
		return "bbr"
	default:
		panic("Unknown TCPCC")
	}
}

// Human-facing string
func (c TCPCCA) String() string {
	switch c {
	case CUBIC:
		return "cubic"
	case CUBIC_NO_HYSTART:
		return "cubic-nohy"
	case BBR:
		return "bbr"
	default:
		panic("Unknown TCPCC")
	}
}

// Parse human-facing name of CCA
func ParseTCPCCA(cca string) (TCPCCA, error) {
	switch strings.ToLower(cca) {
	case "cubic":
		return CUBIC, nil
	case "cubic_nohy":
		return CUBIC_NO_HYSTART, nil
	case "bbr":
		return BBR, nil
	default:
		return 999, fmt.Errorf("Unknown TCPCCA value '%s'", cca)
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
	if err := applyTCPCCA(ctx, conn.(*net.TCPConn), s.Params.CCA); err != nil {
		return nil, err
	}

	// if err := setTCPLingerOff(conn.(*net.TCPConn)); err != nil {
	// 	return nil, err
	// }

	sendFn := func(c chan error) {
		n, err := io.CopyN(conn, mathRand.New(mathRand.NewSource(0)), int64(s.Params.Bytes))
		if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
			// Actual error
			slog.ErrorContext(ctx, "io.copyN returned error", "error", err)
			c <- err
		} else {
			// Expected timeout error (SetWriteDeadline) to break send
			slog.DebugContext(ctx, "io.copyN returned", "n", n)
		}
		close(c)
	}

	return runTCPMonitorAndIO(ctx, conn, sendFn)
}
