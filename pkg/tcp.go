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
	"syscall"
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
func (t *TCPMonitor) Run(ctx context.Context, conn net.Conn, drainQueue bool) {
	defer func() {
		close(t.C)
	}()

	tc, err := tcp.NewConn(conn)
	if err != nil {
		t.Err = fmt.Errorf("Failed opening tcp info conn: %w", err)
		return
	}

	ccName, err := tcpCCName(ctx, tc)
	if err != nil {
		t.Err = fmt.Errorf("Failed getCCName: %w", err)
		return
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

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

outer:
	for {
		select {
		case <-ticker.C:
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
			// Wait for the send queue to drain only on natural test end
			// (all bytes sent, or the duration deadline was reached); on
			// user abort return immediately.
			cause := context.Cause(ctx)
			if drainQueue && (errors.Is(cause, errIODone) || errors.Is(cause, context.DeadlineExceeded)) {
				break outer
			}
			return
		}
	}

	timeoutDuration := 60 * time.Second
	timeout := time.NewTimer(timeoutDuration)
	slog.DebugContext(ctx, "Wait for TCP send queue to drain", "timeout", timeoutDuration)

	for {
		select {
		case <-ticker.C:
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

// Cancellation cause used to signal TCPMonitor that the IO function finished
// naturally (all bytes sent/received), as opposed to a user abort.
var errIODone = errors.New("io completed")

func runTCPMonitorAndIO(ctx context.Context, conn net.Conn, ioFn func(chan error), drainQueue bool) ([]TCPMetric, error) {
	tcpMonitor := NewTCPMonitor()
	monitorCtx, monitorCtxCancel := context.WithCancelCause(ctx)
	defer monitorCtxCancel(nil) // satisfy vet
	go tcpMonitor.Run(monitorCtx, conn, drainQueue)

	ioErrC := make(chan error, 1)
	go ioFn(ioErrC)

	// Wait for either TCPMonitor or IO to terminate
	select {
	case <-tcpMonitor.C: // fires when tcpMonitor has terminated
		if tcpMonitor.Err != nil {
			return nil, tcpMonitor.Err
		}
	case ioErr := <-ioErrC:
		// Also terminate tcpMonitor; the cause lets it distinguish natural
		// IO completion (drain desired) from a user abort (return now).
		monitorCtxCancel(errIODone)
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
	slog.DebugContext(ctx, "TCPMonitor has terminated, return", "error", tcpMonitor.Err)
	return tcpMonitor.Res, tcpMonitor.Err
}

type TCPReceiver struct {
}

func (r *TCPReceiver) Init() {}
func (r *TCPReceiver) Run(ctx context.Context, ln net.Listener) (any, error) {
	// Close the listener when the context ends so that a blocked Accept
	// returns instead of waiting for a client forever.
	stop := context.AfterFunc(ctx, func() { ln.Close() })
	defer stop()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("Cancelled while waiting for TCP client: %w", ctx.Err())
			}
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

		return runTCPMonitorAndIO(ctx, conn, recvFn, false)
	}
}

func (r *TCPReceiver) run(ctx context.Context, conn net.Conn) error {
	slog.DebugContext(ctx, "Reading from TCP conn")
	n, err := io.Copy(droppingWriter{}, conn)
	slog.DebugContext(ctx, "TCPReceiver: io.Copy returned", "n", n)
	if err != nil && !(errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, syscall.ECONNRESET)) {
		// ECONNRESET expected since the server closes the connection with Linger 0
		return fmt.Errorf("Unexpected TCPReceiver error: %w", err)
	}
	return nil
}

type TCPCCA int

const (
	CUBIC TCPCCA = iota
	CUBIC_NO_HYSTART
	CUBIC_HYSTARTPP
	CUBIC_SEARCH
	CUBIC_SUSS
	BBR1
	BBR3
	ILLINOIS
	LEOCC
	SATPIPE
)

var TCPCCAS []TCPCCA = []TCPCCA{CUBIC, CUBIC_NO_HYSTART, CUBIC_HYSTARTPP, CUBIC_SEARCH, CUBIC_SUSS, BBR1, BBR3, ILLINOIS, LEOCC, SATPIPE}

// Used to set CCA with sysctl.
// Returns an error if BBRv3 is likely not available.
// Does not check if the others are available since they may be loaded when trying to be used.
func (c TCPCCA) KernelName() (string, error) {
	if c == BBR1 || c == BBR3 {
		bbr3Av := c.bbr3Available()
		if c == BBR1 {
			if bbr3Av {
				return "bbr1", nil
			} else {
				return "bbr", nil
			}
		} else {
			if bbr3Av {
				return "bbr", nil
			} else {
				return "", fmt.Errorf("BBRv3 is not available")
			}
		}
	}

	switch c {
	case CUBIC, CUBIC_NO_HYSTART:
		return "cubic", nil
	case CUBIC_HYSTARTPP:
		return "cubic_hystartpp", nil
	case CUBIC_SEARCH:
		return "cubic_search", nil
	case CUBIC_SUSS:
		return "cubic_suss", nil
	case ILLINOIS:
		return "illinois", nil
	case LEOCC:
		return "leocc", nil
	case SATPIPE:
		return "satpipe", nil
	default:
		panic("Unknown TCPCC")
	}
}

func (c TCPCCA) bbr3Available() bool {
	ccas, err := readAvailableKernelCCAs()
	if err != nil {
		panic(fmt.Sprintf("Failed to fetch kernel CCAs: %v", err))
	}

	// If true: the kernel is compiled with BBRv3 and, BBRv1 is "bbr1", and BBRv3 is "bbr"
	// If false: either it's not a BBRv3 kernel (and BBRv1 is "bbr") or (false positive)
	// "bbr1" is not loaded.
	return strings.Contains(ccas, "bbr1")
}

// Human-facing string
func (c TCPCCA) String() string {
	switch c {
	case CUBIC:
		return "cubic"
	case CUBIC_NO_HYSTART:
		return "cubic-nohy"
	case CUBIC_HYSTARTPP:
		return "hystartpp"
	case CUBIC_SEARCH:
		return "search"
	case CUBIC_SUSS:
		return "suss"
	case BBR1:
		return "bbr1"
	case BBR3:
		return "bbr3"
	case ILLINOIS:
		return "illinois"
	case LEOCC:
		return "leocc"
	case SATPIPE:
		return "satpipe"
	default:
		panic("Unknown TCPCC")
	}
}

// Parse human-facing name of CCA
func ParseTCPCCA(cca string) (TCPCCA, error) {
	switch strings.ToLower(cca) {
	case "cubic":
		return CUBIC, nil
	case "cubic-nohy":
		return CUBIC_NO_HYSTART, nil
	case "hystartpp":
		return CUBIC_HYSTARTPP, nil
	case "search":
		return CUBIC_SEARCH, nil
	case "suss":
		return CUBIC_SUSS, nil
	case "bbr1":
		return BBR1, nil
	case "bbr3":
		return BBR3, nil
	case "illinois":
		return ILLINOIS, nil
	case "leocc":
		return LEOCC, nil
	case "satpipe":
		return SATPIPE, nil
	default:
		return 999, fmt.Errorf("Unknown TCPCCA value '%s'", cca)
	}
}

// Set Bytes to 0 to send until Duration_ is reached. Otherwise sending will
// terminate once Duration_ is reached or Bytes has been acked, whatever happens
// first.
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
	// if s.Params.Duration_ != time.Duration(0) && s.Parms.Bytes != 0 {
	// 	return nil, fmt.Errorf("TCPSender: Duration and Bytes are exclusive")
	// }

	if err := applyTCPCCA(ctx, conn.(*net.TCPConn), s.Params.CCA); err != nil {
		return nil, err
	}

	// https://ndeepak.com/posts/2016-10-21-tcprst/
	// Discard all queued data and close connection immediately. We use this to actually
	// stop sending after Duration_.
	// The client will receive a ECONNRESET error.
	if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
		return nil, err
	}

	// If the sender stops because Duration has been reached, TCP Monitor will
	// not for wait the send queue to drain but stop abruptly.
	//
	// If the sender stops because Bytes-many bytes have been sent, TCP Monitor
	// will only terminate once the send queue is empty (-> all bytes have been
	// ACKed).
	drainQueue := s.Params.Bytes > 0

	sendFn := func(c chan error) {
		sendBytes := int64(s.Params.Bytes)
		if sendBytes == 0 {
			sendBytes = 1 << 62
		}
		n, err := io.CopyN(conn, mathRand.New(mathRand.NewSource(0)), sendBytes)
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

	return runTCPMonitorAndIO(ctx, conn, sendFn, drainQueue)
}
