package pkg

import (
	"bufio"
	"context"
	"fmt"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

type Client interface {
	Run(ctx context.Context, client *rpc.Client) error
	Gather(ctx context.Context, client *rpc.Client) error
	// Abort tells the server to cancel this test so its goroutines end
	// promptly after a user abort. The ctx must be a
	// live one: the round context is already cancelled at this point.
	Abort(ctx context.Context, client *rpc.Client) error
	Summary() string
}

var _ Client = (*SenderClient)(nil)
var _ Client = (*CommandClient)(nil)

// callCtx invokes client.Call but returns early with the context's error if
// ctx ends first (rpc.Client.Call itself is not context-aware and would
// block for as long as the server takes to respond).
// The abandoned call keeps running in the background; its reply is discarded
// when the RPC connection is closed.
func callCtx(ctx context.Context, client *rpc.Client, serviceMethod string, args any, reply any) error {
	call := client.Go(serviceMethod, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c := <-call.Done:
		return c.Error
	}
}

type SenderClient struct {
	IP        string
	Out       string
	Direction Direction
	StartAt   time.Time

	Sender Sender

	UDPMsgsSent []MsgSent
	UDPMsgsRcvd []MsgRcvd
	UDPResults  []MsgResult

	TCPMetricsSndr []TCPMetric
	TCPMetricsRcvr []TCPMetric

	ID string

	port uint // Remote port, returned by server through RequestServerReply
}

func (c *SenderClient) Run(ctx context.Context, client *rpc.Client) error {
	if c.ID == "" {
		c.ID = GenID(c.Sender.SenderMode().String())
		ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))
	}

	switch c.Direction {
	case UL:
		return c.runUL(ctx, client)
	case DL:
		return c.runDL(ctx, client)
	default:
		panic("Unknown Direction")
	}
}

func (c *SenderClient) runUL(ctx context.Context, client *rpc.Client) error {
	args := RequestServerArgs{
		ID:         c.ID,
		Timeout:    c.Sender.GetParams().GetDuration() + time.Second,
		StartAt:    c.StartAt,
		ServerMode: c.Sender.ReceiverMode(),
		Params:     c.Sender.GetParams(),
	}
	var reply RequestServerReply
	if err := callCtx(ctx, client, "Server.RequestServer", args, &reply); err != nil {
		return fmt.Errorf("Call Server.RequestServerReply failed: %w", err)
	}

	c.port = reply.Port

	var conn net.Conn
	var raddr net.Addr
	var err error
	switch args.ServerMode.SocketType() {
	case UDP:
		if raddr, err = net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%v", c.IP, reply.Port)); err != nil {
			return fmt.Errorf("Failed resolving provided UDP addr: %w", err)
		}
		if conn, err = listenUDP(ctx); err != nil {
			return fmt.Errorf("listenUDP failed: %w", err)
		}
	case TCP:
		// TCP Handshake is performed before the test starts
		addrStr := fmt.Sprintf("%s:%v", c.IP, reply.Port)
		// if raddr, err = net.ResolveTCPAddr("tcp", addrStr); err != nil {
		// 	return fmt.Errorf("Failed resolving provided TCP addr: %w", err)
		// }
		dialer := net.Dialer{}
		// We need to make space for the added TCP option with LeoCC
		if c.Sender.GetParams().(TCPSenderParams).CCA == LEOCC {
			dialer.Control = func(network, address string, rc syscall.RawConn) error {
				if err := LimitTCPMSS(rc, LeoCCMSS); err != nil {
					slog.ErrorContext(ctx, "Failed limiting TCP MSS on LeoCC UL socket", "mss", LeoCCMSS, "error", err)
				}
				return nil
			}
		}

		if conn, err = dialer.Dial("tcp", addrStr); err != nil {
			// if conn, err = net.DialTCP("tcp", nil, raddr.(*net.TCPAddr)); err != nil {
			return fmt.Errorf("net.DialTCP failed: %w", err)
		}
	case ICMP:
		conn, raddr, err = listenICMPClient(ctx, c.IP)
		if err != nil {
			return err
		}
		// Code smell. How else could this be solved?
		c.Sender.(*ICMPSender).Params.ICMPType = ipv4.ICMPTypeEcho
	default:
		panic("socket type not implemented")
	}
	defer func() {
		slog.DebugContext(ctx, "Closing conn")
		conn.Close()
	}()

	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	slog.InfoContext(ctx, "Start UL client", "type", fmt.Sprintf("%T", c.Sender),
		"params", c.Sender.GetParams(), "remoteAddr", raddr)

	sendCtx, sendCancel := context.WithTimeout(ctx, c.Sender.GetParams().GetDuration())
	defer sendCancel()

	res, err := c.Sender.Run(sendCtx, conn, raddr)
	if err != nil {
		return fmt.Errorf("UL failed: %w", err)
	}

	switch res.(type) {
	case []MsgSent:
		c.UDPMsgsSent = res.([]MsgSent)
	case []TCPMetric:
		c.TCPMetricsSndr = res.([]TCPMetric)
	default:
		// panic("Unhandled result type in runUL")
		slog.ErrorContext(ctx, "Unhandled result type in runUL", "result", res)
	}

	return nil
}

func (c *SenderClient) runDL(ctx context.Context, client *rpc.Client) error {
	slog.InfoContext(ctx, "Request DL server", "type", fmt.Sprintf("%T", c.Sender),
		"params", c.Sender.GetParams())

	args := RequestServerArgs{
		ID:         c.ID,
		Timeout:    c.Sender.GetParams().GetDuration() + time.Second,
		StartAt:    c.StartAt,
		ServerMode: c.Sender.SenderMode(),
		Params:     c.Sender.GetParams(),
	}
	var reply RequestServerReply
	if err := callCtx(ctx, client, "Server.RequestServer", args, &reply); err != nil {
		return fmt.Errorf("Call Server.RequestServerReply failed: %w", err)
	}

	c.port = reply.Port

	switch args.ServerMode.SocketType() {
	case UDP:
		return c.runDLUDP(ctx, reply.Port, args.Timeout)
	case TCP:
		return c.runDLTCP(ctx, reply.Port, args.Timeout)
	case ICMP:
		return c.runDLICMP(ctx, c.Sender.GetParams().(ICMPParams).SenderEchoID, args.Timeout)
	default:
		panic("Unknown SocketType")
	}
}

func (c *SenderClient) runDLUDP(ctx context.Context, rport uint, timeout time.Duration) error {
	raddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%v", c.IP, rport))
	if err != nil {
		return fmt.Errorf("Failed resolving provided UDP addr: %w", err)
	}

	conn, err := listenUDP(ctx)
	if err != nil {
		return fmt.Errorf("ListenUDP failed: %w", err)
	}
	defer conn.Close()
	// laddr := conn.LocalAddr().(*net.UDPAddr)

	if err := c.punchUDPHole(ctx, conn, raddr); err != nil {
		return fmt.Errorf("Return due to failed UDP probing: %w", err)
	}

	// slog.DebugContext(ctx, "Wrote UDP to server at %v, receiving at %v, timeout duration is %v\n", raddr, laddr, args.Timeout)
	var receiver Receiver
	switch c.Sender.ReceiverMode() {
	case ReceiveUDP:
		receiver = &UDPReceiver{}
	case ReceiveQUIC:
		receiver = &QUICReceiver{}
	default:
		panic("Unknown ServerMode")
	}

	receiver.Init()

	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	ln := NewDummyListener(conn, conn.LocalAddr())

	recvCtx, recvCancel := context.WithTimeout(ctx, timeout)
	defer recvCancel()
	res, err := receiver.Run(recvCtx, ln)
	if err != nil {
		return fmt.Errorf("Failed ReceiveFrom: %w", err)
	}

	switch res.(type) {
	case []MsgRcvd:
		c.UDPMsgsRcvd = res.([]MsgRcvd)
	default:
		panic("Unhandled result type in runDL")
	}

	slog.InfoContext(ctx, "Finished Run", "packets", len(c.UDPMsgsRcvd))

	return nil
}

func (c *SenderClient) runDLTCP(ctx context.Context, rport uint, timeout time.Duration) error {
	var receiver Receiver
	switch c.Sender.ReceiverMode() {
	case ReceiveTCP:
		receiver = &TCPReceiver{}
	default:
		panic("Unknown ServerMode")
	}

	receiver.Init()

	raddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%v", c.IP, rport))
	if err != nil {
		return fmt.Errorf("Failed resolving provided TCP addr: %w", err)
	}

	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return fmt.Errorf("net.DialTCP failed: %w", err)
	}
	defer conn.Close()

	// Also set the CCA on the receiving socket
	cca := c.Sender.GetParams().(TCPSenderParams).CCA
	if err := applyTCPCCA(ctx, conn, cca); err != nil {
		slog.WarnContext(ctx, "Failed to set CCA on the receiving socket", "cca", cca)
	} else {
		slog.DebugContext(ctx, "Set CCA on the receiving socket", "cca", cca)
	}

	ln := NewDummyListener(conn, conn.LocalAddr())
	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	recvCtx, recvCancel := context.WithTimeout(ctx, timeout)
	defer recvCancel()
	res, err := receiver.Run(recvCtx, ln)
	if err != nil {
		return fmt.Errorf("Failed ReceiveFrom: %w", err)
	}

	slog.InfoContext(ctx, "Finished Run")

	switch res.(type) {
	case []TCPMetric:
		c.TCPMetricsRcvr = res.([]TCPMetric)
	default:
		panic("Unhandled result type in runDL")
	}

	return nil
}

func (c *SenderClient) runDLICMP(ctx context.Context, echoID uint16, timeout time.Duration) error {
	conn, raddr, err := listenICMPClient(ctx, c.IP)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := c.punchICMPHole(ctx, conn.(net.PacketConn), raddr, echoID); err != nil {
		return fmt.Errorf("Return due to failed UDP probing: %w", err)
	}

	// slog.DebugContext(ctx, "Wrote UDP to server at %v, receiving at %v, timeout duration is %v\n", raddr, laddr, args.Timeout)
	if c.Sender.ReceiverMode() != ReceiveICMP {
		panic("Unknown ServerMode")
	}

	receiver := &ICMPReceiver{
		ClientEchoID: echoID,
		ICMPType:     ipv4.ICMPTypeEchoReply,
	}
	receiver.Init()

	slog.DebugContext(ctx, "Start ICMP puncher")
	// Send one ICMP request per second to keep the NAT hole open
	// Must start before the test will start to keep NAT open!
	puncher := ICMPSender{Params: ICMPParams{
		Duration_:    time.Until(c.StartAt) + timeout,
		Interval:     1 * time.Second,
		ClientEchoID: echoID,
		SenderEchoID: echoID,
		ICMPType:     ipv4.ICMPTypeEcho,
		punch:        true,
	}}
	go func() {
		if _, err := puncher.Run(ctx, conn, raddr); err != nil {
			slog.ErrorContext(ctx, "ICMP puncher failed", "error", err)
		}
	}()

	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	ln := NewDummyListener(conn, conn.LocalAddr())
	recvCtx, recvCancel := context.WithTimeout(ctx, timeout)
	defer recvCancel()
	res, err := receiver.Run(recvCtx, ln)
	if err != nil {
		return fmt.Errorf("Failed ReceiveFrom: %w", err)
	}

	switch res.(type) {
	case []MsgRcvd:
		c.UDPMsgsRcvd = res.([]MsgRcvd)
	default:
		panic("Unhandled result type in runDL")
	}

	slog.InfoContext(ctx, "Finished Run", "packets", len(c.UDPMsgsRcvd))

	return nil
}

func (c *SenderClient) punchUDPHole(ctx context.Context, conn *net.UDPConn, raddr net.Addr) error {
	return c.punchHole(ctx, conn, raddr, []byte{})
}

func (c *SenderClient) punchICMPHole(ctx context.Context, conn net.PacketConn, raddr net.Addr, echoID uint16) error {
	echoReq := icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Code: 0,
		Body: &icmp.Echo{
			ID:   int(echoID),
			Seq:  0,
			Data: makeICMPData(echoID, true),
		},
	}
	buf, err := echoReq.Marshal(nil)
	if err != nil {
		return fmt.Errorf("Failed to marshal ICMP message: %w", err)
	}
	return c.punchHole(ctx, conn, raddr, buf)
}

func (c *SenderClient) punchHole(ctx context.Context, conn net.PacketConn, raddr net.Addr, payload []byte) error {
	probeReplyReceived := false
	maxTry := 5
	for try := range maxTry {
		// Send an UDP packet to the newly opened server UDP socket to poke
		// a hole into a potentially existing NAT and wait for the reply.
		if try > 0 {
			slog.DebugContext(ctx, "Sending NAT probe", "try", try+1, "maxTry", maxTry,
				"remoteAddr", raddr)
		}
		if _, err := conn.WriteTo(payload, raddr); err != nil {
			return fmt.Errorf("Failed WriteTo: %w", err)
		}

		probeDeadline := time.Now().Add(time.Second)
		if !c.StartAt.IsZero() && probeDeadline.After(c.StartAt) {
			probeDeadline = c.StartAt
		}
		// TODO: replace this with context deadline?
		if err := conn.SetReadDeadline(probeDeadline); err != nil {
			return fmt.Errorf("Failed to probe deadline: %w", err)
		}

		if !c.StartAt.IsZero() && time.Now().After(c.StartAt) {
			return fmt.Errorf("StartAt %v already passed before received probe reply at %v", c.StartAt, time.Now())
		}

		var buf [1500]byte
		if _, _, err := conn.ReadFrom(buf[:]); err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				return fmt.Errorf("Failed ReadFrom: %w", err)
			}
			// Timeout occured, send another probe
			continue
		}
		slog.DebugContext(ctx, "Received NAT probe", "try", try+1)
		probeReplyReceived = true
		// Clear the ReadDeadline set for probing
		conn.SetReadDeadline(time.Time{})
		break
	}
	if !probeReplyReceived {
		return fmt.Errorf("No probe reply received")
	}
	return nil
}

func (c *SenderClient) Abort(ctx context.Context, client *rpc.Client) error {
	if c.ID == "" {
		// Run never started, so no server-side test was requested
		return nil
	}
	ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))
	var reply AbortReply
	if err := callCtx(ctx, client, "Server.Abort", AbortArgs{ID: c.ID}, &reply); err != nil {
		return fmt.Errorf("Call Server.Abort failed: %w", err)
	}
	return nil
}

func (c *SenderClient) Gather(ctx context.Context, client *rpc.Client) error {
	ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))

	slog.DebugContext(ctx, "Requesting results", "sender", fmt.Sprintf("%T", c.Sender))

	var result RequestServerResultReply
	if err := callCtx(ctx, client, "Server.RequestServerResult",
		RequestServerResultArgs{ID: c.ID}, &result); err != nil {
		return fmt.Errorf("Call Server.RequestServerResult failed: %w", err)
	}
	res := result.Result

	switch res.(type) {
	case []MsgRcvd:
		if c.Direction != UL {
			panic("Unexpected result type")
		}
		c.UDPMsgsRcvd = res.([]MsgRcvd)
	case []MsgSent:
		if c.Direction != DL {
			panic("Unexpected result type")
		}
		c.UDPMsgsSent = res.([]MsgSent)
	case []TCPMetric:
		m := res.([]TCPMetric)
		if c.Direction == UL {
			c.TCPMetricsRcvr = m
		} else {
			c.TCPMetricsSndr = m
		}
	default:
		slog.ErrorContext(ctx, "Unhandled result type in Gather", "result", res)
	}

	slog.DebugContext(ctx, "Received results", "type", fmt.Sprintf("%T", c.Sender))

	switch c.Sender.SenderMode().SocketType() {
	case UDP:
		c.UDPResults = processUDP(c.UDPMsgsSent, c.UDPMsgsRcvd)
		rows := generateUDPResultRows(c.UDPResults)
		if err := writeCSV(c.Out, rows); err != nil {
			return fmt.Errorf("writeCSV failed: %w", err)
		}
	case TCP:
		sndrRows := generateTCPResultRows(c.TCPMetricsSndr)
		// rcvrRows := generateTCPResultRows(c.TCPMetricsRcvr)
		if err := writeCSV(c.Out, sndrRows); err != nil {
			return fmt.Errorf("writeCSV failed: %w", err)
		}
	case ICMP:
		// TODO: change naming or merge strategy?
		c.UDPResults = processUDP(c.UDPMsgsSent, c.UDPMsgsRcvd)
		rows := generateUDPResultRows(c.UDPResults)
		// slog.DebugContext(ctx, "Results", "MSGSSENT", c.UDPMsgsSent, "MSGSRCVD", c.UDPMsgsRcvd) //, "RESULTS", c.UDPResults)
		if err := writeCSV(c.Out, rows); err != nil {
			return fmt.Errorf("writeCSV failed: %w", err)
		}
	default:
		panic("Unknown SocketType")
	}

	return nil
}

func (c *SenderClient) Summary() string {
	var b strings.Builder

	b.WriteString(c.Sender.SenderMode().String())
	b.WriteString("\t")
	b.WriteString(c.Direction.String())
	b.WriteString("\t")
	b.WriteString(fmt.Sprintf("%+v", c.Sender.GetParams()))
	b.WriteString("\t")
	b.WriteString(fmt.Sprintf("%v:%v", c.IP, c.port))
	b.WriteString("\t")
	b.WriteString(c.Out)
	b.WriteString("\t")

	switch c.Sender.SenderMode().SocketType() {
	case UDP:
		c.summaryUDP(&b)
	case TCP:
		c.summaryTCP(&b)
	case ICMP:
		c.summaryUDP(&b)
	default:
		panic("Unknown SocketType")
	}

	return b.String()
}

func (c *SenderClient) summaryUDP(b *strings.Builder) {
	sort.Slice(c.UDPMsgsRcvd, func(i, j int) bool {
		return c.UDPMsgsRcvd[i].Seq < c.UDPMsgsRcvd[j].Seq
	})

	numSent := uint64(len(c.UDPMsgsSent))
	numRcvd := len(c.UDPMsgsRcvd)
	lossPct := 0.0
	if numSent > 0 {
		lossPct = 100.0 - float64(numRcvd)/float64(numSent)*100
	}
	b.WriteString(fmt.Sprintf("%v/%v packets (%.2f%% lost)",
		numRcvd, numSent, lossPct))

	bytesRcvd := uint(0)
	for i := range c.UDPMsgsRcvd {
		bytesRcvd += c.UDPMsgsRcvd[i].Len
	}
	avgGoodput := 0.0
	if len(c.UDPMsgsRcvd) > 0 {
		avgGoodput = float64(bytesRcvd) / c.UDPMsgsRcvd[len(c.UDPMsgsRcvd)-1].TsRcvd.Sub(c.UDPMsgsRcvd[0].TsRcvd).Seconds() * 8 / 1e6
	}
	b.WriteString(fmt.Sprintf("\t%.2f Mbps", avgGoodput))

	if !c.StartAt.IsZero() {
		b.WriteString(fmt.Sprintf("\t%v", c.StartAt))
	}
}

func (c *SenderClient) summaryTCP(b *strings.Builder) {
}

type CommandClient struct {
	Params   CommandParams
	Local    bool
	StartAt  time.Time
	LocalDir string

	ID      string
	tempDir string
}

func (c *CommandClient) Run(ctx context.Context, client *rpc.Client) error {
	if c.ID == "" {
		c.ID = GenID(c.Params.Name())
		ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))
	}

	if c.Local {
		var command Command
		switch c.Params.(type) {
		case TcpdumpParams:
			command = &TcpdumpCommand{Params_: c.Params.(TcpdumpParams)}
		default:
			return fmt.Errorf("Unknown params %v", c.Params)
		}

		var err error
		c.tempDir, err = RandDir(c.Params.Name())
		if err != nil {
			return fmt.Errorf("RunCommand: failed RandDir: %w", err)
		}

		if err := waitUntil(ctx, c.StartAt); err != nil {
			return err
		}

		slog.DebugContext(ctx, "Writing results", "name", c.Params.Name(), "tempDir", c.tempDir)

		cmd, err := command.Exec(c.tempDir)
		if err != nil {
			return fmt.Errorf("RunCommand: failed command.Cmd: %w", err)
		}

		if err := MonitorCommand(ctx, cmd, c.Params.Timeout()); err != nil {
			return fmt.Errorf("RunCommand: %w", err)
		}
	} else {
		args := RunCommandArgs{ID: c.ID, Params: c.Params, StartAt: c.StartAt}
		var reply RunCommandReply
		if err := callCtx(ctx, client, "Server.RunCommand", args, &reply); err != nil {
			return fmt.Errorf("Call Server.RunCommand failed: %w", err)
		}
	}
	return nil
}

func (c *CommandClient) Abort(ctx context.Context, client *rpc.Client) error {
	if c.Local || c.ID == "" {
		// A local command is stopped by the round context itself
		// (MonitorCommand observes it); nothing to abort on the server.
		return nil
	}
	ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))
	var reply AbortReply
	if err := callCtx(ctx, client, "Server.Abort", AbortArgs{ID: c.ID}, &reply); err != nil {
		return fmt.Errorf("Call Server.Abort failed: %w", err)
	}
	return nil
}

func (c *CommandClient) Gather(ctx context.Context, client *rpc.Client) error {
	ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))

	if c.Local {
		entries, err := os.ReadDir(c.tempDir)
		if err != nil {
			return fmt.Errorf("Failed os.ReadDir(%v): %w", c.tempDir, err)
		}

		for _, entry := range entries {
			path := filepath.Join(c.tempDir, entry.Name())

			if entry.IsDir() {
				slog.DebugContext(ctx, "Skipping directory", "path", path)
				continue
			}

			encPath := filepath.Join(c.LocalDir, fmt.Sprintf("%s.zst", entry.Name()))
			f, err := os.Create(encPath)
			if err != nil {
				return fmt.Errorf("Failed os.Create(%v): %w", encPath, err)
			}

			fW := bufio.NewWriter(f)
			if err := CompressFile(path, fW); err != nil {
				return fmt.Errorf("Failed compression: %w", err)
			}
			fW.Flush()

			if err := f.Close(); err != nil {
				return fmt.Errorf("Failed closing compressed file %v: %w", encPath, err)
			}

			if err := os.Remove(path); err != nil {
				slog.WarnContext(ctx, "Failed to remove file after reading", "path", path)
			}
		}

		if err := os.Remove(c.tempDir); err != nil {
			slog.WarnContext(ctx, "Failed to remove result directory", "path", c.tempDir)
		}
	} else {
		slog.DebugContext(ctx, "Requesting results", "name", fmt.Sprintf("%T", c.Params))

		var result RequestRunCommandResultReply
		if err := callCtx(ctx, client, "Server.RequestRunCommandResult", RequestRunCommandResultArgs{ID: c.ID}, &result); err != nil {
			return fmt.Errorf("Call Server.RunCommand failed: %w", err)
		}

		slog.DebugContext(ctx, "Received results", "name", fmt.Sprintf("%T", c.Params))

		for filename, bufEnc := range result.Files {
			path := filepath.Join(c.LocalDir, fmt.Sprintf("%s.zst", filename))
			if err := os.WriteFile(path, bufEnc, 0644); err != nil {
				return fmt.Errorf("Failed writing returned file to %v: %w", path, err)
			}
		}
	}
	return nil
}

func (c *CommandClient) Summary() string {
	return ""
}

// First try setting up a datagram socket for ICMP packets. This works
// without root and without additional capabilities if sysctl
// net.ipv4.ping_group_range != '1 0' (and if it includes the user's
// group ID). sudo and capabilities don't help if the sysctl setting is
// wrong.
//
// If this fails, we try setting up an ipv4 ICMP socket. This works if
// the user is root or if the binary is run with the CAP_NET_RAW capability.
func listenICMPClient(ctx context.Context, ip string) (conn net.Conn, raddr net.Addr, err error) {
	// ICMP PacketConn WriteTo expects an udp address
	if raddr, err = net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%v", ip, "0")); err != nil {
		err = fmt.Errorf("Failed resolving provided addr for ICMP: %w", err)
		return
	}
	var icmpConn *icmp.PacketConn
	if icmpConn, err = icmp.ListenPacket("udp4", "0.0.0.0"); err != nil {
		slog.DebugContext(ctx, "icmp.ListenPacket failed to create dgram ICMP connecti", "error", err)

		if raddr, err = net.ResolveIPAddr("ip4", ip); err != nil {
			err = fmt.Errorf("Failed resolving ip4 addr for ICMP: %w", err)
			return
		}
		var laddr *net.IPAddr
		laddr, err = net.ResolveIPAddr("ip4", "0.0.0.0")
		if err != nil {
			err = fmt.Errorf("Failed resolving catch-all ip4 addr for ICMP: %w", err)
			return
		}
		if conn, err = net.ListenIP("ip4:icmp", laddr); err != nil {
			err = fmt.Errorf("net.ListenIP failed to create raw ICMP connection: %w. Configure net.ipv4.ping_group_range != '1 0', run as root, or with CAP_NET_RAW capability.", err)
			return
		}
	} else {
		conn = &ICMPMockConn{conn: icmpConn}
	}
	return
}
