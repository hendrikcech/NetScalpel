package pkg

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

func logErrContext(ctx context.Context, fmtStr string, args ...interface{}) error {
	e := fmt.Errorf(fmtStr, args...)
	slog.ErrorContext(ctx, "", "error", e)
	return e
}

func RunServer(ctx context.Context, ip string, port uint, slogCh *chan *slog.Record) *Server {
	s := NewServer(ctx, slogCh)

	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		slog.ErrorContext(ctx, "Failed TCP listen", "error", err)
		os.Exit(1)
	}
	slog.InfoContext(ctx, "Listening on TCP for RPC calls", "addr", s.listener.Addr())
	s.wg.Add(1)
	// listener.Close() called in Stop()

	handler := rpc.NewServer()
	if err := handler.Register(s); err != nil {
		slog.ErrorContext(ctx, "Failed rpc.Register", "error", err.Error())
		os.Exit(1)
	}

	// Accept connections and serve them in separate goroutines.
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				if !strings.HasSuffix(err.Error(), "use of closed network connection") {
					slog.ErrorContext(ctx, "Failed listener.Accept()", "error", err)
				}
				return
			}
			s.wg.Add(1)
			slog.InfoContext(ctx, "New RPC client", "remoteAddr", conn.RemoteAddr())
			go func() {
				handler.ServeConn(conn)
				slog.InfoContext(ctx, "RPC client disconnected", "remoteAddr", conn.RemoteAddr())
				s.wg.Done()
			}()
		}
	}()

	return s
}

type Result struct {
	Err error
	Res any
}

type ResultPath struct {
	Path string
	Err  error
}

type Server struct {
	listener net.Listener
	wg       sync.WaitGroup
	ctx      context.Context
	slogCh   *chan *slog.Record

	resultC    map[string]chan *Result
	resultLock sync.RWMutex
}

func NewServer(ctx context.Context, slogCh *chan *slog.Record) *Server {
	return &Server{
		ctx:     ctx,
		slogCh:  slogCh,
		resultC: make(map[string]chan *Result),
	}
}

// TODO: replace with context.Cancel?
func (s *Server) Stop() {
	s.listener.Close()
	s.wg.Wait()
}

type RequestServerArgs struct {
	ID string

	// Server stops reading from socket after Timeout
	Timeout time.Duration

	StartAt time.Time

	ServerMode Mode

	Params SenderParams
}
type RequestServerReply struct {
	Port uint
}

func (s *Server) RequestServer(args RequestServerArgs, reply *RequestServerReply) error {
	ctx := context.WithValue(s.ctx, SlogIDKey{}, slog.Any("id", args.ID))
	if args.ID == "" {
		return logErrContext(ctx, "RequestServer: ID is empty")
	}

	s.resultLock.Lock()
	if _, ok := s.resultC[args.ID]; ok {
		return logErrContext(ctx, "ID already used")
	}
	s.resultC[args.ID] = make(chan *Result, 1)
	s.resultLock.Unlock()

	var laddr net.Addr
	switch args.ServerMode.SocketType() {
	case UDP:
		conn, err := listenUDP(ctx)
		if err != nil {
			return logErrContext(ctx, "listenUDP failed: %v", err.Error())
		}
		laddr = conn.LocalAddr()
		reply.Port = uint(laddr.(*net.UDPAddr).Port)
		go s.handleRequestServerUDP(ctx, conn, args)
	case TCP:
		ln, err := listenTCP(ctx)
		if err != nil {
			return logErrContext(ctx, "listenTCP failed: %v", err.Error())
		}
		laddr = ln.Addr()
		reply.Port = uint(laddr.(*net.TCPAddr).Port)
		go s.handleRequestServerTCP(ctx, ln, args)
	case ICMP:
		conn, err := listenICMP(ctx)
		if err != nil {
			return logErrContext(ctx, "listenICMP failed: %v", err.Error())
		}
		go s.handleRequestServerICMP(ctx, conn, args)

	default:
		panic("socket type not implemented")
	}

	slog.InfoContext(ctx, "RequestServer", "args", args, "reply", reply, "localAddr", laddr)

	// Note: returns no error even if requested ServerMode is invalid

	return nil
}

func (s *Server) handleRequestServerUDP(ctx context.Context, conn *net.UDPConn, args RequestServerArgs) {
	defer conn.Close()

	var result Result

	defer func() {
		s.resultLock.Lock()
		s.resultC[args.ID] <- &result
		close(s.resultC[args.ID])
		s.resultLock.Unlock()
	}()

	var sender Sender
	var receiver Receiver
	switch args.ServerMode {
	case SendBurst:
		sender = &BurstSender{Params: args.Params.(BurstParams)}
	case SendRate:
		sender = &RateSender{Params: args.Params.(RateParamsW)}
	case SendPeriodic:
		sender = &PeriodicSender{Params: args.Params.(PeriodicParams)}
	case SendQUIC:
		sender = &QUICSender{Params: args.Params.(QUICParams)}
	case ReceiveUDP:
		receiver = &UDPReceiver{}
	case ReceiveQUIC:
		receiver = &QUICReceiver{}
	default:
		slog.ErrorContext(ctx, "RequestServer: unknown mode", "mode", args.ServerMode)
		result.Err = fmt.Errorf("RequestServer: unknown mode %v", args.ServerMode)
		return
	}

	if sender != nil {
		raddr, err := waitForUDPProbe(ctx, conn)
		if err != nil {
			slog.ErrorContext(ctx, "RequestServer: failed waiting for probe:", "error", err)
			result.Err = fmt.Errorf("RequestServer: failed waiting for probe: %v", err)
			return
		}
		result.Res, result.Err = handleSender(ctx, conn, args, sender, raddr)
	} else {
		ln := NewDummyListener(conn, conn.LocalAddr())
		result.Res, result.Err = handleReceiver(ctx, ln, args, receiver)
	}
}

func (s *Server) handleRequestServerTCP(ctx context.Context, ln *net.TCPListener, args RequestServerArgs) {
	defer ln.Close()

	var result Result

	defer func() {
		s.resultLock.Lock()
		s.resultC[args.ID] <- &result
		close(s.resultC[args.ID])
		s.resultLock.Unlock()
	}()

	switch args.ServerMode {
	case ReceiveTCP:
		receiver := &TCPReceiver{}
		result.Res, result.Err = handleReceiver(ctx, ln, args, receiver)
	case SendTCP:
		sender := &TCPSender{Params: args.Params.(TCPSenderParams)}
		conn, err := ln.AcceptTCP()
		if err != nil {
			slog.ErrorContext(ctx, "RequestServerTCP: failed AcceptTCP", "error", err)
			result.Err = fmt.Errorf("Failed AcceptTCP: %v", err.Error())
			return
		}
		defer conn.Close()
		result.Res, result.Err = handleSender(ctx, conn, args, sender, conn.RemoteAddr())
		if result.Err != nil {
			slog.ErrorContext(ctx, "handleSender failed", "error", err)
		}
	default:
		slog.ErrorContext(ctx, "RequestServerTCP: unknown mode", "mode", args.ServerMode)
		result.Err = fmt.Errorf("RequestServerTCP: unknown mode %v", args.ServerMode)
		return
	}
}

func (s *Server) handleRequestServerICMP(ctx context.Context, conn *net.IPConn, args RequestServerArgs) {
	defer conn.Close()

	var result Result

	defer func() {
		s.resultLock.Lock()
		s.resultC[args.ID] <- &result
		close(s.resultC[args.ID])
		s.resultLock.Unlock()
	}()

	switch args.ServerMode {
	case SendICMP:
		raddr, echoID, err := waitForICMPProbe(ctx, conn, args.Params.(ICMPParams).ClientEchoID)
		if err != nil {
			slog.ErrorContext(ctx, "RequestServer: failed waiting for probe:", "error", err)
			result.Err = fmt.Errorf("RequestServer: failed waiting for probe: %v", err)
			return
		}
		icmpParams, ok := args.Params.(ICMPParams)
		if !ok {
			result.Err = fmt.Errorf("RequestServer: wrong params for ICMP")
			return
		}
		icmpParams.SenderEchoID = echoID
		slog.DebugContext(ctx, "Staring ICMPSender", "params", icmpParams)
		sender := &ICMPSender{
			Params: icmpParams,
		}
		result.Res, result.Err = handleSender(ctx, conn, args, sender, raddr)
	case ReceiveICMP:
		receiver := &ICMPReceiver{
			ClientEchoID: args.Params.(ICMPParams).ClientEchoID,
			ICMPType:     ipv4.ICMPTypeEcho,
		}
		ln := NewDummyListener(conn, conn.LocalAddr())
		result.Res, result.Err = handleReceiver(ctx, ln, args, receiver)
	default:
		slog.ErrorContext(ctx, "RequestServer: unknown mode", "mode", args.ServerMode)
		result.Err = fmt.Errorf("RequestServer: unknown mode %v", args.ServerMode)
		return
	}
}

func handleSender(ctx context.Context, conn net.Conn, args RequestServerArgs, sender Sender, raddr net.Addr) (any, error) {
	if err := waitUntil(ctx, args.StartAt); err != nil {
		return nil, err
	}

	// TODO: is this working as expected?
	deadline := args.Params.GetDuration() * 2
	minDeadline := 60 * time.Second
	if deadline < 60*time.Second {
		deadline = minDeadline
	}
	if err := conn.SetDeadline(time.Now().Add(minDeadline)); err != nil {
		return nil, err
	}

	sendCtx, sendCancel := context.WithTimeout(ctx, args.Params.GetDuration())
	defer sendCancel()
	res, err := sender.Run(sendCtx, conn, raddr)
	slog.DebugContext(ctx, "Finished handleSender", "remoteAddr", raddr)
	return res, err
}

// TODO: make function generic over conn/listener and receiverUDP/receiverTCP?
func handleReceiver(ctx context.Context, ln net.Listener, args RequestServerArgs, receiver Receiver) (any, error) {
	receiver.Init()

	if err := waitUntil(ctx, args.StartAt); err != nil {
		return nil, err
	}

	recvCtx, recvCancel := context.WithTimeout(ctx, args.Timeout)
	defer recvCancel()
	res, err := receiver.Run(recvCtx, ln)
	slog.DebugContext(ctx, "Finished handleReceiver")
	// , "packets", len(result.Res)
	return res, err
}

func waitForUDPProbe(ctx context.Context, conn net.PacketConn) (net.Addr, error) {
	// Wait for single UDP packet from receiving client UDP socket that opens
	// the client NAT.
	var buf [1500]byte
	_, raddr, err := conn.ReadFrom(buf[0:])
	if err != nil {
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			// not a timeout
			return nil, fmt.Errorf("waitForUDPProbe ReadFrom: %v", err.Error())
		}
		return nil, nil // TODO: return error?
	}
	if _, err := conn.WriteTo([]byte{}, raddr); err != nil {
		return nil, fmt.Errorf("handleSender: WriteTo: %v", err.Error())
	}
	slog.DebugContext(ctx, "Received kick-off msg", "remoteAddr", raddr)
	return raddr, nil
}

// Wait for ICMP echo request from receiving client
// Only terminates on conn deadline, i.e., after 10 seconds
func waitForICMPProbe(ctx context.Context, conn net.PacketConn, echoID uint16) (net.Addr, uint16, error) {
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return nil, 0, err
	}

	var buf [1500]byte
	for {
		n, raddr, err := conn.ReadFrom(buf[0:])
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Timeout() {
				return nil, 0, fmt.Errorf("Did not receive ICMP probe in time: %v", err.Error())
			}
			return nil, 0, fmt.Errorf("waitForICMPProbe ReadFrom error: %v", err.Error())
		}

		msg, err := icmp.ParseMessage(1, buf[:n])
		if err != nil {
			slog.WarnContext(ctx, "Failed to parse ICMP message", "error", err)
			continue
		}
		if msg.Type != ipv4.ICMPTypeEcho {
			slog.WarnContext(ctx, "Unexpected ICMP probe type", "type", msg.Type)
			continue
		}

		body, ok := msg.Body.(*icmp.Echo)
		if !ok {
			slog.WarnContext(ctx, "Unexpected msg body")
			continue
		}
		echoIDData, punch, err := parseICMPData(body.Data)
		if err != nil {
			slog.WarnContext(ctx, "Failed parsing ICMP body", "error", err)
			continue
		}

		if !punch {
			slog.WarnContext(ctx, "Expected punch packet", "msg", msg, "body", body)
			continue
		}

		if echoIDData != echoID {
			slog.WarnContext(ctx, "Unexpected ICMP echoID in ICMP data", "expID", echoID, "data", body.Data)
			continue
		}

		natEchoID := body.ID
		slog.DebugContext(ctx, "Received ICMP echo request probe", "remoteAddr", raddr, "echoID", natEchoID)
		return raddr, uint16(natEchoID), nil
	}
}

type RequestServerResultArgs struct {
	ID string
}
type RequestServerResultReply struct {
	Result any
}

func (s *Server) RequestServerResult(args RequestServerResultArgs, reply *RequestServerResultReply) error {
	ctx := context.WithValue(s.ctx, SlogIDKey{}, slog.Any("id", args.ID))
	slog.DebugContext(ctx, "RequestServerResult: request")

	s.resultLock.Lock()
	c, ok := s.resultC[args.ID]
	s.resultLock.Unlock()

	if !ok {
		return logErrContext(ctx, "No test with id %v started\n", args.ID)
	}

	if err := handleChanResult(ctx, c, args.ID, reply); err != nil {
		return logErrContext(ctx, "Receive from resultC: %v", err)
	}

	slog.DebugContext(ctx, "RequestServerResult: responded")

	return nil
}

// TODO: remove generics
func handleChanResult(ctx context.Context, c chan *Result, id string, reply *RequestServerResultReply) error {
	var (
		result *Result
		closed bool
	)
	slog.DebugContext(ctx, "Waiting on chan result")
	select {
	case result, closed = <-c:
		slog.DebugContext(ctx, "Received on chan result")
	case <-ctx.Done():
		slog.DebugContext(ctx, "ctx.Done() while waiting on chan result")
		return ctx.Err()
	}
	if closed && result == nil {
		return fmt.Errorf("Result %v was already retrieved", id)
	}
	if result == nil {
		panic("result.Res == nil")
	}
	if result.Err != nil {
		return fmt.Errorf("handleChanResult: %v", result.Err.Error())
	}
	reply.Result = result.Res
	return nil
}

type RunCommandArgs struct {
	// Mode RunCommandMode
	ID string

	StartAt time.Time

	Params CommandParams
}

type RunCommandReply struct {
	// Id uuid.UUID
	// Port uint
}

func (s *Server) RunCommand(args RunCommandArgs, reply *RunCommandReply) error {
	ctx := context.WithValue(s.ctx, SlogIDKey{}, slog.Any("id", args.ID))

	if args.ID == "" {
		return logErrContext(ctx, "RunCommand: ID is empty")
	}

	c := make(chan *Result, 1)

	s.resultLock.Lock()
	if _, ok := s.resultC[args.ID]; ok {
		return logErrContext(ctx, "ID already used")
	}
	s.resultC[args.ID] = c
	s.resultLock.Unlock()

	var command Command
	switch args.Params.(type) {
	case TcpdumpParams:
		command = &TcpdumpCommand{Params_: args.Params.(TcpdumpParams)}
	default:
		c <- &Result{Err: fmt.Errorf("Unknown params %v", args.Params)}
		return fmt.Errorf("Unknown params %v", args.Params)
	}

	resultDir, err := RandDir(args.Params.Name())
	if err != nil {
		c <- &Result{Err: fmt.Errorf("RunCommand: failed RandDir: %v", err.Error())}
		return logErrContext(ctx, "RunCommand: failed RandDir: %v", err.Error())
	}

	slog.DebugContext(ctx, "RunCommand", "name", args.Params.Name(), "resultDir", resultDir)

	go func() {
		if err := waitUntil(ctx, args.StartAt); err != nil {
			slog.WarnContext(ctx, "WaitUntil failed", "error", err)
			c <- &Result{Err: fmt.Errorf("WaitUntil failed: %v", err.Error())}
			return
		}

		cmd, err := command.Exec(resultDir)
		if err != nil {
			c <- &Result{Err: fmt.Errorf("RunCommand: command.Exec: %v", err.Error())}
			return
		}

		if err := MonitorCommand(ctx, cmd, args.Params.Timeout()); err != nil {
			c <- &Result{Err: fmt.Errorf("RunCommand: %v", err.Error())}
			return
		}

		c <- &Result{Res: resultDir}
	}()

	return nil
}

type RequestRunCommandResultArgs struct {
	ID string
}
type RequestRunCommandResultReply struct {
	Files map[string][]byte
}

func (s *Server) RequestRunCommandResult(args RequestRunCommandResultArgs, reply *RequestRunCommandResultReply) error {
	ctx := context.WithValue(s.ctx, SlogIDKey{}, slog.Any("id", args.ID))

	s.resultLock.Lock()
	c, ok := s.resultC[args.ID]
	s.resultLock.Unlock()

	if !ok {
		return logErrContext(ctx, "No command with that ID started")
	}

	result := <-c
	if result.Err != nil {
		return logErrContext(ctx, "RequestRunCommandResult returning error: %v", result.Err.Error())
	}

	resultPath := result.Res.(string)

	entries, err := os.ReadDir(resultPath)
	if err != nil {
		return logErrContext(ctx, "Failed os.ReadDir(%v): %v", resultPath, err)
	}

	reply.Files = make(map[string][]byte, len(entries))

	for _, entry := range entries {
		path := filepath.Join(resultPath, entry.Name())

		if entry.IsDir() {
			return logErrContext(ctx, "Skipping directory %v", path)
		}

		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		if err := CompressFile(path, w); err != nil {
			return logErrContext(ctx, "Failed compression: %v", err.Error())
		}
		w.Flush()

		reply.Files[entry.Name()] = buf.Bytes()
		if err := os.Remove(path); err != nil {
			slog.WarnContext(ctx, "Failed to remove path after reading", "path", path)
		}
	}

	if err := os.Remove(resultPath); err != nil {
		slog.WarnContext(ctx, "Failed to remove result directory", "path", resultPath)
	}

	return nil
}

type RequestSlogArgs struct {
}
type RequestSlogReply struct {
	Log string
}

func (s *Server) RequestSlog(args RequestSlogArgs, reply *RequestSlogReply) error {
	var buf bytes.Buffer
	handler := slog.NewTextHandler(&buf, nil)

	if s.slogCh == nil {
		return nil
	}

	for {
		select {
		case r := <-*s.slogCh:
			if err := handler.Handle(context.Background(), *r); err != nil {
				return err
			}
		default:
			reply.Log = buf.String()
			return nil
		}
	}
}
