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
			s.connsLock.Lock()
			s.conns[conn] = struct{}{}
			s.connsLock.Unlock()
			slog.InfoContext(ctx, "New RPC client", "remoteAddr", conn.RemoteAddr())
			go func() {
				handler.ServeConn(conn)
				slog.InfoContext(ctx, "RPC client disconnected", "remoteAddr", conn.RemoteAddr())
				s.connsLock.Lock()
				delete(s.conns, conn)
				s.connsLock.Unlock()
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

	conns     map[net.Conn]struct{}
	connsLock sync.Mutex

	resultC map[string]chan *Result
	// Cancel functions of the per-test contexts, keyed by test ID. Called by
	// the Abort RPC so a client that gives up on a test can end the
	// server-side goroutines early. Entries are
	// removed by finishTest once the test delivers its result.
	testCancel map[string]context.CancelFunc
	resultLock sync.RWMutex
}

func NewServer(ctx context.Context, slogCh *chan *slog.Record) *Server {
	return &Server{
		ctx:        ctx,
		slogCh:     slogCh,
		conns:      make(map[net.Conn]struct{}),
		resultC:    make(map[string]chan *Result),
		testCancel: make(map[string]context.CancelFunc),
	}
}

// Stop closes the RPC listener and all active RPC connections, then waits
// for their handlers to finish. Without closing the connections, Stop would
// block until every client disconnects on its own.
// The caller should cancel the server context first so that RPC handlers
// blocked on pending results return.
func (s *Server) Stop() {
	s.listener.Close()
	s.connsLock.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.connsLock.Unlock()
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

	// Everything the test does runs on a per-test child context so that the
	// Abort RPC can end this one test without touching the rest of the server.
	testCtx, testCancel := context.WithCancel(ctx)

	s.resultLock.Lock()
	if _, ok := s.resultC[args.ID]; ok {
		s.resultLock.Unlock()
		testCancel()
		return logErrContext(ctx, "ID already used")
	}
	s.resultC[args.ID] = make(chan *Result, 1)
	s.testCancel[args.ID] = testCancel
	s.resultLock.Unlock()

	var laddr net.Addr
	switch args.ServerMode.SocketType() {
	case UDP:
		conn, err := listenUDP(ctx)
		if err != nil {
			return s.failTest(ctx, args.ID, "listenUDP failed: %v", err.Error())
		}
		laddr = conn.LocalAddr()
		reply.Port = uint(laddr.(*net.UDPAddr).Port)
		go s.handleRequestServerUDP(testCtx, conn, args)
	case TCP:
		ln, err := listenTCP(ctx)
		if err != nil {
			return s.failTest(ctx, args.ID, "listenTCP failed: %v", err.Error())
		}
		laddr = ln.Addr()
		reply.Port = uint(laddr.(*net.TCPAddr).Port)
		go s.handleRequestServerTCP(testCtx, ln, args)
	case ICMP:
		conn, err := listenICMP(ctx)
		if err != nil {
			return s.failTest(ctx, args.ID, "listenICMP failed: %v", err.Error())
		}
		go s.handleRequestServerICMP(testCtx, conn, args)

	default:
		panic("socket type not implemented")
	}

	slog.InfoContext(ctx, "RequestServer", "args", args, "reply", reply, "localAddr", laddr)

	// Note: returns no error even if requested ServerMode is invalid

	return nil
}

// failTest delivers the error as the test's result before releasing the
// per-test context. Without a result in resultC, a later RequestServerResult
// for this ID would block forever and leave the client stuck at gathering.
func (s *Server) failTest(ctx context.Context, id string, fmtStr string, args ...interface{}) error {
	err := logErrContext(ctx, fmtStr, args...)

	s.resultLock.RLock()
	c := s.resultC[id]
	s.resultLock.RUnlock()
	c <- &Result{Err: err}
	close(c)

	s.finishTest(id)
	return err
}

// finishTest releases the per-test context registered in RequestServer or
// RunCommand; afterwards Abort calls for this ID are a no-op.
func (s *Server) finishTest(id string) {
	s.resultLock.Lock()
	cancel := s.testCancel[id]
	delete(s.testCancel, id)
	s.resultLock.Unlock()
	if cancel != nil {
		cancel()
	}
}

type AbortArgs struct {
	ID string
}
type AbortReply struct {
}

// Abort cancels the per-test context of a running test or command so the
// server-side goroutines end promptly once the client has given up on the
// test, instead of running to their natural end.
// Aborting an unknown or already finished ID is a no-op: on user abort the
// client fires Abort for every test it scheduled.
func (s *Server) Abort(args AbortArgs, reply *AbortReply) error {
	ctx := context.WithValue(s.ctx, SlogIDKey{}, slog.Any("id", args.ID))

	s.resultLock.Lock()
	cancel, ok := s.testCancel[args.ID]
	s.resultLock.Unlock()

	if !ok {
		slog.DebugContext(ctx, "Abort: no running test with this ID")
		return nil
	}

	slog.InfoContext(ctx, "Aborting test on client request")
	cancel()
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
		s.finishTest(args.ID)
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
		raddr, err := waitForUDPProbe(ctx, conn, args.StartAt)
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
	defer func() {
		slog.DebugContext(ctx, "Closing TCP listener")
		ln.Close()
	}()

	var result Result

	defer func() {
		s.resultLock.Lock()
		s.resultC[args.ID] <- &result
		close(s.resultC[args.ID])
		s.resultLock.Unlock()
		s.finishTest(args.ID)
	}()

	switch args.ServerMode {
	case ReceiveTCP:
		receiver := &TCPReceiver{}
		result.Res, result.Err = handleReceiver(ctx, ln, args, receiver)
	case SendTCP:
		sender := &TCPSender{Params: args.Params.(TCPSenderParams)}
		// Bound the accept: if the client has not connected by the
		// first-contact deadline, it never will.
		if err := ln.SetDeadline(firstContactDeadline(args.StartAt)); err != nil {
			result.Err = fmt.Errorf("Failed to set accept deadline: %v", err.Error())
			return
		}
		stop := context.AfterFunc(ctx, func() { ln.SetDeadline(time.Now()) })
		conn, err := ln.AcceptTCP()
		stop()
		if err != nil {
			slog.ErrorContext(ctx, "RequestServerTCP: failed AcceptTCP", "error", err)
			result.Err = fmt.Errorf("Failed AcceptTCP: %v", err.Error())
			return
		}
		defer func() {
			slog.DebugContext(ctx, "Closing TCP connection")
			conn.Close()
		}()
		result.Res, result.Err = handleSender(ctx, conn, args, sender, conn.RemoteAddr())
		if result.Err != nil {
			slog.ErrorContext(ctx, "handleSender failed", "error", result.Err)
		}
	default:
		slog.ErrorContext(ctx, "RequestServerTCP: unknown mode", "mode", args.ServerMode)
		result.Err = fmt.Errorf("RequestServerTCP: unknown mode %v", args.ServerMode)
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
		s.finishTest(args.ID)
	}()

	switch args.ServerMode {
	case SendICMP:
		raddr, echoID, err := waitForICMPProbe(ctx, conn, args.Params.(ICMPParams).ClientEchoID, args.StartAt)
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
		args.Params = icmpParams
		sender := &ICMPSender{
			Params: args.Params.(ICMPParams),
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

	// Safety net for blocked I/O: generous socket deadline covering the
	// whole test (a fixed 60s deadline was applied before, breaking
	// server-side send tests longer than 60s)
	deadline := args.Params.GetDuration() * 2
	if deadline < 60*time.Second {
		deadline = 60 * time.Second
	}
	if err := conn.SetDeadline(time.Now().Add(deadline)); err != nil {
		return nil, err
	}

	sendCtx, sendCancel := context.WithTimeout(ctx, args.Params.GetDuration())
	defer sendCancel()
	slog.DebugContext(ctx, "Calling sender.Run")
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

// The client contacts the server (NAT probe or TCP connect) right after the
// RequestServer RPC returns, and it gives up before StartAt. Waiting past
// this deadline means the client is gone; the test goroutine must give up
// and deliver an error result instead of blocking forever, which would
// in turn deadlock the client's result-gathering RPC.
func firstContactDeadline(startAt time.Time) time.Time {
	if startAt.IsZero() {
		return time.Now().Add(10 * time.Second)
	}
	deadline := startAt.Add(time.Second)
	if min := time.Now().Add(2 * time.Second); deadline.Before(min) {
		deadline = min
	}
	return deadline
}

func waitForUDPProbe(ctx context.Context, conn net.PacketConn, startAt time.Time) (net.Addr, error) {
	// Wait for single UDP packet from receiving client UDP socket that opens
	// the client NAT.
	deadline := firstContactDeadline(startAt)
	if err := conn.SetReadDeadline(deadline); err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { conn.SetReadDeadline(time.Now()) })
	defer stop()

	var buf [1500]byte
	_, raddr, err := conn.ReadFrom(buf[0:])
	if err != nil {
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			// not a timeout
			return nil, fmt.Errorf("waitForUDPProbe ReadFrom: %v", err.Error())
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("waitForUDPProbe: cancelled: %v", ctx.Err())
		}
		return nil, fmt.Errorf("waitForUDPProbe: no probe received before %v", deadline)
	}
	if _, err := conn.WriteTo([]byte{}, raddr); err != nil {
		return nil, fmt.Errorf("handleSender: WriteTo: %v", err.Error())
	}
	slog.DebugContext(ctx, "Received kick-off msg", "remoteAddr", raddr)
	return raddr, nil
}

// Wait for ICMP echo request from receiving client.
// Terminates on the first-contact deadline or on ctx cancellation.
func waitForICMPProbe(ctx context.Context, conn net.PacketConn, echoID uint16, startAt time.Time) (net.Addr, uint16, error) {
	if err := conn.SetReadDeadline(firstContactDeadline(startAt)); err != nil {
		return nil, 0, err
	}
	stop := context.AfterFunc(ctx, func() { conn.SetReadDeadline(time.Now()) })
	defer stop()

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
			// slog.DebugContext(ctx, "Unexpected ICMP echoID in ICMP data", "expID", echoID, "data", body.Data)
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
		return logErrContext(ctx, "No test with id %v: never started or result already retrieved", args.ID)
	}

	received, err := handleChanResult(ctx, c, args.ID, reply)
	if received {
		// The server runs indefinitely (--rounds 0); drop the entry so the
		// map does not grow by one test forever.
		s.resultLock.Lock()
		delete(s.resultC, args.ID)
		s.resultLock.Unlock()
	}
	if err != nil {
		return logErrContext(ctx, "Receive from resultC: %v", err)
	}

	slog.DebugContext(ctx, "RequestServerResult: responded")

	return nil
}

// The first return value reports whether the test delivered its result (or
// the channel was found closed), i.e. whether the caller can release the
// per-test state; it is false only when ctx ended before a result arrived.
func handleChanResult(ctx context.Context, c chan *Result, id string, reply *RequestServerResultReply) (bool, error) {
	var (
		result *Result
		ok     bool
	)
	slog.DebugContext(ctx, "Waiting on chan result")
	select {
	case result, ok = <-c:
		slog.DebugContext(ctx, "Received on chan result")
	case <-ctx.Done():
		slog.DebugContext(ctx, "ctx.Done() while waiting on chan result")
		return false, ctx.Err()
	}
	if !ok {
		return true, fmt.Errorf("Result %v was already retrieved", id)
	}
	if result.Err != nil {
		return true, fmt.Errorf("handleChanResult: %v", result.Err.Error())
	}
	reply.Result = result.Res
	return true, nil
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

	// Like in RequestServer: a per-test context so the Abort RPC can stop the
	// command (MonitorCommand kills it once its ctx ends).
	testCtx, testCancel := context.WithCancel(ctx)

	s.resultLock.Lock()
	if _, ok := s.resultC[args.ID]; ok {
		s.resultLock.Unlock()
		testCancel()
		return logErrContext(ctx, "ID already used")
	}
	s.resultC[args.ID] = c
	s.testCancel[args.ID] = testCancel
	s.resultLock.Unlock()

	var command Command
	switch args.Params.(type) {
	case TcpdumpParams:
		command = &TcpdumpCommand{Params_: args.Params.(TcpdumpParams)}
	default:
		s.finishTest(args.ID)
		c <- &Result{Err: fmt.Errorf("Unknown params %v", args.Params)}
		return fmt.Errorf("Unknown params %v", args.Params)
	}

	resultDir, err := RandDir(args.Params.Name())
	if err != nil {
		s.finishTest(args.ID)
		c <- &Result{Err: fmt.Errorf("RunCommand: failed RandDir: %v", err.Error())}
		return logErrContext(ctx, "RunCommand: failed RandDir: %v", err.Error())
	}

	slog.DebugContext(ctx, "RunCommand", "name", args.Params.Name(), "resultDir", resultDir)

	go func() {
		defer s.finishTest(args.ID)

		if err := waitUntil(testCtx, args.StartAt); err != nil {
			slog.WarnContext(ctx, "WaitUntil failed", "error", err)
			c <- &Result{Err: fmt.Errorf("WaitUntil failed: %v", err.Error())}
			return
		}

		cmd, err := command.Exec(resultDir)
		if err != nil {
			c <- &Result{Err: fmt.Errorf("RunCommand: command.Exec: %v", err.Error())}
			return
		}

		if err := MonitorCommand(testCtx, cmd, args.Params.Timeout()); err != nil {
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
		return logErrContext(ctx, "No command with id %v: never started or result already retrieved", args.ID)
	}

	var result *Result
	select {
	case result = <-c:
	case <-s.ctx.Done():
		return logErrContext(ctx, "Server shutting down while waiting for command result")
	}
	// See RequestServerResult: the map must not grow forever on long runs.
	s.resultLock.Lock()
	delete(s.resultC, args.ID)
	s.resultLock.Unlock()
	if result == nil {
		return logErrContext(ctx, "Command result %v was already retrieved", args.ID)
	}
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
