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

	ids        map[string]bool
	resultC    map[string]chan *Result
	resultLock sync.RWMutex
}

func NewServer(ctx context.Context, slogCh *chan *slog.Record) *Server {
	return &Server{
		ctx:     ctx,
		slogCh:  slogCh,
		ids:     make(map[string]bool),
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

	// Check that the ID is unused
	s.resultLock.Lock()
	if _, ok := s.ids[args.ID]; ok {
		return logErrContext(ctx, "ID already used")
	}
	s.ids[args.ID] = true
	s.resultLock.Unlock()

	conn, err := ListenUDP(ctx)
	if err != nil {
		return logErrContext(ctx, "ListenUDP failed: %v", err.Error())
	}
	laddr := conn.LocalAddr().(*net.UDPAddr)

	reply.Port = uint(laddr.Port)

	slog.InfoContext(ctx, "RequestServer", "args", args, "reply", reply, "localAddr", laddr)

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
		return logErrContext(ctx, "RequestServer: unknown mode %v", args.ServerMode)
	}

	if sender != nil {
		go s.handleSender(ctx, conn, args, sender)
	} else {
		go s.handleReceive(ctx, conn, args, receiver)
	}

	return nil
}

func (s *Server) handleReceive(ctx context.Context, conn *net.UDPConn, args RequestServerArgs, receiver Receiver) {
	defer conn.Close()

	s.resultLock.Lock()
	s.resultC[args.ID] = make(chan *Result, 1)
	s.resultLock.Unlock()

	var result Result

	defer func() {
		s.resultLock.Lock()
		s.resultC[args.ID] <- &result
		close(s.resultC[args.ID])
		s.resultLock.Unlock()
	}()

	receiver.Init()

	if result.Err = waitUntil(ctx, args.StartAt); result.Err != nil {
		return
	}

	recvCtx, recvCancel := context.WithTimeout(ctx, args.Timeout)
	defer recvCancel()
	if result.Res, result.Err = receiver.Run(recvCtx, conn, args.Params.NumPackets()); result.Err != nil {
		return
	}
	slog.DebugContext(ctx, "Finished handleReceive")
	// , "packets", len(result.Res)
}

func (s *Server) handleSender(ctx context.Context, conn *net.UDPConn, args RequestServerArgs, sender Sender) {
	defer conn.Close()

	s.resultLock.Lock()
	s.resultC[args.ID] = make(chan *Result, 1)
	s.resultLock.Unlock()

	var result Result

	defer func() {
		s.resultLock.Lock()
		s.resultC[args.ID] <- &result
		close(s.resultC[args.ID])
		s.resultLock.Unlock()
	}()

	// Wait for single UDP packet from receiving client UDP socket that opens
	// the client NAT.
	var buf [1500]byte
	_, raddr, err := conn.ReadFrom(buf[0:])
	if err != nil {
		if e, ok := err.(net.Error); !ok || !e.Timeout() {
			// not a timeout
			result.Err = fmt.Errorf("handleSender: ReadFrom: %v", err.Error())
			return
		}
		return
	}

	// Reply to NAT UDP packet
	if _, err := conn.WriteTo([]byte{}, raddr); err != nil {
		result.Err = fmt.Errorf("handleSender: WriteTo: %v", err.Error())
		return
	}

	slog.DebugContext(ctx, "Received kick-off msg", "remoteAddr", raddr)

	if err := waitUntil(ctx, args.StartAt); err != nil {
		result.Err = err
		return
	}

	// TODO: Why was the read ddeadline set here?
	// conn.SetReadDeadline(time.Now().Add(args.Timeout))

	sendCtx, sendCancel := context.WithTimeout(ctx, args.Params.GetDuration())
	defer sendCancel()
	result.Res, result.Err = sender.Run(sendCtx, conn, raddr.(*net.UDPAddr))
	if result.Err != nil {
		return
	}
	slog.DebugContext(ctx, "Finished handleSender", "remoteAddr", raddr)
	// "packets", len(result.Res),
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
	if _, ok := s.ids[args.ID]; ok {
		return logErrContext(ctx, "ID already used")
	}
	s.ids[args.ID] = true
	s.resultC[args.ID] = c
	s.resultLock.Unlock()

	var command Command
	switch args.Params.(type) {
	case TcpdumpParams:
		command = &TcpdumpCommand{Params_: args.Params.(TcpdumpParams)}
	default:
		return fmt.Errorf("Unknown params %v", args.Params)
	}

	resultDir, err := RandDir(args.Params.Name())
	if err != nil {
		return logErrContext(ctx, "RunCommand: failed RandDir: %v", err.Error())
	}

	slog.DebugContext(ctx, "RunCommand", "name", args.Params.Name(), "resultDir", resultDir)

	go func() {
		if err := waitUntil(ctx, args.StartAt); err != nil {
			slog.WarnContext(ctx, "WaitUntil failed", "error", err)
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
		return logErrContext(ctx, "%v", result.Err.Error())
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
