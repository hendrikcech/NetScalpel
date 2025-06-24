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

type Result[S []T, T any] struct {
	Err error
	Res S
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

	ids          map[string]bool
	resultsRcvdC map[string]chan *Result[[]MsgRcvd, MsgRcvd]
	resultsSentC map[string]chan *Result[[]MsgSent, MsgSent]
	resultsPathC map[string]chan ResultPath
	resultsLock  sync.RWMutex
}

func NewServer(ctx context.Context, slogCh *chan *slog.Record) *Server {
	return &Server{
		ctx:          ctx,
		slogCh:       slogCh,
		ids:          make(map[string]bool),
		resultsRcvdC: make(map[string]chan *Result[[]MsgRcvd, MsgRcvd]),
		resultsSentC: make(map[string]chan *Result[[]MsgSent, MsgSent]),
		resultsPathC: make(map[string]chan ResultPath),
	}
}

// TODO: replace with context.Cancel?
func (s *Server) Stop() {
	s.listener.Close()
	s.wg.Wait()
}

type RequestUDPServerArgs struct {
	ID string

	// Server stops reading from socket after Timeout
	Timeout time.Duration

	StartAt time.Time

	ServerMode Mode

	Params SenderParams
}
type RequestUDPServerReply struct {
	Port uint
}

func (s *Server) RequestUDPServer(args RequestUDPServerArgs, reply *RequestUDPServerReply) error {
	ctx := context.WithValue(s.ctx, SlogIDKey{}, slog.Any("id", args.ID))
	if args.ID == "" {
		return logErrContext(ctx, "RequestUDPServer: ID is empty")
	}

	// Check that the ID is unused
	s.resultsLock.Lock()
	if _, ok := s.ids[args.ID]; ok {
		return logErrContext(ctx, "ID already used")
	}
	s.ids[args.ID] = true
	s.resultsLock.Unlock()

	conn, err := ListenUDP(ctx)
	if err != nil {
		return logErrContext(ctx, "ListenUDP failed: %v", err.Error())
	}
	laddr := conn.LocalAddr().(*net.UDPAddr)

	reply.Port = uint(laddr.Port)

	slog.InfoContext(ctx, "RequestUDPServer", "args", args, "reply", reply, "localAddr", laddr)

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
		return logErrContext(ctx, "RequestUDPServer: unknown mode %v", args.ServerMode)
	}

	if sender != nil {
		go s.handleSender(ctx, conn, args, sender)
	} else {
		go s.handleReceive(ctx, conn, args, receiver)
	}

	return nil
}

func (s *Server) handleReceive(ctx context.Context, conn *net.UDPConn, args RequestUDPServerArgs, receiver Receiver) {
	defer conn.Close()

	s.resultsLock.Lock()
	s.resultsRcvdC[args.ID] = make(chan *Result[[]MsgRcvd, MsgRcvd], 1)
	s.resultsLock.Unlock()

	var result Result[[]MsgRcvd, MsgRcvd]

	defer func() {
		s.resultsLock.Lock()
		s.resultsRcvdC[args.ID] <- &result
		close(s.resultsRcvdC[args.ID])
		s.resultsLock.Unlock()
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
	slog.DebugContext(ctx, "Finished handleReceive", "packets", len(result.Res))
}

func (s *Server) handleSender(ctx context.Context, conn *net.UDPConn, args RequestUDPServerArgs, sender Sender) {
	defer conn.Close()

	s.resultsLock.Lock()
	s.resultsSentC[args.ID] = make(chan *Result[[]MsgSent, MsgSent], 1)
	s.resultsLock.Unlock()

	var result Result[[]MsgSent, MsgSent]

	defer func() {
		s.resultsLock.Lock()
		s.resultsSentC[args.ID] <- &result
		close(s.resultsSentC[args.ID])
		s.resultsLock.Unlock()
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
	slog.DebugContext(ctx, "Finished handleSender", "packets", len(result.Res), "remoteAddr", raddr)
}

type RequestUDPServerResultArgs struct {
	ID string
}
type RequestUDPServerResultReply struct {
	Msgs []interface{}
}

func (r *RequestUDPServerResultReply) MsgRcvd() []MsgRcvd {
	msgs := make([]MsgRcvd, len(r.Msgs))
	for i, d := range r.Msgs {
		msgs[i] = d.(MsgRcvd)
	}
	return msgs
}
func (r *RequestUDPServerResultReply) MsgSent() []MsgSent {
	msgs := make([]MsgSent, len(r.Msgs))
	for i, d := range r.Msgs {
		msgs[i] = d.(MsgSent)
	}
	return msgs
}

func (s *Server) RequestUDPServerResult(args RequestUDPServerResultArgs, reply *RequestUDPServerResultReply) error {
	ctx := context.WithValue(s.ctx, SlogIDKey{}, slog.Any("id", args.ID))
	slog.DebugContext(ctx, "RequestUDPServerResult: request")

	s.resultsLock.Lock()
	cRcvd, okRcvd := s.resultsRcvdC[args.ID]
	cSent, okSent := s.resultsSentC[args.ID]
	s.resultsLock.Unlock()

	if okRcvd {
		if err := handleChanResult(ctx, cRcvd, args.ID, reply); err != nil {
			return logErrContext(ctx, "Receive from RcvdC: %v", err)
		}
	} else if okSent {
		if err := handleChanResult(ctx, cSent, args.ID, reply); err != nil {
			return logErrContext(ctx, "Receive from SentC: %v", err)
		}
	} else {
		return logErrContext(ctx, "No test with id %v started\n", args.ID)
	}

	slog.DebugContext(ctx, "RequestUDPServerResult: responded")

	return nil
}

func handleChanResult[S []T, T any](ctx context.Context, c chan *Result[S, T], id string, reply *RequestUDPServerResultReply) error {
	var (
		result *Result[S, T]
		closed bool
	)
	slog.DebugContext(ctx, "Waiting on chan result")
	select {
	case result, closed = <-c:
		slog.DebugContext(ctx, "Received on chan result")
	case <-ctx.Done():
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
	reply.Msgs = make([]interface{}, len(result.Res))
	for i, d := range result.Res {
		reply.Msgs[i] = d
	}
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

	c := make(chan ResultPath, 1)
	s.resultsLock.Lock()
	if _, ok := s.ids[args.ID]; ok {
		return logErrContext(ctx, "ID already used")
	}
	s.ids[args.ID] = true
	s.resultsPathC[args.ID] = c
	s.resultsLock.Unlock()

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
			c <- ResultPath{Err: fmt.Errorf("RunCommand: command.Exec: %v", err.Error())}
			return
		}

		if err := MonitorCommand(ctx, cmd, args.Params.Timeout()); err != nil {
			c <- ResultPath{Err: fmt.Errorf("RunCommand: %v", err.Error())}
			return
		}

		c <- ResultPath{Path: resultDir}
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

	s.resultsLock.Lock()
	c, ok := s.resultsPathC[args.ID]
	s.resultsLock.Unlock()

	if !ok {
		return logErrContext(ctx, "No command with that ID started")
	}

	result := <-c
	if result.Err != nil {
		return logErrContext(ctx, "%v", result.Err.Error())
	}

	entries, err := os.ReadDir(result.Path)
	if err != nil {
		return logErrContext(ctx, "Failed os.ReadDir(%v): %v", result.Path, err)
	}

	reply.Files = make(map[string][]byte, len(entries))

	for _, entry := range entries {
		path := filepath.Join(result.Path, entry.Name())

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

	if err := os.Remove(result.Path); err != nil {
		slog.WarnContext(ctx, "Failed to remove result directory", "path", result.Path)
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
