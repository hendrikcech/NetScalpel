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

	"github.com/google/uuid"
)

func logErr(fmtStr string, args ...interface{}) error {
	e := fmt.Errorf(fmtStr, args...)
	slog.Error(e.Error())
	return e
}

func logErrCtx(ctx context.Context, fmtStr string, args ...interface{}) error {
	e := fmt.Errorf(fmtStr, args...)
	slog.ErrorContext(ctx, "", "error", e)
	return e
}

func RunServer(ctx context.Context, ip string, port uint) *Server {
	s := NewServer(ctx)

	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		slog.ErrorContext(ctx, "Failed TCP listen", "error", err)
		os.Exit(1)
	}
	slog.InfoContext(ctx, "Listening on TCP for RPC calls", "addr", s.listener.Addr())
	s.wg.Add(1)
	// TODO: readd somewhere else
	// defer listener.Close()

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
	quit     chan any
	ctx      context.Context

	resultsRcvdC map[uuid.UUID]chan *Result[[]MsgRcvd, MsgRcvd]
	resultsSentC map[uuid.UUID]chan *Result[[]MsgSent, MsgSent]
	resultsPathC map[uuid.UUID]chan ResultPath
	resultsLock  sync.RWMutex
}

func NewServer(ctx context.Context) *Server {
	return &Server{quit: make(chan any),
		ctx:          ctx,
		resultsRcvdC: make(map[uuid.UUID]chan *Result[[]MsgRcvd, MsgRcvd]),
		resultsSentC: make(map[uuid.UUID]chan *Result[[]MsgSent, MsgSent]),
		resultsPathC: make(map[uuid.UUID]chan ResultPath),
	}
}

// TODO: replace with context.Cancel?
func (s *Server) Stop() {
	close(s.quit)
	s.listener.Close()
	s.wg.Wait()
}

type RequestUdpServerArgs struct {
	// Server stops reading from socket after Timeout
	Timeout time.Duration

	StartAt time.Time

	Mode UdpServerMode

	Params SenderParams
}
type RequestUdpServerReply struct {
	Id   uuid.UUID
	Port uint
}

func (s *Server) RequestUdpServer(args RequestUdpServerArgs, reply *RequestUdpServerReply) error {
	reply.Id = uuid.New()

	ctx := context.WithValue(s.ctx, SlogIdKey{}, slog.Any("id", reply.Id))

	conn, err := ListenUDP(ctx)
	if err != nil {
		return logErr("ListenUDP failed: %v", err.Error())
	}
	laddr := conn.LocalAddr().(*net.UDPAddr)

	reply.Port = uint(laddr.Port)

	slog.InfoContext(ctx, "RequestUdpServer", "args", args, "reply", reply, "localAddr", laddr)

	if args.Mode == Receive {
		go s.handleReceive(ctx, conn, reply.Id, args)
	} else {
		var sender Sender
		switch args.Mode {
		case SendBurst:
			sender = &BurstSender{Params: args.Params.(BurstParams)}
		case SendRate:
			sender = &RateSender{Params: args.Params.(RateParamsW)}
		case SendPeriodic:
			sender = &PeriodicSender{Params: args.Params.(PeriodicParams)}
		default:
			return logErr("RequestUdpServer: unknown mode %v", args.Mode)
		}
		go s.handleSender(ctx, conn, reply.Id, sender, args)
	}

	return nil
}

func (s *Server) handleReceive(ctx context.Context, conn *net.UDPConn, id uuid.UUID, req RequestUdpServerArgs) {
	defer conn.Close()

	s.resultsLock.Lock()
	s.resultsRcvdC[id] = make(chan *Result[[]MsgRcvd, MsgRcvd], 1)
	s.resultsLock.Unlock()

	var result Result[[]MsgRcvd, MsgRcvd]

	defer func() {
		s.resultsLock.Lock()
		s.resultsRcvdC[id] <- &result
		close(s.resultsRcvdC[id])
		s.resultsLock.Unlock()
	}()

	if result.Err = waitUntil(ctx, req.StartAt); result.Err != nil {
		return
	}

	conn.SetReadDeadline(time.Now().Add(req.Timeout))
	if result.Res, result.Err = ReceiveFrom(ctx, conn, req.Params.NumPackets()); result.Err != nil {
		return
	}
	slog.DebugContext(ctx, "Finished handleReceive", "packets", len(result.Res))
}

func (s *Server) handleSender(ctx context.Context, conn *net.UDPConn, id uuid.UUID, sender Sender, req RequestUdpServerArgs) {
	defer conn.Close()

	s.resultsLock.Lock()
	s.resultsSentC[id] = make(chan *Result[[]MsgSent, MsgSent], 1)
	s.resultsLock.Unlock()

	var result Result[[]MsgSent, MsgSent]

	defer func() {
		s.resultsLock.Lock()
		s.resultsSentC[id] <- &result
		close(s.resultsSentC[id])
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

	if err := waitUntil(ctx, req.StartAt); err != nil {
		result.Err = err
		return
	}

	conn.SetReadDeadline(time.Now().Add(req.Timeout))
	result.Res, result.Err = sender.Run(ctx, conn, raddr.(*net.UDPAddr))
	if result.Err != nil {
		return
	}
	slog.DebugContext(ctx, "Finished handleSender", "packets", len(result.Res), "remoteAddr", raddr)
}

type RequestUdpServerResultArgs struct {
	Id uuid.UUID
}
type RequestUdpServerResultReply struct {
	Msgs []interface{}
}

func (r *RequestUdpServerResultReply) MsgRcvd() []MsgRcvd {
	msgs := make([]MsgRcvd, len(r.Msgs))
	for i, d := range r.Msgs {
		msgs[i] = d.(MsgRcvd)
	}
	return msgs
}
func (r *RequestUdpServerResultReply) MsgSent() []MsgSent {
	msgs := make([]MsgSent, len(r.Msgs))
	for i, d := range r.Msgs {
		msgs[i] = d.(MsgSent)
	}
	return msgs
}

func (s *Server) RequestUdpServerResult(args RequestUdpServerResultArgs, reply *RequestUdpServerResultReply) error {
	ctx := context.WithValue(s.ctx, SlogIdKey{}, slog.Any("id", args.Id))
	slog.DebugContext(ctx, "RequestUdpServerResult: request")

	s.resultsLock.Lock()
	cRcvd, okRcvd := s.resultsRcvdC[args.Id]
	cSent, okSent := s.resultsSentC[args.Id]
	s.resultsLock.Unlock()

	if okRcvd {
		if err := handleChanResult(ctx, cRcvd, args.Id, reply); err != nil {
			return logErr("Receive from RcvdC: %v", err)
		}
	} else if okSent {
		if err := handleChanResult(ctx, cSent, args.Id, reply); err != nil {
			return logErr("Receive from SentC: %v", err)
		}
	} else {
		return logErr("No test with id %v started\n", args.Id)
	}

	slog.DebugContext(ctx, "RequestUdpServerResult: responded")

	return nil
}

func handleChanResult[S []T, T any](ctx context.Context, c chan *Result[S, T], id uuid.UUID, reply *RequestUdpServerResultReply) error {
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

	StartAt time.Time

	Params CommandParams
}

type RunCommandReply struct {
	Id uuid.UUID
	// Port uint
}

func (s *Server) RunCommand(args RunCommandArgs, reply *RunCommandReply) error {
	reply.Id = uuid.New()

	ctx := context.WithValue(s.ctx, SlogIdKey{}, slog.Any("id", reply.Id))

	var command Command
	switch args.Params.(type) {
	case TcpdumpParams:
		command = &TcpdumpCommand{Params_: args.Params.(TcpdumpParams)}
	default:
		return fmt.Errorf("Unknown params %v", args.Params)
	}

	resultDir, err := RandDir(args.Params.Name())
	if err != nil {
		return logErr("RunCommand: failed RandDir: %v", err.Error())
	}

	slog.DebugContext(ctx, "RunCommand", "name", args.Params.Name(), "resultDir", resultDir)

	c := make(chan ResultPath, 1)
	s.resultsLock.Lock()
	s.resultsPathC[reply.Id] = c
	s.resultsLock.Unlock()

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
	Id uuid.UUID
}
type RequestRunCommandResultReply struct {
	Files map[string][]byte
}

func (s *Server) RequestRunCommandResult(args RequestRunCommandResultArgs, reply *RequestRunCommandResultReply) error {
	ctx := context.WithValue(s.ctx, SlogIdKey{}, slog.Any("id", args.Id))

	s.resultsLock.Lock()
	c, ok := s.resultsPathC[args.Id]
	s.resultsLock.Unlock()

	if !ok {
		return logErr("No command with id %v started\n", args.Id)
	}

	result := <-c
	if result.Err != nil {
		return logErr("%v", result.Err.Error())
	}

	entries, err := os.ReadDir(result.Path)
	if err != nil {
		return logErr("Failed os.ReadDir(%v): %v", result.Path, err)
	}

	reply.Files = make(map[string][]byte, len(entries))

	for _, entry := range entries {
		path := filepath.Join(result.Path, entry.Name())

		if entry.IsDir() {
			return logErr("Skipping directory %v", path)
		}

		var buf bytes.Buffer
		w := bufio.NewWriter(&buf)
		if err := CompressFile(path, w); err != nil {
			return logErr("Failed compression: %v", err.Error())
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
