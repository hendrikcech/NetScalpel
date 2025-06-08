package pkg

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
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
	log.Print(e.Error())
	return e
}

func RunServer(ip string, port uint) *Server {
	s := NewServer()

	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Listening on TCP %v for RPC calls\n", s.listener.Addr())
	s.wg.Add(1)
	// TODO: readd somewhere else
	// defer listener.Close()

	handler := rpc.NewServer()
	if err := handler.Register(s); err != nil {
		log.Fatalf("Failed rpc.Register: %v", err.Error())
	}

	// Accept connections and serve them in separate goroutines.
	go func() {
		defer s.wg.Done()
		for {
			conn, err := s.listener.Accept()
			if err != nil {
				if !strings.HasSuffix(err.Error(), "use of closed network connection") {
					log.Printf("Failed listener.Accept(): %v\n", err.Error())
				}
				return
			}
			s.wg.Add(1)
			log.Printf("New RPC client at %v\n", conn.RemoteAddr())
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

	resultsRcvdC map[uuid.UUID]chan *Result[[]MsgRcvd, MsgRcvd]
	resultsSentC map[uuid.UUID]chan *Result[[]MsgSent, MsgSent]
	resultsPathC map[uuid.UUID]chan ResultPath
	resultsLock  sync.RWMutex
}

func NewServer() *Server {
	s := new(Server)
	s.quit = make(chan any)
	s.resultsRcvdC = make(map[uuid.UUID]chan *Result[[]MsgRcvd, MsgRcvd])
	s.resultsSentC = make(map[uuid.UUID]chan *Result[[]MsgSent, MsgSent])
	s.resultsPathC = make(map[uuid.UUID]chan ResultPath)
	return s
}

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
	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return logErr("RequestUdpServer: net.ResolveUDPAddr failed: %v", err.Error())
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return logErr("RequestUdpServer: net.ListenUDP failed: %v\n", err.Error())
	}
	laddr := conn.LocalAddr().(*net.UDPAddr)

	setSocketBuffers(conn)

	reply.Id = uuid.New()
	reply.Port = uint(laddr.Port)

	log.Printf("RequestUdpServer %+v -> %+v: new UDP server listening at %v\n", args, reply, laddr)

	if args.Mode == Receive {
		go s.handleReceive(conn, reply.Id, args)
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
		go s.handleSender(conn, reply.Id, sender, args)
	}

	return nil
}

func (s *Server) handleReceive(conn *net.UDPConn, id uuid.UUID, req RequestUdpServerArgs) {
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

	if result.Err = waitUntil(req.StartAt); result.Err != nil {
		return
	}

	conn.SetReadDeadline(time.Now().Add(req.Timeout))
	if result.Res, result.Err = ReceiveFrom(conn, req.Params.NumPackets()); result.Err != nil {
		return
	}
	log.Printf("Received %v packets for %v\n", len(result.Res), id)
}

func (s *Server) handleSender(conn *net.UDPConn, id uuid.UUID, sender Sender, req RequestUdpServerArgs) {
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

	log.Printf("Received kick-off msg from %v", raddr)

	if err := waitUntil(req.StartAt); err != nil {
		result.Err = err
		return
	}

	conn.SetReadDeadline(time.Now().Add(req.Timeout))
	result.Res, result.Err = sender.Run(conn, raddr.(*net.UDPAddr))
	if result.Err != nil {
		return
	}
	log.Printf("Sent %v messages to %v\n", len(result.Res), raddr)
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
	log.Printf("RequestUdpServerResult: request for %v ...", args.Id)

	s.resultsLock.Lock()
	cRcvd, okRcvd := s.resultsRcvdC[args.Id]
	cSent, okSent := s.resultsSentC[args.Id]
	s.resultsLock.Unlock()

	if okRcvd {
		if err := handleChanResult(cRcvd, args.Id, reply); err != nil {
			return logErr("Receive from RcvdC: %v", err)
		}
	} else if okSent {
		if err := handleChanResult(cSent, args.Id, reply); err != nil {
			return logErr("Receive from SentC: %v", err)
		}
	} else {
		return logErr("No test with id %v started\n", args.Id)
	}

	log.Printf("RequestUdpServerResult: ... responded for %v", args.Id)

	return nil
}

func handleChanResult[S []T, T any](c chan *Result[S, T], id uuid.UUID, reply *RequestUdpServerResultReply) error {
	result, closed := <-c
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

	cmd, err := command.Cmd(resultDir)
	if err != nil {
		return logErr("RunCommand: failed command.Cmd: %v", err.Error())
	}

	reply.Id = uuid.New()
	log.Printf("Writing %v %v result to %v", args.Params.Name(), reply.Id, resultDir)

	c := make(chan ResultPath, 1)
	s.resultsLock.Lock()
	s.resultsPathC[reply.Id] = c
	s.resultsLock.Unlock()

	go func() {
		if err := waitUntil(args.StartAt); err != nil {
			log.Printf("%v", err.Error())
			return
		}

		if err := RunCommand(cmd, args.Params.Timeout()); err != nil {
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
			log.Printf("Failed to remove %v after reading", path)
		}
	}

	if err := os.Remove(result.Path); err != nil {
		log.Printf("Failed to remove result directory %v", result.Path)
	}

	return nil
}
