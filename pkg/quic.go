package pkg

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	mathRand "math/rand"
	"net"
	"os"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/logging"
)

const quicProto = "quic-echo-example"

type QUICReceiver struct {
	tlsConf *tls.Config
}

func (r *QUICReceiver) Init() {
	r.tlsConf = generateTLSConfig()
}

// Setup a bare-bones TLS config for the server
func generateTLSConfig() *tls.Config {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, priv.Public(), priv)
	if err != nil {
		panic(err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{certDER},
			PrivateKey:  priv,
		}},
		NextProtos: []string{quicProto},
	}
}

func (r *QUICReceiver) Run(ctx context.Context, ln net.Listener) (any, error) {
	tracer := NewQUICTracer()
	cr := NewChanReader[MsgRcvd]()
	go cr.Read(tracer.rcvdC, tracer.quit)

	// DummyListener used
	conn, err := ln.Accept()
	if err != nil {
		panic(err)
	}

	tr := &quic.Transport{
		Conn: conn.(*net.UDPConn),
	}
	conf := &quic.Config{
		Tracer: tracer.NewReceiveTracer,
	}
	listener, err := tr.Listen(r.tlsConf, conf)
	if err != nil {
		return nil, err
	}
	defer listener.Close()

	err = r.listen(ctx, listener)
	// The QUIC connection may outlive listen (e.g. after an abort); stop the
	// tracer via quit instead of closing rcvdC under running callbacks.
	close(tracer.quit)
	if err != nil {
		return nil, err
	}
	<-cr.Done

	return cr.Result, nil
}

func (r *QUICReceiver) listen(ctx context.Context, listener *quic.Listener) error {
	conn, err := listener.Accept(ctx)
	if err != nil {
		return err
	}
	// Deliberately no conn.CloseWithError here: the sender closes the
	// connection, and a receiver-side close races with the sender's final
	// CONNECTION_CLOSE packet, making the tracers' sent/received packet
	// counts diverge. The caller closing the UDP socket tears everything
	// down in the end.

	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		if quicClosedCleanly(err) {
			// The sender already finished and closed the connection before we
			// got to accept the stream; the tracer has the packet counts
			return nil
		}
		return err
	}
	defer stream.Close()

	// Wake the blocking read below when the test ends; without this it
	// returns only once quic-go's idle timeout (~30s) fires if the sender
	// vanishes
	stop := context.AfterFunc(ctx, func() { stream.SetReadDeadline(time.Now()) })
	defer stop()

	slog.DebugContext(ctx, "Reading from QUIC stream")
	n, err := io.Copy(droppingWriter{}, stream)
	slog.DebugContext(ctx, "QUIC Server io.Copy returned", "n", n, "error", err)
	if err != nil {
		if e, ok := err.(net.Error); ok && e.Timeout() {
			// The ctx wakeup above: test over, the data received so far counts
			return nil
		}
		if quicClosedCleanly(err) {
			return nil
		}
		return fmt.Errorf("Unexpected QUIC error: %v", err)
	}

	return nil
}

// A wrapper for io.Writer that also logs the message.
type droppingWriter struct{}

func (w droppingWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

// ---

type QUICParams struct {
	Duration_ time.Duration
	Bytes     uint
}

var _ SenderParams = (*QUICParams)(nil)

func (b QUICParams) GetDuration() time.Duration {
	return b.Duration_
}

type QUICSender struct {
	Params QUICParams
}

func (s *QUICSender) GetParams() SenderParams {
	return s.Params
}

func (s *QUICSender) SenderMode() Mode {
	return SendQUIC
}

func (s *QUICSender) ReceiverMode() Mode {
	return ReceiveQUIC
}

// Runs until ctx is cancelled
func (s *QUICSender) Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error) {
	tracer := NewQUICTracer()
	cr := NewChanReader[MsgSent]()
	go cr.Read(tracer.sentC, tracer.quit)

	err := s.send(ctx, conn, raddr, tracer)
	// The connection teardown emits packets (and tracer callbacks) after send
	// returns; stop the tracer via quit instead of closing sentC under them.
	close(tracer.quit)
	if err != nil {
		return nil, err
	}
	<-cr.Done

	return cr.Result, nil
}

func (s *QUICSender) send(ctx context.Context, conn net.Conn, raddr net.Addr, tracer *QUICTracer) error {
	tr := &quic.Transport{
		Conn: conn.(*net.UDPConn),
	}
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{quicProto},
	}
	conf := &quic.Config{
		Tracer: tracer.NewSendTracer,
	}
	quicConn, err := tr.Dial(ctx, raddr, tlsConf, conf)
	if err != nil {
		return fmt.Errorf("QUIC Dial failed: %v", err.Error())
	}
	defer quicConn.CloseWithError(0, "")

	stream, err := quicConn.OpenStreamSync(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	go func() {
		<-ctx.Done()
		stream.SetWriteDeadline(time.Now())
	}()

	n, err := io.CopyN(stream, mathRand.New(mathRand.NewSource(0)), int64(s.Params.Bytes))
	slog.DebugContext(ctx, "io.copyN of sender returned", "n", n, "error", err)
	if err != nil {
		if errors.Is(err, os.ErrDeadlineExceeded) {
			return nil
		}
		if quicClosedCleanly(err) {
			// The receiver closed the connection (e.g. after an abort on its
			// side); the packets sent until then are the result
			return nil
		}
		return err
	}
	return nil
}

// quicClosedCleanly reports whether err is the peer closing the connection
// with application error code 0, i.e. a clean shutdown. Teardown is not
// synchronized with the data exchange: the CONNECTION_CLOSE can overtake
// stream delivery and surface as an error from AcceptStream or a stream
// read/write instead of a clean EOF (flaky TestQUICDL). The tracers count
// wire packets independently of the application-level stream, so results
// are complete despite the error.
func quicClosedCleanly(err error) bool {
	var appErr *quic.ApplicationError
	return errors.As(err, &appErr) && appErr.ErrorCode == 0
}

// The channels are never closed: quic-go emits tracer callbacks from the
// connection's run loop, which can outlive the sender/receiver Run functions
// (teardown packets, or a peer that is still sending after an abort). Closing
// the quit channel stops collection instead; late callbacks are discarded.
type QUICTracer struct {
	sentC chan MsgSent
	rcvdC chan MsgRcvd
	quit  chan struct{}
}

func NewQUICTracer() *QUICTracer {
	return &QUICTracer{
		sentC: make(chan MsgSent, 1000),
		rcvdC: make(chan MsgRcvd, 1000),
		quit:  make(chan struct{}),
	}
}

func (t *QUICTracer) NewSendTracer(ctx context.Context, pers logging.Perspective, id quic.ConnectionID) *logging.ConnectionTracer {
	return &logging.ConnectionTracer{
		SentShortHeaderPacket: func(hdr *logging.ShortHeader, size logging.ByteCount, ecn logging.ECN, ack *logging.AckFrame, frames []logging.Frame) {
			// slog.DebugContext(ctx, "Sent", "dcid", hdr.DestConnectionID, "pn", hdr.PacketNumber)
			msg := MsgSent{
				Seq:    uint64(hdr.PacketNumber),
				TsSent: time.Now(),
				Len:    uint(size),
			}
			// Prefer the buffered send: a select over both a ready channel
			// and the closed quit picks randomly and would drop packets that
			// race with quit (the drain in ChanReader.Read still counts them)
			select {
			case t.sentC <- msg:
			default:
				select {
				case t.sentC <- msg:
				case <-t.quit:
				}
			}
		},
	}
}

func (t *QUICTracer) NewReceiveTracer(ctx context.Context, pers logging.Perspective, id quic.ConnectionID) *logging.ConnectionTracer {
	return &logging.ConnectionTracer{
		ReceivedShortHeaderPacket: func(hdr *logging.ShortHeader, size logging.ByteCount, ecn logging.ECN, frames []logging.Frame) {
			msg := MsgRcvd{
				Seq:    uint64(hdr.PacketNumber),
				TsRcvd: time.Now(),
				Len:    uint(size),
			}
			// See NewSendTracer: prefer the buffered send over quit
			select {
			case t.rcvdC <- msg:
			default:
				select {
				case t.rcvdC <- msg:
				case <-t.quit:
				}
			}
		},
	}
}

type ChanReader[T MsgSent | MsgRcvd] struct {
	Result []T
	Done   chan bool
}

func NewChanReader[T MsgSent | MsgRcvd]() *ChanReader[T] {
	return &ChanReader[T]{
		Done: make(chan bool, 1),
	}
}

// Read collects from c until quit is closed, then keeps draining briefly so
// tracer callbacks that raced with quit are still counted.
func (r *ChanReader[T]) Read(c chan T, quit chan struct{}) {
	for {
		select {
		case e := <-c:
			r.Result = append(r.Result, e)
		case <-quit:
			for {
				select {
				case e := <-c:
					r.Result = append(r.Result, e)
				case <-time.After(100 * time.Millisecond):
					r.Done <- true
					return
				}
			}
		}
	}
}
