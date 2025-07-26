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

func (r *QUICReceiver) Run(ctx context.Context, udpConn *net.UDPConn, expectedNumPackets uint) (any, error) {
	tracer := &QUICTracer{rcvdC: make(chan MsgRcvd, 1000)}
	cr := NewChanReader[MsgRcvd]()
	go cr.Read(tracer.rcvdC)

	tr := &quic.Transport{
		Conn: udpConn,
	}
	conf := &quic.Config{
		Tracer: tracer.NewReceiveTracer,
	}
	listener, err := tr.Listen(r.tlsConf, conf)
	if err != nil {
		return nil, err
	}
	defer listener.Close()

	if err := r.listen(ctx, listener); err != nil {
		return nil, err
	}

	close(tracer.rcvdC)
	<-cr.Done

	return cr.Result, nil
}

func (r *QUICReceiver) listen(ctx context.Context, listener *quic.Listener) error {
	conn, err := listener.Accept(ctx)
	if err != nil {
		return err
	}

	stream, err := conn.AcceptStream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()

	slog.DebugContext(ctx, "Reading from QUIC stream")
	n, err := io.Copy(droppingWriter{}, stream)
	slog.DebugContext(ctx, "QUIC Server io.Copy returned", "n", n, "error", err)
	if err != nil {
		var appErr *quic.ApplicationError
		if errors.As(err, &appErr) {
			if appErr.ErrorCode == 0 {
				return nil
			}
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

func (b QUICParams) NumPackets() uint {
	return 0 // TODO: this doesn't make sense for QUIC
}

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
func (s *QUICSender) Run(ctx context.Context, udpConn *net.UDPConn, raddr *net.UDPAddr) (any, error) {
	tracer := &QUICTracer{sentC: make(chan MsgSent, 1000)}
	cr := NewChanReader[MsgSent]()
	go cr.Read(tracer.sentC)

	if err := s.send(ctx, udpConn, raddr, tracer); err != nil {
		return nil, err
	}

	close(tracer.sentC)
	<-cr.Done

	return cr.Result, nil
}

func (s *QUICSender) send(ctx context.Context, udpConn *net.UDPConn, raddr *net.UDPAddr, tracer *QUICTracer) error {
	tr := &quic.Transport{
		Conn: udpConn,
	}
	tlsConf := &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{quicProto},
	}
	conf := &quic.Config{
		Tracer: tracer.NewSendTracer,
	}
	conn, err := tr.Dial(ctx, raddr, tlsConf, conf)
	if err != nil {
		return fmt.Errorf("QUIC Dial failed: %v", err.Error())
	}
	defer conn.CloseWithError(0, "")

	stream, err := conn.OpenStreamSync(ctx)
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
		return err
	}
	return nil
}

type QUICTracer struct {
	sentC chan MsgSent
	rcvdC chan MsgRcvd
}

func (t *QUICTracer) NewSendTracer(ctx context.Context, pers logging.Perspective, id quic.ConnectionID) *logging.ConnectionTracer {
	return &logging.ConnectionTracer{
		SentShortHeaderPacket: func(hdr *logging.ShortHeader, size logging.ByteCount, ecn logging.ECN, ack *logging.AckFrame, frames []logging.Frame) {
			// slog.DebugContext(ctx, "Sent", "dcid", hdr.DestConnectionID, "pn", hdr.PacketNumber)
			t.sentC <- MsgSent{
				Seq:    uint64(hdr.PacketNumber),
				TsSent: time.Now(),
				Len:    uint(size),
			}
		},
	}
}

func (t *QUICTracer) NewReceiveTracer(ctx context.Context, pers logging.Perspective, id quic.ConnectionID) *logging.ConnectionTracer {
	return &logging.ConnectionTracer{
		ReceivedShortHeaderPacket: func(hdr *logging.ShortHeader, size logging.ByteCount, ecn logging.ECN, frames []logging.Frame) {
			t.rcvdC <- MsgRcvd{
				Seq:    uint64(hdr.PacketNumber),
				TsRcvd: time.Now(),
				Len:    uint(size),
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

func (r *ChanReader[T]) Read(c chan T) {
	for e := range c {
		r.Result = append(r.Result, e)
	}
	r.Done <- true
}
