package pkg

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"log/slog"
	"net"
	"os"
	"time"
)

type ICMPReceiver struct{}

func (r *ICMPReceiver) Init() {}

func (r *ICMPReceiver) Run(ctx context.Context, ln net.Listener) (any, error) {
	// DummyListener used
	conn, err := ln.Accept()
	if err != nil {
		panic(err)
	}

	// Receives until ctx is cancelled.
	// ReadDeadline = time.Now() is set to wake up and return from ReceiveFrom.
	go func() {
		<-ctx.Done()
		conn.SetReadDeadline(time.Now())
	}()

	return receiveICMP(ctx, conn)
}

type ICMPParams struct {
	Duration_ time.Duration
	Interval  time.Duration
}

var _ SenderParams = (*ICMPParams)(nil)

func (b ICMPParams) GetDuration() time.Duration {
	return b.Duration_
}

type ICMPSender struct {
	Params ICMPParams
}

func (s *ICMPSender) GetParams() SenderParams {
	return s.Params
}

func (s *ICMPSender) SenderMode() Mode {
	return SendICMP
}

func (s *ICMPSender) ReceiverMode() Mode {
	return ReceiveICMP
}

// Runs until ctx is cancelled
func (s *ICMPSender) Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error) {
	var msgsSent []MsgSent

	ticker := time.NewTicker(s.Params.Interval)
	duration := time.After(s.Params.Duration_)

	go func() {
		<-ctx.Done()
		conn.SetReadDeadline(time.Now())
	}()

	// ICMP sequence numbers are 2 B, starting at 1

	// TODO: terminate correctly through conn?
	// TODO: do something with the result?
	go receiveICMP(ctx, conn)

	seq := uint64(0)

	slog.DebugContext(ctx, "waiting in ICMP run")

outer:
	for {
		select {
		case <-ticker.C:
			msgSent, err := s.send(ctx, conn, raddr, seq)
			if err != nil {
				return msgsSent, err
			}
			msgsSent = append(msgsSent, msgSent)
			seq += 1
		case <-duration:
			break outer
		case <-ctx.Done():
			break outer
		}
	}
	return msgsSent, nil
}

func (s *ICMPSender) send(ctx context.Context, conn net.Conn, raddr net.Addr, seq uint64) (MsgSent, error) {
	// data := []byte("hello from client")
	echoReq := icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Code: 0,
		Body: &icmp.Echo{
			ID:   os.Getpid() & 0xffff,
			Seq:  int(seq % (1 << 16)),
			Data: []byte{},
		},
	}

	// if err := conn.SetReadDeadline(time.Now().Add(1 * time.Second)); err != nil {
	// 	return fmt.Errorf("failed to set read deadline: %w", err)
	// }

	buf, err := echoReq.Marshal(nil)
	msgSent := MsgSent{Seq: seq, TsSent: time.Now(), Len: uint(len(buf))}
	if err != nil {
		return msgSent, fmt.Errorf("failed to marshal ICMP message: %w", err)
	}

	if _, err := conn.(net.PacketConn).WriteTo(buf, raddr); err != nil {
		return msgSent, fmt.Errorf("failed to send ICMP message: %w", err)
	}

	slog.DebugContext(ctx, "sent echo", "msg", echoReq, "body", echoReq.Body)

	return msgSent, nil
}

// Terminates on deadline of conn
func receiveICMP(ctx context.Context, conn net.Conn) ([]MsgRcvd, error) {
	var rcvdMsgs []MsgRcvd
	buf := make([]byte, 1500)

	receivedSinceWrap := 0
	baseSeq := 0
	for {
		n, peer, err := conn.(net.PacketConn).ReadFrom(buf)
		if err != nil {
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				return nil, fmt.Errorf("Failed to read ICMP response: %w", err)
			}
			return rcvdMsgs, nil
		}

		parsedMsg, err := icmp.ParseMessage(1, buf[:n])
		if err != nil {
			return nil, fmt.Errorf("failed to parse ICMP message: %w", err)
		}

		echoType := parsedMsg.Type
		body := parsedMsg.Body.(*icmp.Echo)
		proto := parsedMsg.Type.Protocol()

		switch parsedMsg.Type {
		case ipv4.ICMPTypeEcho:
			fmt.Printf("%d bytes from %s: pid=%d, icmp_type=%v, icmp_seq=%d, data=%s\n",
				body.Len(proto), peer, body.ID, echoType, body.Seq, string(body.Data))
		case ipv4.ICMPTypeEchoReply:
			fmt.Printf("%d bytes from %s: pid=%d, icmp_type=%v, icmp_seq=%d, data=%s\n",
				body.Len(proto), peer, body.ID, echoType, body.Seq, string(body.Data))
		default:
			err := fmt.Errorf("received unexpected message from %s: pid=%d, icmp_type=%v, parsed_type=%v, icmp_seq=%d, data=%s\n",
				peer, body.ID, echoType, parsedMsg.Type, body.Seq, string(body.Data))
			return nil, err
		}

		// Track how many were received since we last increased baseSeq. Once we
		// have received more than half of the ICMP seqnum space, we will
		// consider any sequence number of less than 10k as a packet after a
		// seqnum wrap. We track the number of received packets to prevent
		// double wraps if an out-of-order packet arrives around the wrap-around
		// point.
		if receivedSinceWrap > (1<<16)/2 && body.Seq < 10000 {
			baseSeq += 1 << 16
			receivedSinceWrap = 0
		}
		receivedSinceWrap += 1

		seq := uint64(baseSeq + body.Seq)
		msg := MsgRcvd{Seq: seq, TsRcvd: time.Now(), Len: uint(n)}
		rcvdMsgs = append(rcvdMsgs, msg)
	}
}

type ICMPMockConn struct {
	conn *icmp.PacketConn
}

// Read reads data from the connection.
// Read can be made to time out and return an error after a fixed
// time limit; see SetDeadline and SetReadDeadline.
func (c *ICMPMockConn) Read(b []byte) (n int, err error) {
	n, _, err = c.conn.ReadFrom(b)
	return
}

func (c *ICMPMockConn) Write(b []byte) (n int, err error) {
	panic("ICMPMockConn can not make Write work")
}

func (c *ICMPMockConn) Close() error {
	return c.conn.Close()
}

func (c *ICMPMockConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *ICMPMockConn) RemoteAddr() net.Addr {
	return nil
}

func (c *ICMPMockConn) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *ICMPMockConn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *ICMPMockConn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *ICMPMockConn) ReadFrom(b []byte) (int, net.Addr, error) {
	return c.conn.ReadFrom(b)
}

func (c *ICMPMockConn) WriteTo(b []byte, dst net.Addr) (int, error) {
	return c.conn.WriteTo(b, dst)
}
