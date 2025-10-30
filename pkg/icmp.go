package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"syscall"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// The receiver reacts to ICMP packets of msg.Type == r.ICMPType (on server
// ICMPTypeEchoReply, on client ICMPTypeEcho) that have the ClientEchoID encoded
// in the ICMP data field.
type ICMPReceiver struct {
	ClientEchoID uint16
	ICMPType     ipv4.ICMPType
}

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

	return r.receive(ctx, conn)
}

// Terminates on deadline of conn
func (r *ICMPReceiver) receive(ctx context.Context, conn net.Conn) ([]MsgRcvd, error) {
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

		msg, err := icmp.ParseMessage(1, buf[:n])
		if err != nil {
			return nil, fmt.Errorf("failed to parse ICMP message: %w", err)
		}

		if msg.Type != ipv4.ICMPTypeEcho && msg.Type != ipv4.ICMPTypeEchoReply {
			slog.WarnContext(ctx, "Received unexpected ICMP type", "peer", peer, "msg", msg)
			continue
		}

		if msg.Type != r.ICMPType {
			continue
		}

		body, ok := msg.Body.(*icmp.Echo)
		if !ok {
			slog.WarnContext(ctx, "ICMP body not Echo")
			continue
		}

		echoID, punch, err := parseICMPData(body.Data)
		if err != nil {
			slog.WarnContext(ctx, "Failed parsing ICMP data", "error", err)
			continue
		}

		if punch {
			slog.DebugContext(ctx, "Received ICMP hole punch packet")
			continue
		}

		if echoID != r.ClientEchoID {
			slog.WarnContext(ctx, "Received ICMP Echo with wrong ID in data", "exp", r.ClientEchoID, "id", body.ID)
			continue
		}

		slog.DebugContext(ctx, "rcvd echo", "msg", msg, "body", body)

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
		rcvdMsgs = append(rcvdMsgs, MsgRcvd{Seq: seq, TsRcvd: time.Now(), Len: uint(n)})
	}
}

type ICMPParams struct {
	Duration_ time.Duration
	Interval  time.Duration
	// Automatically set if ClientEchoID == 0 by GetParams() by the client
	ClientEchoID uint16
	// On client, ClientEchoID == SenderEchoID. Not true on the server: the
	// echoID is changed in-flight and the server must use the echoID for
	// sending that is correct from its PoV.
	SenderEchoID uint16
	// TODO: true?
	// Set by client: Must be set to: UL -> ICMPTypeEcho, DL -> ipv4.ICMPTypeEchoReply
	// By default ICMPTypeEchoReply (value 0), i.e., suitable for the server
	ICMPType ipv4.ICMPType
	ICMPData []byte
}

var _ SenderParams = (*ICMPParams)(nil)

func (b ICMPParams) GetDuration() time.Duration {
	return b.Duration_
}

type ICMPSender struct {
	Params ICMPParams
	// Client: ipv4.ICMPTypeEcho
	// Server: ipv4.ICMPTypeEchoReply
	// ICMPType ipv4.ICMPType
	// icmpTypeSet bool
}

func (s *ICMPSender) GetParams() SenderParams {
	if s.Params.ClientEchoID == 0 {
		var err error
		if s.Params.ClientEchoID, err = genEchoID(); err != nil {
			slog.Error("Failed generating echo ID but continuing")
		}
		s.Params.SenderEchoID = s.Params.ClientEchoID
		slog.Debug("Generated ICMP ClientEchoID", "echoID", s.Params.ClientEchoID)
	}
	// TODO: what to do here?
	// Default ICMPType 0 is ICMPTypeEchoReply -> Correct if server is sending
	// if !s.icmpTypeSet {
	// 	s.ICMPType = ipv4.ICMPTypeEcho
	// 	s.icmpTypeSet = true
	// }
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
	seq := uint64(0)

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
			return msgsSent, nil
		case <-ctx.Done():
			return msgsSent, nil
		}
	}
}

func (s *ICMPSender) send(ctx context.Context, conn net.Conn, raddr net.Addr, seq uint64) (MsgSent, error) {
	msg := icmp.Message{
		Type: s.Params.ICMPType,
		Code: 0,
		Body: &icmp.Echo{
			ID:   int(s.Params.SenderEchoID),
			Seq:  int(seq % (1 << 16)),
			Data: makeICMPData(s.Params.ClientEchoID, false),
		},
	}
	buf, err := msg.Marshal(nil)
	msgSent := MsgSent{Seq: seq, TsSent: time.Now(), Len: uint(len(buf))}
	if err != nil {
		return msgSent, fmt.Errorf("failed to marshal ICMP message: %w", err)
	}
	if _, err := conn.(net.PacketConn).WriteTo(buf, raddr); err != nil {
		return msgSent, fmt.Errorf("failed to send ICMP message: %w", err)
	}
	slog.DebugContext(ctx, "sent echo", "msg", msg, "body", msg.Body, "echoID", s.Params.SenderEchoID, "dataEchoID", s.Params.ClientEchoID)
	return msgSent, nil
}

// func sendSingleICMP(ctx context.Context, conn net.Conn, raddr net.Addr, msg icmp.Message) (MsgSent, error) {
// 	buf, err := msg.Marshal(nil)
// 	msgSent := MsgSent{Seq: msg.Body.(*icmp.Echo).Seq, TsSent: time.Now(), Len: uint(len(buf))}
// 	if err != nil {
// 		return msgSent, fmt.Errorf("failed to marshal ICMP message: %w", err)
// 	}
// 	if _, err := conn.(net.PacketConn).WriteTo(buf, raddr); err != nil {
// 		return msgSent, fmt.Errorf("failed to send ICMP message: %w", err)
// 	}
// 	slog.DebugContext(ctx, "sent echo", "msg", echoReq, "body", echoReq.Body)
// 	return msgSent, nil
// }

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

func genEchoID() (uint16, error) {
	for {
		// Don't generate 0
		id := rand.Intn((1<<16)-1-1) + 1
		exists, err := pidExists(id)
		if err != nil {
			slog.Warn("Failed to check pid", "pid", id, "error", err)
			// Still return: if we can't check the PID, what else are wo gonna do
			return uint16(id), nil
		}
		if !exists {
			return uint16(id), nil
		}
	}
}

// Checks if a process with the giving process ID exists.
func pidExists(pid int) (bool, error) {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false, err
	}

	// Signal 0 checks for existence without sending a signal
	err = process.Signal(syscall.Signal(0))
	if err == nil {
		return true, nil // Process exists and we have permission
	}

	if err.Error() == "operation not permitted" {
		return true, nil // Process exists, but we don't have permission
	}

	return false, nil
}

// Set in the data field of ICMP Echo packets if this packet was sent to punch a
// hole into a NAT.
var ICMPProbePayload = []byte("whatapunch")

func makeICMPData(echoID uint16, punch bool) []byte {
	size := 2
	if punch {
		size += len(ICMPProbePayload)
	}
	data := make([]byte, 2, size)
	binary.LittleEndian.PutUint16(data, echoID)
	if punch {
		data = append(data, ICMPProbePayload...)
	}
	return data
}

func parseICMPData(data []byte) (echoID uint16, punch bool, err error) {
	if len(data) < 2 {
		err = fmt.Errorf("ICMP data too short")
		return
	}
	echoID = binary.LittleEndian.Uint16(data[:2])
	if len(data) == 2+len(ICMPProbePayload) {
		punch = bytes.Equal(ICMPProbePayload, data[2:])
	}
	return
}
