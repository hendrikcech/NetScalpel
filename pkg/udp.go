package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

// Dummy type to unify UDP and TCP receiver interfaces. Is constructed with a conn
// and simply returns that conn on the first call to Accept().
type DummyListener struct {
	c         chan net.Conn
	laddr     net.Addr
	closeOnce sync.Once
}

func NewDummyListener(conn net.Conn, laddr net.Addr) *DummyListener {
	c := make(chan net.Conn, 1)
	c <- conn
	return &DummyListener{c: c, laddr: laddr}
}

// Accept returns conn and nothing afterwards.
func (l *DummyListener) Accept() (net.Conn, error) {
	conn, ok := <-l.c
	if !ok {
		return nil, fmt.Errorf("DummyListener closed")
	}
	return conn, nil
}

// Close closes the listener. Safe to call multiple times.
// Any blocked Accept operations will be unblocked and return errors.
func (l *DummyListener) Close() error {
	l.closeOnce.Do(func() { close(l.c) })
	return nil
}

// Addr returns the listener's network address.
func (l *DummyListener) Addr() net.Addr {
	return l.laddr
}

var _ net.Listener = (*DummyListener)(nil)

type UDPReceiver struct {
}

func (r *UDPReceiver) Init() {
}

// Receives until ctx is cancelled. ReadDeadline = time.Now() is set to wake up and return from ReceiveFrom.
func (r *UDPReceiver) Run(ctx context.Context, ln net.Listener) (any, error) {
	// DummyListener used
	conn, err := ln.Accept()
	if err != nil {
		panic(err)
	}

	tsEnabled := true
	if err := enableRxTimestamping(conn.(*net.UDPConn)); err != nil {
		slog.WarnContext(ctx, "Failed enabling rx timestamping", "error", err.Error())
		tsEnabled = false
	}

	// conn ReadDeadline must be set, otherwise this function never returns
	// msgs := make([]MsgRcvd, 0, int(float64(expectedNumPackets)*1.1))
	msgs := make([]MsgRcvd, 0, 1024)

	// batch size
	rx := make([]mmsg.Message, 1024)
	for k := range rx {
		rx[k].Buffers = [][]byte{make([]byte, 1500)}
		rx[k].OOB = make([]byte, 500)
	}

	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return nil, fmt.Errorf("Failed mmsg.NewConn: %w", err)
	}

	go func() {
		<-ctx.Done()
		conn.SetReadDeadline(time.Now())
	}()

	var tsRcvd time.Time
	for {
		n, err := mconn.RecvMsgs(rx, 0)
		if err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				// not a timeout
				return nil, fmt.Errorf("ReceiveFrom: ReadMsgs: %w", err)
			}
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "RecvMsgs returned Timeout error from unknown origin -> extending",
					"error", err,
					"ctxErr", ctx.Err())
				conn.SetReadDeadline(time.Now().Add(1000 * time.Hour))
				continue
			}
			// Grace period to collect in-flight packets, but only on natural
			// test end (duration deadline); on user abort return right away.
			if errors.Is(ctx.Err(), context.DeadlineExceeded) && time.Since(tsRcvd) < 250*time.Millisecond {
				msg := "Wanted to stop but previous packet was received very recently; trying again in 250 ms"
				slog.WarnContext(ctx, msg, "msSinceLastPacket", time.Since(tsRcvd).Milliseconds())
				conn.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
				continue
			}
			break
		}

		tsRcvd = time.Now()

		for i := 0; i < n; i++ {
			packet := rx[i]
			if packet.N == 0 {
				// Ignore probe packets
				continue
			}

			var msg Msg
			msg.Decode(packet.Buffers[0][:packet.N])
			rcvd := MsgRcvd{
				Seq:    msg.Seq,
				TsRcvd: tsRcvd,
				Len:    uint(packet.N),
			}

			if tsEnabled {
				cms, err := unix.ParseSocketControlMessage(packet.OOB[:packet.NN])
				if err != nil {
					slog.ErrorContext(ctx, "receiveFrom: Failed parsing cmsg", "error", err)
				}

				timestampParsed := false
				for _, cm := range cms {
					if cm.Header.Level == syscall.SOL_SOCKET && cm.Header.Type == syscall.SCM_TIMESTAMPING {
						var times unix.ScmTimestamping
						tsBuf := bytes.NewReader(cm.Data)
						binary.Read(tsBuf, binary.LittleEndian, &times)
						ts := times.Ts[0]
						rcvd.TsRcvd = time.Unix(ts.Sec, ts.Nsec)
						timestampParsed = true
					}
				}

				if !timestampParsed {
					slog.WarnContext(ctx, "Missing rx timestamp", "seq", msg.Seq)
				}
			}

			msgs = append(msgs, rcvd)
		}
	}

	return msgs, nil
}

type BurstParams struct {
	Timeout time.Duration
	Num     uint
	Pad     uint
}

var _ SenderParams = (*BurstParams)(nil)

func (b BurstParams) GetDuration() time.Duration {
	return b.Timeout
}

// Sends numPackets packets as quickly as possible
type BurstSender struct {
	Params BurstParams
}

func (r *BurstSender) GetParams() SenderParams {
	return r.Params
}

func (s *BurstSender) SenderMode() Mode {
	return SendBurst
}

func (s *BurstSender) ReceiverMode() Mode {
	return ReceiveUDP
}

func (s *BurstSender) Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error) {
	tsReader, tsReaderCancel := startTxTsReader(ctx, conn.(*net.UDPConn))
	defer tsReaderCancel()

	msgsSent, err := s.run(ctx, conn, raddr)
	if err != nil {
		return nil, err
	}

	msgsSent = selectMsgsSent(ctx, tsReader, tsReaderCancel, msgsSent)

	return msgsSent, nil
}

func (s *BurstSender) run(ctx context.Context, conn net.Conn, raddr net.Addr) ([]MsgSent, error) {
	msgsSent := make([]MsgSent, s.Params.Num)
	msgs := make([]mmsg.Message, s.Params.Num)
	for i := 0; i < int(s.Params.Num); i++ {
		b := Msg{Seq: uint64(i), PadN: s.Params.Pad}
		buf := make([]byte, 1500)
		n, err := b.Encode(buf)
		if err != nil {
			return nil, err
		}

		msgs[i].Buffers = append(msgs[i].Buffers, buf[:n])
		msgs[i].Addr = raddr

		msgsSent[i] = MsgSent{Seq: b.Seq, Len: 8 + s.Params.Pad}
	}

	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return nil, fmt.Errorf("Failed mmsg.NewConn: %w", err)
	}

	// Same wakeup as in RateSender.Run: unblock SendMsgs when the test ends
	stopWake := context.AfterFunc(ctx, func() { conn.SetWriteDeadline(time.Now()) })
	defer stopWake()

	sentN := uint(0)
	for sentN < s.Params.Num {
		if ctx.Err() != nil {
			return msgsSent[:sentN], nil
		}
		left := s.Params.Num - sentN
		numRound := min(1024, left)

		tsSent := time.Now()
		for i := uint(0); i < numRound; i++ {
			idx := sentN + i
			msgsSent[idx].TsSent = tsSent
		}

		tx := msgs[sentN : sentN+numRound]
		n, err := mconn.SendMsgs(tx, 0)
		if n != int(numRound) {
			slog.WarnContext(ctx, fmt.Sprintf("Only bursted %v packets instead of %v", n, numRound),
				"sentN", sentN, "left", left)
		}
		if err != nil {
			if n > 0 {
				sentN += uint(n)
			}
			if e, ok := err.(net.Error); ok && e.Timeout() {
				// Test over (write deadline wakeup); report what went out
				return msgsSent[:sentN], nil
			}
			return nil, err
		}
		sentN += uint(n)
		if n != int(numRound) {
			break
		}
	}

	return msgsSent[:sentN], nil
}

var _ SenderParams = (*PeriodicParams)(nil)

type PeriodicParams struct {
	Interval time.Duration
	Duration time.Duration
	Pad      uint
}

func (p PeriodicParams) NumPackets() uint {
	return uint(float64(1e9/p.Interval.Nanoseconds()) * p.Duration.Seconds())
}

func (p PeriodicParams) GetDuration() time.Duration {
	return p.Duration
}

// Sends one packet every interval
type PeriodicSender struct {
	Params PeriodicParams
	seq    uint64
}

var _ Sender = (*PeriodicSender)(nil)

func (r *PeriodicSender) GetParams() SenderParams {
	return r.Params
}

func (s *PeriodicSender) SenderMode() Mode {
	return SendPeriodic
}

func (s *PeriodicSender) ReceiverMode() Mode {
	return ReceiveUDP
}

func (r *PeriodicSender) Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error) {
	tsReader, tsReaderCancel := startTxTsReader(ctx, conn.(*net.UDPConn))
	defer tsReaderCancel()

	msgsSent, err := r.run(ctx, conn, raddr)
	if err != nil {
		return nil, err
	}

	msgsSent = selectMsgsSent(ctx, tsReader, tsReaderCancel, msgsSent)

	return msgsSent, nil
}
func (r *PeriodicSender) run(ctx context.Context, conn net.Conn, raddr net.Addr) ([]MsgSent, error) {
	msgsSent := make([]MsgSent, 0, r.Params.NumPackets())

	ticker := time.NewTicker(r.Params.Interval)
	defer ticker.Stop()
	duration := time.After(r.Params.Duration)
	buf := make([]byte, 1500)

	udpConn := conn.(*net.UDPConn)

	// Unblock a WriteTo stuck on a full socket buffer when the test ends;
	// the timeout error below is treated as a clean stop
	stopWake := context.AfterFunc(ctx, func() { udpConn.SetWriteDeadline(time.Now()) })
	defer stopWake()

	seq := 0
	for {
		select {
		case <-ticker.C:
			b := Msg{Seq: uint64(seq), PadN: r.Params.Pad}
			n, err := b.Encode(buf)
			if err != nil {
				return nil, err
			}
			read := buf[:n]

			msgsSent = append(msgsSent, MsgSent{Seq: b.Seq, TsSent: time.Now(), Len: 8 + r.Params.Pad})

			nConn, err := udpConn.WriteTo(read, raddr)
			if err != nil {
				// The message appended above was not sent
				msgsSent = msgsSent[:len(msgsSent)-1]
				if e, ok := err.(net.Error); ok && e.Timeout() {
					return msgsSent, nil
				}
				return nil, err
			}
			if nConn != len(read) {
				slog.WarnContext(ctx, fmt.Sprintf("Not written the entire buffer: %v != %v", n, nConn))
			}

			seq += 1
		case <-duration:
			return msgsSent, nil
		case <-ctx.Done():
			return msgsSent, nil
		}
	}
}

type RateParams struct {
	Pps         uint
	Interval    time.Duration // TODO: deprecate
	Duration    time.Duration
	PayloadSize uint
}

var _ SenderParams = (*RateParams)(nil)

func (r RateParams) NumPackets() uint {
	// return uint(float64(uint(r.RateMbps*float64(1e6))/8/r.PayloadSize) * r.Duration.Seconds())
	return uint(float64(r.Pps) * r.Duration.Seconds())
}

func (r RateParams) GetDuration() time.Duration {
	return r.Duration
}

type RateParamsW []RateParams

var _ SenderParams = (*RateParamsW)(nil)

func (r RateParamsW) NumPackets() uint {
	packets := uint(0)
	for i := range r {
		packets += r[i].NumPackets()
	}
	return packets
}

func (r RateParamsW) GetDuration() time.Duration {
	var duration time.Duration
	for i := range r {
		duration += r[i].GetDuration()
	}
	return duration
}

// Generate Params.Pps many packets per second
type RateSender struct {
	Params    RateParamsW
	seq       uint64
	tsEnabled bool
	msgs      []MsgSent
	tx        []mmsg.Message
}

var _ Sender = (*RateSender)(nil)

func (r *RateSender) GetParams() SenderParams {
	return r.Params
}

func (s *RateSender) SenderMode() Mode {
	return SendRate
}

func (s *RateSender) ReceiverMode() Mode {
	return ReceiveUDP
}

func (r *RateSender) Run(ctx context.Context, conn net.Conn, raddr net.Addr) (any, error) {
	r.msgs = make([]MsgSent, 0, int(float64(r.Params.NumPackets())*1.1))
	r.tx = make([]mmsg.Message, 1024)
	for i := range r.tx {
		r.tx[i].Buffers = [][]byte{make([]byte, 1500)}
	}

	tsReader, tsReaderCancel := startTxTsReader(ctx, conn.(*net.UDPConn))
	defer tsReaderCancel()

	// Wake a SendMsgs blocked on a full socket buffer when the test ends;
	// the send loops treat the timeout error as a clean stop.
	stopWake := context.AfterFunc(ctx, func() { conn.SetWriteDeadline(time.Now()) })
	defer stopWake()

	for i := range r.Params {
		// Segment duration is owned by a per-segment child context; the
		// caller's ctx carries the total duration and user cancellation.
		segCtx, segCancel := context.WithTimeout(ctx, r.Params[i].Duration)
		err := r.runParams(segCtx, conn, raddr, r.Params[i])
		segCancel()
		if err != nil {
			return nil, err
		}
		if ctx.Err() != nil {
			break
		}
	}

	r.msgs = selectMsgsSent(ctx, tsReader, tsReaderCancel, r.msgs)

	return r.msgs, nil
}

func (r *RateSender) runParams(ctx context.Context, conn net.Conn, raddr net.Addr, params RateParams) error {
	if params.Pps == 0 {
		// Cooldown segment: send nothing until the segment duration is
		// reached or the caller aborts. (Also avoids the interval formula
		// below dividing by zero.)
		<-ctx.Done()
		return nil
	}

	start := time.Now() // must appear before ticker is started

	maxPacketPerBurst := float64(5)
	calcInterval := time.Duration((1/(float64(params.Pps)/maxPacketPerBurst))*1e9) * time.Nanosecond
	interval := min(calcInterval, 1*time.Millisecond)
	slog.DebugContext(ctx, "Dynamic interval calculation", "interval", interval.String(), "calculatedInterval", calcInterval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	numPacketsSent := uint(0)

	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return fmt.Errorf("Failed mmsg.NewConn: %w", err)
	}

	for {
		select {
		case now := <-ticker.C:
			elapsed := now.Sub(start).Seconds()
			numPacketsGoal := uint(elapsed * float64(params.Pps))
			if elapsed < 0 {
				slog.ErrorContext(ctx, "elapsed < 0",
					"sent", numPacketsSent,
					"goal", numPacketsGoal,
					"elapsed", elapsed,
					"start", start,
					"now", now)
				panic("elapsed < 0")
			}
			if numPacketsSent > numPacketsGoal {
				// I am not sure how this happens but it seldomly does
				// Give a warning but proceed as if nothing has happened
				slog.WarnContext(ctx, "numPacketsSent > numPacketsGoal",
					"sent", numPacketsSent,
					"goal", numPacketsGoal,
					"elapsed", elapsed,
					"start", start,
					"now", now)
				continue
			}
			numPackets := numPacketsGoal - numPacketsSent
			if numPackets == 0 {
				continue
			}
			n, err := r.sendRound(ctx, mconn, raddr, numPackets, params.PayloadSize)
			if err != nil {
				return err
			}
			numPacketsSent += n
		case <-ctx.Done():
			// Segment duration reached (per-segment deadline) or user abort
			return nil
		}
	}
}

func (r *RateSender) sendRound(ctx context.Context, conn *mmsg.Conn, raddr net.Addr, packets uint, payloadSize uint) (uint, error) {
	packetsLeft := packets
	sent := uint(0)
	for packetsLeft > 0 {
		// A single round can span many 1024-packet batches; observe the test
		// end between them
		if ctx.Err() != nil {
			return sent, nil
		}
		tsSent := time.Now()
		packetsRound := packetsLeft
		if packetsRound > 1024 {
			slog.DebugContext(ctx, "UDP RateSender", "packets", packets)
			packetsRound = 1024
		}
		for i := 0; uint(i) < packetsRound; i++ {
			msg := &r.tx[i]
			b := Msg{Seq: r.seq + uint64(i), PadN: payloadSize}
			n, err := b.Encode(msg.Buffers[0])
			if err != nil {
				// r.msgs contains unsent messages
				return sent, err
			}
			msg.Buffers[0] = msg.Buffers[0][:n]
			msg.Addr = raddr

			r.msgs = append(r.msgs, MsgSent{Seq: b.Seq, TsSent: tsSent, Len: 8 + payloadSize})
		}

		n, err := conn.SendMsgs(r.tx[:packetsRound], 0)
		if err != nil {
			// Nothing of this round went out (sendmmsg reports a count, not
			// an error, if it sent anything); drop the messages appended above
			r.msgs = r.msgs[:len(r.msgs)-int(packetsRound)]
			if e, ok := err.(net.Error); ok && e.Timeout() {
				return sent, nil
			}
			return sent, err
		}
		if n < 0 {
			panic(fmt.Sprintf("SendMsgs returned n < 0, n=%v", n))
		}
		sent += uint(n)
		r.seq += uint64(n)
		packetsLeft -= uint(n)
		if n != int(packetsRound) {
			slog.WarnContext(ctx, fmt.Sprintf("Only sent %v packets instead of %v", n, packetsRound))
			r.msgs = r.msgs[:len(r.msgs)-(int(packetsRound)-n)]
			r.seq -= uint64(packetsRound) - uint64(n)
			break
		}
	}
	return sent, nil
}
