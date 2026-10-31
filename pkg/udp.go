package pkg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
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
	Config   TimestampConfig
	report   TimestampDirectionReport
	prepared bool
}

func (r *UDPReceiver) TimestampReport() TimestampDirectionReport { return r.report }

func (r *UDPReceiver) Init() {
}

// prepare enables the one configured kernel RX source before a measurement
// can deliver its first data packet. A direct Run call prepares lazily before
// its first read; network callers prepare before their existing kickoff.
func (r *UDPReceiver) prepare(conn *net.UDPConn, config TimestampConfig) error {
	return r.prepareWith(conn, config, func(conn *net.UDPConn, source TimestampSource) error {
		return enableRxTimestampingSource(conn, source)
	})
}

func (r *UDPReceiver) prepareWith(conn *net.UDPConn, config TimestampConfig, enable func(*net.UDPConn, TimestampSource) error) error {
	r.prepared = false
	config, err := config.Normalize()
	if err != nil {
		r.report = TimestampDirectionReport{}
		return err
	}
	selection, err := selectTimestampSource(context.Background(), config.RX, false)
	if err != nil {
		r.Config = config
		r.report = selection
		return err
	}
	r.Config = config
	r.report = selection
	if selection.Source != TimestampApp {
		if err := enable(conn, selection.Source); err != nil {
			return fmt.Errorf("could not enable selected %s RX timestamps: %w", selection.Source, err)
		}
	}
	r.prepared = true
	return nil
}

// Receives until ctx is cancelled. ReadDeadline = time.Now() is set to wake up and return from ReceiveFrom.
func (r *UDPReceiver) Run(ctx context.Context, ln net.Listener) (any, error) {
	config, err := r.Config.Normalize()
	if err != nil {
		r.report = TimestampDirectionReport{}
		return nil, err
	}
	r.Config = config
	r.report.PacketCount = 0
	r.report.MissingCount = 0
	r.report.InvalidCount = 0

	// DummyListener used
	conn, err := ln.Accept()
	if err != nil {
		if !r.prepared {
			r.report, _ = selectTimestampSource(ctx, config.RX, false)
		}
		return nil, err
	}
	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		err := fmt.Errorf("UDPReceiver requires *net.UDPConn, got %T", conn)
		if !r.prepared {
			r.report, _ = selectTimestampSource(ctx, config.RX, false)
		}
		return nil, err
	}
	if !r.prepared {
		if err := r.prepare(udpConn, config); err != nil {
			return nil, err
		}
	}
	source := r.report.Source

	// conn ReadDeadline must be set, otherwise this function never returns.
	msgs := make([]MsgRcvd, 0, 1024)
	var firstTimestampErr error

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
				readErr := fmt.Errorf("ReceiveFrom: ReadMsgs: %w", err)
				if firstTimestampErr != nil {
					readErr = errors.Join(readErr, firstTimestampErr)
				}
				return nil, readErr
			}
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "RecvMsgs returned Timeout error from unknown origin -> clearing deadline",
					"error", err,
					"ctxErr", ctx.Err())
				conn.SetReadDeadline(time.Time{})
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
			if packet.N < 8 || len(packet.Buffers) == 0 || packet.N > len(packet.Buffers[0]) {
				payloadErr := fmt.Errorf("invalid UDP payload length %d", packet.N)
				r.report.PacketCount++
				r.report.MissingCount++
				r.report.InvalidCount++
				if firstTimestampErr != nil {
					payloadErr = errors.Join(payloadErr, firstTimestampErr)
				}
				return nil, payloadErr
			}

			var msg Msg
			msg.Decode(packet.Buffers[0][:packet.N])
			rcvd := MsgRcvd{
				Seq: msg.Seq,
				Len: uint(packet.N),
			}
			r.report.PacketCount++
			if source == TimestampApp {
				rcvd.TsRcvd = tsRcvd
			} else {
				timestamp, timestampErr := parseRxTimestampOOB(packet, source)
				if timestampErr != nil {
					r.report.MissingCount++
					r.report.InvalidCount++
					if firstTimestampErr == nil {
						firstTimestampErr = fmt.Errorf("invalid %s RX timestamp for packet %d: %w", source, msg.Seq, timestampErr)
					}
					slog.WarnContext(ctx, "RX timestamp invalid", "seq", msg.Seq, "reason", timestampErr)
				} else {
					rcvd.TsRcvd = timestamp
				}
			}
			if rcvd.TsRcvd.IsZero() && source == TimestampApp {
				r.report.MissingCount++
			}
			msgs = append(msgs, rcvd)
		}
	}

	if r.report.MissingCount != 0 {
		if firstTimestampErr == nil {
			firstTimestampErr = fmt.Errorf("selected %s RX timestamps are missing for %d of %d packets", source, r.report.MissingCount, r.report.PacketCount)
		}
		return msgs, firstTimestampErr
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
	Params          BurstParams
	TimestampSource TimestampSource
	report          TimestampDirectionReport
}

func (s *BurstSender) TimestampReport() TimestampDirectionReport { return s.report }

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
	tsReader, tsReaderCancel, selection, err := beginTxTimestampCollection(ctx, conn.(*net.UDPConn), s.Params.Num, s.TimestampSource)
	s.report = selection
	if err != nil {
		return nil, err
	}
	defer tsReaderCancel()

	msgsSent, runErr := s.run(ctx, conn, raddr)
	msgsSent, s.report, err = selectTxTimestamps(ctx, tsReader, tsReaderCancel, msgsSent, selection)
	if runErr != nil {
		if err != nil {
			runErr = errors.Join(runErr, err)
		}
		return nil, runErr
	}
	if err != nil {
		return nil, err
	}

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
	Params          PeriodicParams
	TimestampSource TimestampSource
	report          TimestampDirectionReport
}

func (s *PeriodicSender) TimestampReport() TimestampDirectionReport { return s.report }

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
	tsReader, tsReaderCancel, selection, err := beginTxTimestampCollection(ctx, conn.(*net.UDPConn), r.Params.NumPackets(), r.TimestampSource)
	r.report = selection
	if err != nil {
		return nil, err
	}
	defer tsReaderCancel()

	msgsSent, runErr := r.run(ctx, conn, raddr)
	msgsSent, r.report, err = selectTxTimestamps(ctx, tsReader, tsReaderCancel, msgsSent, selection)
	if runErr != nil {
		if err != nil {
			runErr = errors.Join(runErr, err)
		}
		return nil, runErr
	}
	if err != nil {
		return nil, err
	}

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
	if r.Duration <= 0 {
		return 0
	}
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
		duration += max(0, r[i].GetDuration())
	}
	return duration
}

// Generate Params.Pps many packets per second
type RateSender struct {
	Params          RateParamsW
	TimestampSource TimestampSource
	seq             uint64
	msgs            []MsgSent
	tx              []mmsg.Message
	report          TimestampDirectionReport
}

func (r *RateSender) TimestampReport() TimestampDirectionReport { return r.report }

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
	start := time.Now()
	r.msgs = make([]MsgSent, 0, int(float64(r.Params.NumPackets())*1.1))
	r.tx = make([]mmsg.Message, 1024)
	for i := range r.tx {
		r.tx[i].Buffers = [][]byte{make([]byte, 1500)}
	}

	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return nil, fmt.Errorf("Failed mmsg.NewConn: %w", err)
	}
	tsReader, tsReaderCancel, selection, err := beginTxTimestampCollection(ctx, conn.(*net.UDPConn), uint(cap(r.msgs)), r.TimestampSource)
	r.report = selection
	if err != nil {
		return nil, err
	}
	defer tsReaderCancel()

	runErr := r.runSegments(ctx, conn, mconn, raddr, start)
	r.msgs, r.report, err = selectTxTimestamps(ctx, tsReader, tsReaderCancel, r.msgs, selection)
	if runErr != nil {
		if err != nil {
			runErr = errors.Join(runErr, err)
		}
		return nil, runErr
	}
	if err != nil {
		return nil, err
	}

	return r.msgs, nil
}

// rateInterval returns the ticker interval for a segment paced at pps:
// short enough that a tick asks for at most ~5 packets, but no longer than
// 1ms so low rates still observe the segment end promptly.
func rateInterval(pps uint) time.Duration {
	const maxPacketsPerBurst = 5
	calcInterval := time.Duration((maxPacketsPerBurst / float64(pps)) * 1e9)
	return max(time.Nanosecond, min(calcInterval, 1*time.Millisecond))
}

// numPacketsGoal returns how many packets in total should have left the
// socket elapsed time into a segment paced at pps: exactly ⌊elapsed·pps⌋.
// Integer math, so it is exact (overflows only past elapsed·pps ≈ 1.8e19,
// far beyond any real segment).
func numPacketsGoal(elapsed time.Duration, pps uint) uint {
	if elapsed <= 0 {
		return 0
	}
	return uint(uint64(elapsed) * uint64(pps) / uint64(time.Second))
}

// packetsDue returns how many packets a tick at elapsed must send given
// what was already sent. The second return reports the sent-ahead-of-goal
// anomaly (nothing is due then; the caller logs it).
func packetsDue(elapsed time.Duration, pps uint, alreadySent uint) (uint, bool) {
	goal := numPacketsGoal(elapsed, pps)
	if alreadySent > goal {
		return 0, true
	}
	return goal - alreadySent, false
}

func (r *RateSender) runParams(ctx context.Context, conn net.Conn, raddr net.Addr, params RateParams) error {
	start := time.Now()
	if params.Duration <= 0 || rateContextExpired(ctx, start) {
		return nil
	}
	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return fmt.Errorf("Failed mmsg.NewConn: %w", err)
	}
	return r.runSegment(ctx, conn, mconn, raddr, params, start, start.Add(max(0, params.Duration)))
}

type rateMessageSender interface {
	SendMsgs([]mmsg.Message, int) (int, error)
}

type rateWriteDeadliner interface {
	SetWriteDeadline(time.Time) error
}

// A context's timer callback may run late. Check its actual deadline as well
// as Err before sending, including between batches in a catch-up round.
func rateContextExpired(ctx context.Context, now time.Time) bool {
	if ctx.Err() != nil {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !now.Before(deadline)
}

func (r *RateSender) runSegments(ctx context.Context, conn rateWriteDeadliner, sender rateMessageSender, raddr net.Addr, start time.Time) error {
	boundary := start
	for i, params := range r.Params {
		if rateContextExpired(ctx, time.Now()) {
			break
		}
		if params.Duration <= 0 {
			continue
		}
		end := boundary.Add(params.Duration)
		if !time.Now().Before(end) {
			boundary = end
			continue
		}
		slog.DebugContext(ctx, "RateSender segment start", "segment", i)
		if err := r.runSegment(ctx, conn, sender, raddr, params, boundary, end); err != nil {
			return err
		}
		boundary = end
	}
	return nil
}

// Drop whole-packet demand missed before entering a segment, but retain the
// existing cumulative catch-up policy once it is live. Neither debt nor
// fractional credit crosses a segment boundary, even between equal rates.
func rateSegmentGoal(now, start, end time.Time, pps, missed uint) uint {
	goal := numPacketsGoal(min(now.Sub(start), end.Sub(start)), pps)
	return goal - min(goal, missed)
}

func (r *RateSender) runSegment(ctx context.Context, conn rateWriteDeadliner, sender rateMessageSender, raddr net.Addr, params RateParams, start, end time.Time) error {
	now := time.Now()
	if params.Duration <= 0 || !now.Before(end) || rateContextExpired(ctx, now) {
		return nil
	}
	missed := numPacketsGoal(now.Sub(start), params.Pps)
	ctx, cancel := context.WithDeadline(ctx, end)
	defer cancel()
	deadline, _ := ctx.Deadline() // includes an earlier outer deadline
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	// Joining the callback before clearing the deadline prevents a late
	// cancellation wakeup from poisoning the next segment (or conn reuse).
	woke := make(chan struct{})
	stopWake := context.AfterFunc(ctx, func() {
		defer close(woke)
		conn.SetWriteDeadline(time.Now())
	})
	defer func() {
		if !stopWake() {
			<-woke
		}
		conn.SetWriteDeadline(time.Time{})
	}()

	if params.Pps == 0 {
		<-ctx.Done()
		return nil
	}
	interval := rateInterval(params.Pps)
	slog.DebugContext(ctx, "Dynamic interval calculation", "interval", interval.String())
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	numPacketsSent := uint(0)

	for {
		select {
		case <-ticker.C:
			now := time.Now() // not the potentially stale ticker timestamp
			if rateContextExpired(ctx, now) {
				return nil
			}
			goal := rateSegmentGoal(now, start, end, params.Pps, missed)
			if numPacketsSent >= goal {
				continue
			}
			n, err := r.sendRound(ctx, sender, raddr, goal-numPacketsSent, params.PayloadSize)
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

func (r *RateSender) sendRound(ctx context.Context, conn interface {
	SendMsgs([]mmsg.Message, int) (int, error)
}, raddr net.Addr, packets uint, payloadSize uint) (uint, error) {
	packetsLeft := packets
	sent := uint(0)
	for packetsLeft > 0 {
		// A single round can span many 1024-packet batches; observe the test
		// end between them
		if rateContextExpired(ctx, time.Now()) {
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
			// The previous packet shortened the slice; encode using the full
			// capacity so later segments can grow their payload again.
			n, err := b.Encode(msg.Buffers[0][:cap(msg.Buffers[0])])
			if err != nil {
				// r.msgs contains unsent messages
				return sent, err
			}
			msg.Buffers[0] = msg.Buffers[0][:n]
			msg.Addr = raddr

			r.msgs = append(r.msgs, MsgSent{Seq: b.Seq, TsSent: tsSent, Len: 8 + payloadSize})
		}

		if rateContextExpired(ctx, time.Now()) {
			r.msgs = r.msgs[:len(r.msgs)-int(packetsRound)]
			return sent, nil
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
			// r.seq already advanced by the number actually sent above.
			break
		}
	}
	return sent, nil
}
