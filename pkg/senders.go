package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"syscall"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

type UDPServerMode int

const (
	Receive UDPServerMode = iota
	SendBurst
	SendRate
	SendPeriodic
)

func (m UDPServerMode) String() string {
	switch m {
	case Receive:
		return "receive"
	case SendBurst:
		return "burst"
	case SendRate:
		return "rate"
	case SendPeriodic:
		return "periodic"
	default:
		panic(fmt.Sprintf("Unknown UDPServerMode '%d'", m))
	}
}

type Sender interface {
	GetParams() SenderParams
	Mode() UDPServerMode
	Run(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr) ([]MsgSent, error)
}

type SenderParams interface {
	NumPackets() uint
	GetDuration() time.Duration
}

// UDP packet
type Msg struct {
	Seq  uint64
	PadN uint
}

// Stores sent and received messages
type MsgSent struct {
	Seq    uint64
	TsSent time.Time
	Len    uint
}
type MsgRcvd struct {
	Seq    uint64
	TsRcvd time.Time
	Len    uint
}

func (m *Msg) Encode(buf []byte) (int, error) {
	binary.BigEndian.PutUint64(buf[0:], m.Seq)
	if len(buf) < int(8+m.PadN) {
		return 8, fmt.Errorf("Provided buffer too small to add %v padding bytes", m.PadN)
	}
	rand.Read(buf[8 : 8+m.PadN]) // always succeeds
	return int(8 + m.PadN), nil
}

func (m *Msg) Decode(buf []byte) {
	m.Seq = binary.BigEndian.Uint64(buf[0:])
}

func ListenUDP(ctx context.Context) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, fmt.Errorf("net.ResolveUDPAddr failed: %v", err.Error())
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, fmt.Errorf("net.ListenUDP failed: %v\n", err.Error())
	}

	setSocketBuffers(ctx, conn)

	return conn, nil
}

func ReceiveFrom(ctx context.Context, conn *net.UDPConn, expectedNumPackets uint) ([]MsgRcvd, error) {
	tsEnabled := true
	if err := enableRxTimestamping(conn); err != nil {
		slog.WarnContext(ctx, "Failed enabling rx timestamping", "error", err.Error())
		tsEnabled = false
	}

	// conn ReadDeadline must be set, otherwise this function never returns
	msgs := make([]MsgRcvd, 0, int(float64(expectedNumPackets)*1.1))

	// batch size
	rx := make([]mmsg.Message, 1024)
	for k := range rx {
		rx[k].Buffers = [][]byte{make([]byte, 1500)}
		rx[k].OOB = make([]byte, 500)
	}

	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return nil, fmt.Errorf("Failed mmsg.NewConn: %v", err.Error())
	}

	// TODO: close connection on ctx.Done?

	for {
		n, err := mconn.RecvMsgs(rx, 0)
		if err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				// not a timeout
				return nil, fmt.Errorf("receiveFrom: ReadBatch: %v\n", err.Error())
			}
			break
		}

		tsRcvd := time.Now()

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

				for _, cm := range cms {
					if cm.Header.Level == syscall.SOL_SOCKET && cm.Header.Type == syscall.SCM_TIMESTAMPING {
						var times unix.ScmTimestamping
						tsBuf := bytes.NewReader(cm.Data)
						binary.Read(tsBuf, binary.LittleEndian, &times)
						ts := times.Ts[0]
						rcvd.TsRcvd = time.Unix(ts.Sec, ts.Nsec)
					}
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

func (b BurstParams) NumPackets() uint {
	return b.Num
}

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

func (s *BurstSender) Mode() UDPServerMode {
	return SendBurst
}

func (s *BurstSender) Run(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	tsReader, tsReaderCancel := startTxTsReader(ctx, conn)
	defer tsReaderCancel()

	msgsSent, err := s.run(ctx, conn, raddr)
	if err != nil {
		return nil, err
	}

	msgsSent = selectMsgsSent(ctx, tsReader, tsReaderCancel, msgsSent)

	return msgsSent, nil
}

func (s *BurstSender) run(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr) ([]MsgSent, error) {
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
		return nil, fmt.Errorf("Failed mmsg.NewConn: %v", err.Error())
	}

	sentN := uint(0)
	for sentN < s.Params.Num {
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
			return nil, err
		}
		if n != int(numRound) {
			break
		}
		sentN += uint(n)
	}

	return msgsSent, nil
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

func (s *PeriodicSender) Mode() UDPServerMode {
	return SendPeriodic
}

func (r *PeriodicSender) Run(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	tsReader, tsReaderCancel := startTxTsReader(ctx, conn)
	defer tsReaderCancel()

	msgsSent, err := r.run(ctx, conn, raddr)
	if err != nil {
		return nil, err
	}

	msgsSent = selectMsgsSent(ctx, tsReader, tsReaderCancel, msgsSent)

	return msgsSent, nil
}
func (r *PeriodicSender) run(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	msgsSent := make([]MsgSent, 0, r.Params.NumPackets())

	ticker := time.NewTicker(r.Params.Interval)
	duration := time.After(r.Params.Duration)
	buf := make([]byte, 1500)

	// TODO: close socket on ctx.Done?

	seq := 0
	for {
		select {
		case <-ticker.C:
			// TODO: add pad
			b := Msg{Seq: uint64(seq), PadN: r.Params.Pad}
			n, err := b.Encode(buf)
			if err != nil {
				return nil, err
			}
			read := buf[:n]

			msgsSent = append(msgsSent, MsgSent{Seq: b.Seq, TsSent: time.Now(), Len: 8 + r.Params.Pad})

			nConn, err := conn.WriteTo(read, raddr)
			if err != nil {
				return nil, err
			}
			if nConn != len(read) {
				slog.WarnContext(ctx, fmt.Sprintf("Not written the entire buffer: %v != %v", n, nConn))
			}

			seq += 1
		case <-duration:
			return msgsSent, nil
		}
	}
}

type RateParams struct {
	Pps         uint
	Interval    time.Duration
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

func (s *RateSender) Mode() UDPServerMode {
	return SendRate
}

func (r *RateSender) Run(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	r.msgs = make([]MsgSent, 0, int(float64(r.Params.NumPackets())*1.1))
	r.tx = make([]mmsg.Message, 1024)
	for i := range r.tx {
		r.tx[i].Buffers = [][]byte{make([]byte, 1500)}
	}

	tsReader, tsReaderCancel := startTxTsReader(ctx, conn)
	defer tsReaderCancel()

	for i := range r.Params {
		if err := r.runParams(ctx, conn, raddr, r.Params[i], tsReader != nil); err != nil {
			return nil, err
		}
	}

	r.msgs = selectMsgsSent(ctx, tsReader, tsReaderCancel, r.msgs)

	return r.msgs, nil
}

func (r *RateSender) runParams(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr, params RateParams, tsEnabled bool) error {
	// Only enable socket pacing if we can retrieve the actual tx timestamps
	// TODO: reenable
	// leads to txtsreader returning a different number of packets than Run (half of it)
	// if tsEnabled {
	// 	// approximate 50 bytes overhead from L2, IP and UDP headers
	// 	pacingRate := uint(params.Pps*params.PayloadSize) + 50
	// 	if err := setMaxPacingRate(conn, pacingRate); err != nil {
	// 		slog.WarnContext(ctx, "Failed enabling pacing", "error", err)
	// 	} else {
	// 		slog.DebugContext(ctx, "Enabled pacing")
	// 	}
	// }
	slog.DebugContext(ctx, "Disabled pacing (manually)")

	start := time.Now() // must appear before ticker is started
	ticker := time.Tick(params.Interval)
	duration := time.After(params.Duration)

	numPacketsSent := uint(0)

	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return fmt.Errorf("Failed mmsg.NewConn: %v", err.Error())
	}

	for {
		select {
		case now := <-ticker:
			// elapsed := max(0, time.Since(start).Seconds())
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
				slog.ErrorContext(ctx, "numPacketsSent > numPacketsGoal",
					"sent", numPacketsSent,
					"goal", numPacketsGoal,
					"elapsed", elapsed,
					"start", start,
					"now", now)
				panic("numPacketsSent > numPacketsGoal")
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
		case <-duration:
			return ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *RateSender) sendRound(ctx context.Context, conn *mmsg.Conn, raddr *net.UDPAddr, packets uint, payloadSize uint) (uint, error) {
	packetsLeft := packets
	sent := uint(0)
	for packetsLeft > 0 {
		tsSent := time.Now()
		packetsRound := packetsLeft
		if packetsRound > 1024 {
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
	return sent, ctx.Err()
}
