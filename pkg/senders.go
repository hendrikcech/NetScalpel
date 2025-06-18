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

	"golang.org/x/net/ipv4"
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

func setSocketBuffers(ctx context.Context, conn *net.UDPConn) {
	udpBufferSize := 15_000_000
	sndPrev, rcvPrev, _ := getSocketBuffers(conn)
	errWrite := conn.SetWriteBuffer(udpBufferSize)
	errRead := conn.SetReadBuffer(udpBufferSize)
	sndBuf, rcvBuf, errGet := getSocketBuffers(conn)
	if errWrite != nil || errRead != nil || errGet != nil || sndBuf < udpBufferSize || rcvBuf < udpBufferSize {
		errForce := forceSetSocketBuffers(conn, udpBufferSize)
		sndBuf, rcvBuf, errGet = getSocketBuffers(conn)
		if sndBuf < udpBufferSize || rcvBuf < udpBufferSize {
			slog.WarnContext(ctx, "Failed setting UDP socket buffers",
				"goal", udpBufferSize,
				"sndPrev", sndPrev,
				"sndNow", sndBuf,
				"rcvPrev", rcvPrev,
				"rcvNow", rcvBuf,
				"errorWrite", errWrite,
				"errorGet", errGet,
				"errorForce", errForce)
		}
	}
}

func getSocketBuffers(conn *net.UDPConn) (int, int, error) {
	fd, err := conn.File()
	defer fd.Close()
	if err != nil {
		return -1, -1, err
	}
	// Necessary to continue make Deadline work on conn: https://stackoverflow.com/a/74886460
	defer syscall.SetNonblock(int(fd.Fd()), true)

	snd, err := syscall.GetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
	if err != nil {
		return -1, -1, err
	}
	rcv, err := syscall.GetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	if err != nil {
		return -1, -1, err
	}

	return snd, rcv, nil
}

func forceSetSocketBuffers(conn *net.UDPConn, size int) error {
	fd, err := conn.File()
	defer fd.Close()
	if err != nil {
		return err
	}
	// Necessary to continue make Deadline work on conn: https://stackoverflow.com/a/74886460
	defer syscall.SetNonblock(int(fd.Fd()), true)

	if err := syscall.SetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_SNDBUFFORCE, size); err != nil {
		return err
	}
	if err := syscall.SetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_RCVBUFFORCE, size); err != nil {
		return err
	}

	return nil
}

func setMaxPacingRate(conn *net.UDPConn, rate uint) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Control(func(fd uintptr) {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_MAX_PACING_RATE, int(rate)); err != nil {
			err = fmt.Errorf("Failed setting SO_MAX_PACING_RATE: %v", err.Error())
		}
	})

	return err
}

func enableTxTimestamping(conn *net.UDPConn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Control(func(fd uintptr) {
		flags := unix.SOF_TIMESTAMPING_TX_SOFTWARE |
			unix.SOF_TIMESTAMPING_TX_HARDWARE |
			unix.SOF_TIMESTAMPING_SOFTWARE |
			unix.SOF_TIMESTAMPING_RAW_HARDWARE |
			unix.SOF_TIMESTAMPING_OPT_ID
		// unix.SOF_TIMESTAMPING_OPT_TSONLY // needed to determine size of packet

		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPING, flags)
	})

	return err
}

func enableRxTimestamping(conn *net.UDPConn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Control(func(fd uintptr) {
		flags := unix.SOF_TIMESTAMPING_RX_SOFTWARE |
			unix.SOF_TIMESTAMPING_RX_HARDWARE |
			unix.SOF_TIMESTAMPING_SOFTWARE |
			unix.SOF_TIMESTAMPING_RAW_HARDWARE |
			unix.SOF_TIMESTAMPING_OPT_ID
		// unix.SOF_TIMESTAMPING_OPT_TSONLY // needed to determine size of packet

		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPING, flags)
	})

	return err
}

type TxTsReader struct {
	C        chan []MsgSent
	conn     *net.UDPConn
	deadline <-chan time.Time
}

func NewTxTsReader() *TxTsReader {
	return &TxTsReader{
		C: make(chan []MsgSent, 1),
	}
}

func (t *TxTsReader) Run(ctx context.Context, conn *net.UDPConn, duration time.Duration) error {
	t.conn = conn
	// TODO: use ctx deadline instead of the explicit duration?
	t.deadline = time.After(duration)
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	return rawConn.Control(t.run(ctx))
}

func (t *TxTsReader) run(ctx context.Context) func(uintptr) {
	return func(fd uintptr) {
		buf := make([]byte, 1500)
		oob := make([]byte, 1500)

		// TODO: size from the beginning
		sentMsgs := make([]MsgSent, 0, 1024)
		defer func() {
			t.C <- sentMsgs
		}()

		for {
			n, oobn, _, _, err := syscall.Recvmsg(int(fd), buf, oob, unix.MSG_ERRQUEUE)
			if err != nil {
				if err == syscall.EAGAIN {
					select {
					case <-time.After(100 * time.Millisecond):
						continue
					case <-t.deadline:
						break
					case <-ctx.Done():
						break
					}
				} else {
					slog.ErrorContext(ctx, "TxTsReader recvmsg error", "error", err)
				}
			}

			cms, err := unix.ParseSocketControlMessage(oob[:oobn])
			if err != nil {
				slog.ErrorContext(ctx, "TxTsReader: Failed parsing cmsg", "error", err)
				continue
			}

			msg := MsgSent{Len: uint(n)}

			tsSet := false
			seqSet := false
			for _, cm := range cms {
				if cm.Header.Level == syscall.SOL_SOCKET && cm.Header.Type == syscall.SCM_TIMESTAMPING {
					var times unix.ScmTimestamping
					tsBuf := bytes.NewReader(cm.Data)
					binary.Read(tsBuf, binary.LittleEndian, &times)
					ts := times.Ts[0]
					msg.TsSent = time.Unix(ts.Sec, ts.Nsec)
					tsSet = true
				} else if (cm.Header.Level == syscall.SOL_IP || cm.Header.Level == syscall.SOL_IPV6) &&
					(cm.Header.Type == syscall.IP_RECVERR || cm.Header.Type == syscall.IPV6_RECVERR) {
					var sockErr unix.SockExtendedErr
					sockErrBuf := bytes.NewReader(cm.Data)
					binary.Read(sockErrBuf, binary.LittleEndian, &sockErr)
					if sockErr.Errno == uint32(syscall.ENOMSG) { // expected for timestamps
						msg.Seq = uint64(sockErr.Data)
						seqSet = true
					}
				} else {
					slog.WarnContext(ctx, "TxTsReader: Unknown cm", "cm", cm)
				}
			}

			if !tsSet || !seqSet {
				slog.WarnContext(ctx, "TxTsReader: Missing data in cm",
					"ts", tsSet,
					"seq", seqSet,
					"cms", cms)
			}

			sentMsgs = append(sentMsgs, msg)
		}
	}
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
	rx := make([]ipv4.Message, 1024)
	for k := range rx {
		rx[k].Buffers = [][]byte{make([]byte, 1500)}
		rx[k].OOB = make([]byte, 500)
	}

	pconn := ipv4.NewPacketConn(conn)

	// TODO: close connection on ctx.Done?

	for {
		n, err := pconn.ReadBatch(rx, 0)
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
	var txTsReader *TxTsReader
	if err := enableTxTimestamping(conn); err != nil {
		slog.WarnContext(ctx, "Failed enabling tx timestamping", "error", err)
	} else {
		txTsReader = NewTxTsReader()
		go txTsReader.Run(ctx, conn, s.Params.GetDuration()+500*time.Millisecond)
	}

	msgsSent, err := s.run(ctx, conn, raddr)
	if err != nil {
		return nil, err
	}

	if txTsReader != nil {
		msgsSent = <-txTsReader.C
	}

	return msgsSent, nil
}

func (s *BurstSender) run(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	msgsSent := make([]MsgSent, s.Params.Num)
	msgs := make([]ipv4.Message, s.Params.Num)

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

	// TODO: close conn on ctx.Done?

	pconn := ipv4.NewPacketConn(conn)

	left := s.Params.Num
	for round := uint(0); left > 0; round++ {
		numRound := min(1024, left)
		tx := make([]ipv4.Message, numRound)
		tsSent := time.Now()

		for j := uint(0); j < numRound; j++ {
			idx := round*1024 + j
			tx[j] = msgs[idx]
			msgsSent[idx].TsSent = tsSent
		}

		n, err := pconn.WriteBatch(tx, 0)
		if n != int(numRound) {
			slog.WarnContext(ctx, fmt.Sprintf("Only bursted %v packets instead of %v in round %v", n, numRound, round))
			break
		}
		if err != nil {
			return nil, err
		}

		left -= uint(n)
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
	var txTsReader *TxTsReader
	if err := enableTxTimestamping(conn); err != nil {
		slog.WarnContext(ctx, "Failed enabling tx timestamping", "error", err.Error())
	} else {
		txTsReader = NewTxTsReader()
		go txTsReader.Run(ctx, conn, r.Params.GetDuration()+500*time.Millisecond)
	}

	msgsSent, err := r.run(ctx, conn, raddr)
	if err != nil {
		return nil, err
	}

	if txTsReader != nil {
		msgsSent = <-txTsReader.C
	}

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
	tx        []ipv4.Message
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
	r.tx = make([]ipv4.Message, 1024)
	for i := range r.tx {
		r.tx[i].Buffers = [][]byte{make([]byte, 1500)}
	}

	var txTsReader *TxTsReader
	if err := enableTxTimestamping(conn); err != nil {
		slog.WarnContext(ctx, "Failed enabling tx timestamping", "error", err.Error())
	} else {
		r.tsEnabled = true
		txTsReader = NewTxTsReader()
		go txTsReader.Run(ctx, conn, r.Params.GetDuration()+500*time.Millisecond)
	}

	for i := range r.Params {
		if err := r.runParams(ctx, conn, raddr, r.Params[i]); err != nil {
			return nil, err
		}
	}

	if r.tsEnabled {
		r.msgs = <-txTsReader.C
	}

	return r.msgs, nil
}

func (r *RateSender) runParams(ctx context.Context, conn *net.UDPConn, raddr *net.UDPAddr, params RateParams) error {
	ticker := time.Tick(params.Interval)
	duration := time.After(params.Duration)

	// Only enable socket pacing if we can retrieve the actual tx timestamps
	if r.tsEnabled {
		// approximate 50 bytes overhead from L2, IP and UDP headers
		pacingRate := uint(params.Pps*params.PayloadSize) + 50
		if err := setMaxPacingRate(conn, pacingRate); err != nil {
			slog.WarnContext(ctx, err.Error())
		}
	}

	start := time.Now()
	numPacketsSent := uint(0)

	pconn := ipv4.NewPacketConn(conn)

	for {
		select {
		case now := <-ticker:
			numPacketsGoal := uint(now.Sub(start).Seconds() * float64(params.Pps))
			if numPacketsSent > numPacketsGoal {
				panic("numPacketsSent > numPacketsGoal")
			}
			numPackets := numPacketsGoal - numPacketsSent
			if numPackets == 0 {
				continue
			}
			n, err := r.sendRound(ctx, pconn, raddr, numPackets, params.PayloadSize)
			if err != nil {
				return err
			}
			numPacketsSent += n
		case <-duration:
			return nil
		}
	}
}

func (r *RateSender) sendRound(ctx context.Context, conn *ipv4.PacketConn, raddr *net.UDPAddr, packets uint, payloadSize uint) (uint, error) {
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

		n, err := conn.WriteBatch(r.tx[:packetsRound], 0)
		if err != nil {
			return sent, err
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
