package pkg

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/net/ipv4"
	"log"
	"net"
	"syscall"
	"time"
)

type UdpServerMode int

const (
	Receive UdpServerMode = iota
	SendBurst
	SendRate
	SendPeriodic
)

type Sender interface {
	GetParams() SenderParams
	Mode() UdpServerMode
	Run(conn *ipv4.PacketConn, raddr *net.UDPAddr) ([]MsgSent, error)
}

type SenderParams interface {
	NumPackets() uint
	GetDuration() time.Duration
}

// UDP packet
type Msg struct {
	Seq uint64
	Pad []byte
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
	n := copy(buf[8:], m.Pad)
	if n < len(m.Pad) {
		return 8, fmt.Errorf("Provided buffer was too small")
	}
	return 8 + n, nil
}

func (m *Msg) Decode(buf []byte) {
	m.Seq = binary.BigEndian.Uint64(buf[0:])
}

func OpenUdpPacketConn(ip string, port uint) (*ipv4.PacketConn, *net.UDPAddr, *net.UDPAddr, error) {
	raddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%v", ip, port))
	if err != nil {
		return nil, nil, nil, err
	}

	udpConn, err := net.DialUDP("udp", nil, raddr)
	if err != nil {
		return nil, nil, nil, err
	}

	setSocketBuffers(udpConn)

	return ipv4.NewPacketConn(udpConn), udpConn.LocalAddr().(*net.UDPAddr), raddr, nil
}

func setSocketBuffers(conn *net.UDPConn) {
	udpBufferSize := 15_000_000
	errWrite := conn.SetWriteBuffer(udpBufferSize)
	errRead := conn.SetReadBuffer(udpBufferSize)
	sndBuf, rcvBuf, errGet := getSocketBuffer(conn)
	if errWrite != nil || errRead != nil || errGet != nil || sndBuf < udpBufferSize || rcvBuf < udpBufferSize {
		log.Printf("Failed to set the UDP buffers to %v; snd=%v rcv=%v: %v %v %v\n",
			udpBufferSize, sndBuf, rcvBuf, errWrite, errRead, errGet)
	}
}

func getSocketBuffer(conn *net.UDPConn) (int, int, error) {
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

func receiveFromSingle(conn *ipv4.PacketConn, num uint) []MsgRcvd {
	msgs := make([]MsgRcvd, 0, num)

	for {
		var buf [1500]byte
		log.Printf("Blocking on ReadFrom...")
		n, _, _, err := conn.ReadFrom(buf[0:])
		if err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				// not a timeout
				log.Printf("receiveFrom: ReadFrom: %v", err.Error())
				break
			}
			break
		}
		log.Printf("Received n=%v\n", n)

		var msg Msg
		msg.Decode(buf[:n])
		rcvd := MsgRcvd{
			Seq:    msg.Seq,
			TsRcvd: time.Now(),
			Len:    uint(n),
		}
		msgs = append(msgs, rcvd)

	}
	log.Printf("Received %v messages\n", len(msgs))

	return msgs
}

func receiveFrom(conn *ipv4.PacketConn, num uint) ([]MsgRcvd, error) {
	// conn ReadDeadline must be set, otherwise this function never returns
	msgs := make([]MsgRcvd, 0, num)

	// batch size
	rx := make([]ipv4.Message, 1024)
	for k := range rx {
		rx[k].Buffers = [][]byte{make([]byte, 1500)}
	}

	for {
		n, err := conn.ReadBatch(rx, 0)
		if err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				// not a timeout
				return nil, fmt.Errorf("receiveFrom: ReadBatch: %v\n", err.Error())
			}
			break
		}

		tsRcvd := time.Now()

		for i := 0; i < n; i++ {
			var msg Msg
			packet := rx[i]
			if packet.N == 0 {
				continue
			}
			msg.Decode(packet.Buffers[0][:packet.N])
			rcvd := MsgRcvd{
				Seq:    msg.Seq,
				TsRcvd: tsRcvd,
				Len:    uint(packet.N),
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

func (s *BurstSender) Mode() UdpServerMode {
	return SendBurst
}

func (s *BurstSender) Run(conn *ipv4.PacketConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	msgsSent := make([]MsgSent, s.Params.Num)
	msgs := make([]ipv4.Message, s.Params.Num)

	for i := 0; i < int(s.Params.Num); i++ {
		b := Msg{Seq: uint64(i)}
		if s.Params.Pad > 0 {
			b.Pad = make([]byte, s.Params.Pad)
		}
		buf := make([]byte, 1500)
		n, err := b.Encode(buf)
		if err != nil {
			return nil, err
		}

		msgs[i].Buffers = append(msgs[i].Buffers, buf[:n])
		msgs[i].Addr = raddr

		msgsSent[i] = MsgSent{Seq: b.Seq, Len: 8 + s.Params.Pad}
	}

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

		n, err := conn.WriteBatch(tx, 0)
		if n != int(numRound) {
			log.Printf("Only bursted %v packets instead of %v in round %v", n, numRound, round)
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

func (s *PeriodicSender) Mode() UdpServerMode {
	return SendPeriodic
}

func (r *PeriodicSender) Run(conn *ipv4.PacketConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	msgsSent := make([]MsgSent, 0, r.Params.NumPackets())

	ticker := time.NewTicker(r.Params.Interval)
	duration := time.After(r.Params.Duration)

	seq := 0
	for {
		select {
		case <-ticker.C:
			// TODO: add pad
			b := Msg{Seq: uint64(seq)}
			if r.Params.Pad > 0 {
				b.Pad = make([]byte, r.Params.Pad)
			}

			buf := make([]byte, 1500)
			n, err := b.Encode(buf)
			if err != nil {
				return nil, err
			}
			buf = buf[:n]

			msgsSent = append(msgsSent, MsgSent{Seq: b.Seq, TsSent: time.Now(), Len: 8 + r.Params.Pad})

			nConn, err := conn.WriteTo(buf, nil, raddr)
			if err != nil {
				return nil, err
			}
			if nConn != len(buf) {
				log.Printf("Not written the entire buffer: %v != %v", n, nConn)
			}

			seq += 1
		case <-duration:
			return msgsSent, nil
		}
	}
}

type RateParams struct {
	RateMbps    float64
	Interval    time.Duration
	Duration    time.Duration
	PayloadSize uint
}

var _ SenderParams = (*RateParams)(nil)

func (r RateParams) NumPackets() uint {
	return uint(float64(uint(r.RateMbps*float64(1e6))/8/r.PayloadSize) * r.Duration.Seconds())
}

func (r RateParams) GetDuration() time.Duration {
	return r.Duration
}

// Tries to generate packets to match RateMbps
type RateSender struct {
	Params RateParams
	seq    uint64
	msgs   []MsgSent
}

var _ Sender = (*RateSender)(nil)

func (r *RateSender) GetParams() SenderParams {
	return r.Params
}

func (s *RateSender) Mode() UdpServerMode {
	return SendRate
}

func (r *RateSender) Run(conn *ipv4.PacketConn, raddr *net.UDPAddr) ([]MsgSent, error) {
	ticker := time.NewTicker(r.Params.Interval)
	duration := time.After(r.Params.Duration)

	bytesPerInterval := uint(r.Params.RateMbps*float64(1e6)) / 8 / 1000 * uint(r.Params.Interval.Milliseconds())
	log.Printf("Sending %v Bytes per interval", bytesPerInterval)

	r.msgs = make([]MsgSent, 0, r.Params.NumPackets())

	for {
		select {
		case <-ticker.C:
			if err := r.sendRound(conn, raddr, bytesPerInterval); err != nil {
				return nil, err
			}
		case <-duration:
			return r.msgs, nil
		}
	}
}

func (r *RateSender) sendRound(conn *ipv4.PacketConn, raddr *net.UDPAddr, numBytes uint) error {
	sendPackets := numBytes / r.Params.PayloadSize
	lastPacketSize := numBytes % r.Params.PayloadSize
	if lastPacketSize > 0 {
		sendPackets += 1
	}

	for sendPackets > 0 {
		tsSent := time.Now()
		numPackets := sendPackets
		if sendPackets > 1024 {
			numPackets = 1024
			lastPacketSize = 0 // don't worry about this if the rate is that high
		}
		tx := make([]ipv4.Message, numPackets)
		for i := uint64(0); i < uint64(numPackets); i++ {
			msg := ipv4.Message{}
			padSize := r.Params.PayloadSize
			if i == uint64(numPackets-1) && lastPacketSize != 0 {
				padSize = lastPacketSize
			}
			b := Msg{Seq: r.seq + i, Pad: make([]byte, padSize)}
			buf := make([]byte, 1500)
			n, err := b.Encode(buf)
			if err != nil {
				return err
			}

			msg.Buffers = append(msg.Buffers, buf[:n])
			msg.Addr = raddr
			tx[i] = msg

			r.msgs = append(r.msgs, MsgSent{Seq: b.Seq, TsSent: tsSent, Len: 8 + padSize})
		}

		n, err := conn.WriteBatch(tx, 0)
		if err != nil {
			return err
		}
		r.seq += uint64(n)
		sendPackets -= uint(n)
		if n != int(numPackets) {
			log.Printf("Only sent %v packets instead of %v (len tx=%v)\n", n, numPackets, len(tx))
			r.msgs = r.msgs[:len(r.msgs)-(int(numPackets)-n)]
			break
		}
	}
	return nil
}
