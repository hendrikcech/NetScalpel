package pkg

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Client interface {
	Run(ctx context.Context, client *rpc.Client) error
	Gather(ctx context.Context, client *rpc.Client) error
	Summary() string
}

var _ Client = (*SenderClient)(nil)
var _ Client = (*CommandClient)(nil)

// C: Written to CSV
type MsgResult struct {
	Seq    uint64
	TsSent time.Time
	TsRcvd time.Time
	Owd    time.Duration
	Len    uint
	Lost   bool
}

type SenderClient struct {
	IP        string
	Out       string
	Direction Direction
	StartAt   time.Time

	Sender Sender

	UDPMsgsSent []MsgSent
	UDPMsgsRcvd []MsgRcvd
	UDPResults  []MsgResult

	TCPMetricsSndr []TCPMetric
	TCPMetricsRcvr []TCPMetric

	ID string
}

func (c *SenderClient) Run(ctx context.Context, client *rpc.Client) error {
	if c.ID == "" {
		c.ID = GenID(c.Sender.SenderMode().String())
		ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))
	}

	switch c.Direction {
	case UL:
		return c.runUL(ctx, client)
	case DL:
		return c.runDL(ctx, client)
	default:
		panic("Unknown Direction")
	}
}

func (c *SenderClient) runUL(ctx context.Context, client *rpc.Client) error {
	args := RequestServerArgs{
		ID:         c.ID,
		Timeout:    c.Sender.GetParams().GetDuration() + time.Second,
		StartAt:    c.StartAt,
		ServerMode: c.Sender.ReceiverMode(),
		Params:     c.Sender.GetParams(),
	}
	var reply RequestServerReply
	if err := client.Call("Server.RequestServer", args, &reply); err != nil {
		return fmt.Errorf("Call Server.RequestServerReply failed: %v", err.Error())
	}

	var conn net.Conn
	var raddr net.Addr
	var err error
	switch args.ServerMode.SocketType() {
	case UDP:
		if raddr, err = net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%v", c.IP, reply.Port)); err != nil {
			return fmt.Errorf("Failed resolving provided UDP addr: %v", err.Error())
		}
		if conn, err = listenUDP(ctx); err != nil {
			return fmt.Errorf("listenUDP failed: %v", err.Error())
		}
	case TCP:
		// TCP Handshake is performed before the test starts
		if raddr, err = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%v", c.IP, reply.Port)); err != nil {
			return fmt.Errorf("Failed resolving provided TCP addr: %v", err.Error())
		}
		if conn, err = net.DialTCP("tcp", nil, raddr.(*net.TCPAddr)); err != nil {
			return fmt.Errorf("net.DialTCP failed: %v", err.Error())
		}
	case ICMP:
		conn, raddr, err = listenICMPClient(ctx, c.IP)
		if err != nil {
			return err
		}
		// Code smell. How else could this be solved?
		c.Sender.(*ICMPSender).Params.ICMPType = ipv4.ICMPTypeEcho
	default:
		panic("socket type not implemented")
	}
	defer func() {
		slog.DebugContext(ctx, "Closing conn")
		conn.Close()
	}()

	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	slog.InfoContext(ctx, "Start UL client", "type", fmt.Sprintf("%T", c.Sender),
		"params", c.Sender.GetParams(), "remoteAddr", raddr)

	sendCtx, sendCancel := context.WithTimeout(ctx, c.Sender.GetParams().GetDuration())
	defer sendCancel()

	res, err := c.Sender.Run(sendCtx, conn, raddr)
	if err != nil {
		return fmt.Errorf("UL failed: %v\n", err)
	}

	switch res.(type) {
	case []MsgSent:
		c.UDPMsgsSent = res.([]MsgSent)
	case []TCPMetric:
		c.TCPMetricsSndr = res.([]TCPMetric)
	default:
		// panic("Unhandled result type in runUL")
		slog.ErrorContext(ctx, "Unhandled result type in runUL", "result", res)
	}

	return nil
}

func (c *SenderClient) runDL(ctx context.Context, client *rpc.Client) error {
	slog.InfoContext(ctx, "Request DL server", "type", fmt.Sprintf("%T", c.Sender),
		"params", c.Sender.GetParams())

	args := RequestServerArgs{
		ID:         c.ID,
		Timeout:    c.Sender.GetParams().GetDuration() + time.Second,
		StartAt:    c.StartAt,
		ServerMode: c.Sender.SenderMode(),
		Params:     c.Sender.GetParams(),
	}
	var reply RequestServerReply
	if err := client.Call("Server.RequestServer", args, &reply); err != nil {
		return fmt.Errorf("Call Server.RequestServerReply failed: %v", err.Error())
	}

	switch args.ServerMode.SocketType() {
	case UDP:
		return c.runDLUDP(ctx, reply.Port, args.Timeout)
	case TCP:
		return c.runDLTCP(ctx, reply.Port, args.Timeout)
	case ICMP:
		return c.runDLICMP(ctx, c.Sender.GetParams().(ICMPParams).SenderEchoID, args.Timeout)
	default:
		panic("Unknown SocketType")
	}
}

func (c *SenderClient) runDLUDP(ctx context.Context, rport uint, timeout time.Duration) error {
	raddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%v", c.IP, rport))
	if err != nil {
		return fmt.Errorf("Failed resolving provided UDP addr: %v", err.Error())
	}

	conn, err := listenUDP(ctx)
	if err != nil {
		return fmt.Errorf("ListenUDP failed: %v", err.Error())
	}
	defer conn.Close()
	// laddr := conn.LocalAddr().(*net.UDPAddr)

	if err := c.punchUDPHole(ctx, conn, raddr); err != nil {
		return fmt.Errorf("Return due to failed UDP probing: %v", err.Error())
	}

	// slog.DebugContext(ctx, "Wrote UDP to server at %v, receiving at %v, timeout duration is %v\n", raddr, laddr, args.Timeout)
	var receiver Receiver
	switch c.Sender.ReceiverMode() {
	case ReceiveUDP:
		receiver = &UDPReceiver{}
	case ReceiveQUIC:
		receiver = &QUICReceiver{}
	default:
		panic("Unknown ServerMode")
	}

	receiver.Init()

	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	ln := NewDummyListener(conn, conn.LocalAddr())

	recvCtx, recvCancel := context.WithTimeout(ctx, timeout)
	defer recvCancel()
	res, err := receiver.Run(recvCtx, ln)
	if err != nil {
		return fmt.Errorf("Failed ReceiveFrom: %v", err)
	}

	switch res.(type) {
	case []MsgRcvd:
		c.UDPMsgsRcvd = res.([]MsgRcvd)
	default:
		panic("Unhandled result type in runDL")
	}

	slog.InfoContext(ctx, "Finished Run", "packets", len(c.UDPMsgsRcvd))

	return nil
}

func (c *SenderClient) runDLTCP(ctx context.Context, rport uint, timeout time.Duration) error {
	var receiver Receiver
	switch c.Sender.ReceiverMode() {
	case ReceiveTCP:
		receiver = &TCPReceiver{}
	default:
		panic("Unknown ServerMode")
	}

	receiver.Init()

	raddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%v", c.IP, rport))
	if err != nil {
		return fmt.Errorf("Failed resolving provided TCP addr: %v", err.Error())
	}

	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return fmt.Errorf("net.DialTCP failed: %v", err.Error())
	}
	defer conn.Close()

	ln := NewDummyListener(conn, conn.LocalAddr())
	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	recvCtx, recvCancel := context.WithTimeout(ctx, timeout)
	defer recvCancel()
	res, err := receiver.Run(recvCtx, ln)
	if err != nil {
		return fmt.Errorf("Failed ReceiveFrom: %v", err)
	}

	slog.InfoContext(ctx, "Finished Run")

	switch res.(type) {
	case []TCPMetric:
		c.TCPMetricsRcvr = res.([]TCPMetric)
	default:
		panic("Unhandled result type in runDL")
	}

	return nil
}

func (c *SenderClient) runDLICMP(ctx context.Context, echoID uint16, timeout time.Duration) error {
	conn, raddr, err := listenICMPClient(ctx, c.IP)
	if err != nil {
		return err
	}
	defer conn.Close()

	if err := c.punchICMPHole(ctx, conn.(net.PacketConn), raddr, echoID); err != nil {
		return fmt.Errorf("Return due to failed UDP probing: %v", err.Error())
	}

	// slog.DebugContext(ctx, "Wrote UDP to server at %v, receiving at %v, timeout duration is %v\n", raddr, laddr, args.Timeout)
	if c.Sender.ReceiverMode() != ReceiveICMP {
		panic("Unknown ServerMode")
	}

	receiver := &ICMPReceiver{
		ClientEchoID: echoID,
		ICMPType:     ipv4.ICMPTypeEchoReply,
	}
	receiver.Init()

	slog.DebugContext(ctx, "Start ICMP puncher")
	// Send one ICMP request per second to keep the NAT hole open
	// Must start before the test will start to keep NAT open!
	puncher := ICMPSender{Params: ICMPParams{
		Duration_:    time.Until(c.StartAt) + timeout,
		Interval:     1 * time.Second,
		ClientEchoID: echoID,
		SenderEchoID: echoID,
		ICMPType:     ipv4.ICMPTypeEcho,
		punch:        true,
	}}
	go func() {
		if _, err := puncher.Run(ctx, conn, raddr); err != nil {
			slog.ErrorContext(ctx, "ICMP puncher failed", "error", err)
		}
	}()

	if err := waitUntil(ctx, c.StartAt); err != nil {
		return err
	}

	ln := NewDummyListener(conn, conn.LocalAddr())
	recvCtx, recvCancel := context.WithTimeout(ctx, timeout)
	defer recvCancel()
	res, err := receiver.Run(recvCtx, ln)
	if err != nil {
		return fmt.Errorf("Failed ReceiveFrom: %v", err)
	}

	switch res.(type) {
	case []MsgRcvd:
		c.UDPMsgsRcvd = res.([]MsgRcvd)
	default:
		panic("Unhandled result type in runDL")
	}

	slog.InfoContext(ctx, "Finished Run", "packets", len(c.UDPMsgsRcvd))

	return nil
}

func (c *SenderClient) punchUDPHole(ctx context.Context, conn *net.UDPConn, raddr net.Addr) error {
	return c.punchHole(ctx, conn, raddr, []byte{})
}

func (c *SenderClient) punchICMPHole(ctx context.Context, conn net.PacketConn, raddr net.Addr, echoID uint16) error {
	echoReq := icmp.Message{
		Type: ipv4.ICMPTypeEcho,
		Code: 0,
		Body: &icmp.Echo{
			ID:   int(echoID),
			Seq:  0,
			Data: makeICMPData(echoID, true),
		},
	}
	buf, err := echoReq.Marshal(nil)
	if err != nil {
		return fmt.Errorf("Failed to marshal ICMP message: %w", err)
	}
	return c.punchHole(ctx, conn, raddr, buf)
}

func (c *SenderClient) punchHole(ctx context.Context, conn net.PacketConn, raddr net.Addr, payload []byte) error {
	probeReplyReceived := false
	maxTry := 5
	for try := range maxTry {
		// Send an UDP packet to the newly opened server UDP socket to poke
		// a hole into a potentially existing NAT and wait for the reply.
		if try > 0 {
			slog.DebugContext(ctx, "Sending NAT probe", "try", try+1, "maxTry", maxTry,
				"remoteAddr", raddr)
		}
		if _, err := conn.WriteTo(payload, raddr); err != nil {
			return fmt.Errorf("Failed WriteTo: %v\n", err.Error())
		}

		probeDeadline := time.Now().Add(time.Second)
		if !c.StartAt.IsZero() && probeDeadline.After(c.StartAt) {
			probeDeadline = c.StartAt
		}
		// TODO: replace this with context deadline?
		if err := conn.SetReadDeadline(probeDeadline); err != nil {
			return fmt.Errorf("Failed to probe deadline: %v\n", err.Error())
		}

		if !c.StartAt.IsZero() && time.Now().After(c.StartAt) {
			return fmt.Errorf("StartAt %v already passed before received probe reply at %v", c.StartAt, time.Now())
		}

		var buf [1500]byte
		if _, _, err := conn.ReadFrom(buf[:]); err != nil {
			if e, ok := err.(net.Error); !ok || !e.Timeout() {
				return fmt.Errorf("Failed ReadFrom: %v", err.Error())
			}
			// Timeout occured, send another probe
			continue
		}
		slog.DebugContext(ctx, "Received NAT probe", "try", try+1)
		probeReplyReceived = true
		// Effectively deactive ReadDeadline set for probing
		conn.SetReadDeadline(time.Now().Add(1000 * time.Hour))
		break
	}
	if !probeReplyReceived {
		return fmt.Errorf("No probe reply received")
	}
	return nil
}

func (c *SenderClient) Gather(ctx context.Context, client *rpc.Client) error {
	ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))

	slog.DebugContext(ctx, "Requesting results", "sender", fmt.Sprintf("%T", c.Sender))

	var result RequestServerResultReply
	if err := client.Call("Server.RequestServerResult",
		RequestServerResultArgs{ID: c.ID}, &result); err != nil {
		return fmt.Errorf("Call Server.RequestServerResult failed: %v", err.Error())
	}
	res := result.Result

	switch res.(type) {
	case []MsgRcvd:
		if c.Direction != UL {
			panic("Unexpected result type")
		}
		c.UDPMsgsRcvd = res.([]MsgRcvd)
	case []MsgSent:
		if c.Direction != DL {
			panic("Unexpected result type")
		}
		c.UDPMsgsSent = res.([]MsgSent)
	case []TCPMetric:
		m := res.([]TCPMetric)
		if c.Direction == UL {
			c.TCPMetricsRcvr = m
		} else {
			c.TCPMetricsSndr = m
		}
	default:
		slog.ErrorContext(ctx, "Unhandled result type in Gather", "result", res)
	}

	slog.DebugContext(ctx, "Received results", "type", fmt.Sprintf("%T", c.Sender))

	switch c.Sender.SenderMode().SocketType() {
	case UDP:
		c.UDPResults = processUDP(c.UDPMsgsSent, c.UDPMsgsRcvd)
		rows := generateUDPResultRows(c.UDPResults)
		if err := writeCSV(c.Out, rows); err != nil {
			return fmt.Errorf("writeCSV failed: %v", err.Error())
		}
	case TCP:
		sndrRows := generateTCPResultRows(c.TCPMetricsSndr)
		// rcvrRows := generateTCPResultRows(c.TCPMetricsRcvr)
		if err := writeCSV(c.Out, sndrRows); err != nil {
			return fmt.Errorf("writeCSV failed: %v", err.Error())
		}
	case ICMP:
		// TODO: change naming or merge strategy?
		c.UDPResults = processUDP(c.UDPMsgsSent, c.UDPMsgsRcvd)
		rows := generateUDPResultRows(c.UDPResults)
		// slog.DebugContext(ctx, "Results", "MSGSSENT", c.UDPMsgsSent, "MSGSRCVD", c.UDPMsgsRcvd) //, "RESULTS", c.UDPResults)
		if err := writeCSV(c.Out, rows); err != nil {
			return fmt.Errorf("writeCSV failed: %v", err.Error())
		}
	default:
		panic("Unknown SocketType")
	}

	return nil
}

func processUDP(sent []MsgSent, rcvd []MsgRcvd) []MsgResult {
	sort.Slice(rcvd, func(i, j int) bool {
		return rcvd[i].Seq < rcvd[j].Seq
	})

	resultIdx := 0
	num := uint64(len(sent))
	results := make([]MsgResult, 0, num)
	for seq := uint64(0); seq < num; seq++ {
		// slog.Debug("STA", "seq", seq, "rcvdSeq", rcvd[resultIdx].Seq, "resultIdx", resultIdx)
		var result MsgResult
		if resultIdx > len(rcvd)-1 || seq < rcvd[resultIdx].Seq {
			result = MsgResult{
				Seq:    seq,
				TsSent: sent[seq].TsSent,
				TsRcvd: time.Time{},
				Owd:    time.Duration(0),
				Len:    sent[seq].Len,
				Lost:   true,
			}
		} else if seq > rcvd[resultIdx].Seq {
			for resultIdx < len(rcvd) && seq > rcvd[resultIdx].Seq {
				slog.Warn("Received duplicate packet", "seq", rcvd[resultIdx].Seq)
				resultIdx += 1
				// slog.Debug("DUP", "seq", seq, "rcvdSeq", rcvd[resultIdx].Seq, "resultIdx", resultIdx)
			}
			seq -= 1
			continue
		} else if seq == rcvd[resultIdx].Seq {
			tsSent := sent[seq].TsSent
			result = MsgResult{
				Seq:    seq,
				TsSent: tsSent,
				TsRcvd: rcvd[resultIdx].TsRcvd,
				Owd:    rcvd[resultIdx].TsRcvd.Sub(tsSent),
				Len:    sent[seq].Len,
				Lost:   false,
			}
			resultIdx += 1
		}
		results = append(results, result)
		// rcvdSeq := -1
		// if resultIdx < len(rcvd) {
		// 	rcvdSeq = int(rcvd[resultIdx].Seq)
		// }
		// slog.Debug("ROW", "seq", seq, "rcvdSeq", rcvdSeq, "resultIdx", resultIdx, "result", result)
	}

	return results
}

func generateUDPResultRows(results []MsgResult) [][]string {
	rows := make([][]string, 1+len(results))
	rows[0] = []string{"seq", "ts_sent", "ts_rcvd", "owd_ms", "size", "lost"}

	for i := uint(0); i < uint(len(results)); i++ {
		r := results[i]

		tsRcvd := r.TsRcvd.Format(time.RFC3339Nano)
		if r.TsRcvd.IsZero() {
			tsRcvd = ""
		}

		owdStr := ""
		if !r.Lost {
			owdStr = fmtDurationMs(r.Owd)
		}

		rows[i+1] = []string{
			strconv.FormatUint(r.Seq, 10),
			r.TsSent.Format(time.RFC3339Nano),
			tsRcvd,
			owdStr,
			strconv.FormatUint(uint64(r.Len), 10),
			strconv.FormatBool(r.Lost),
		}
	}

	return rows
}

func generateTCPResultRows(results []TCPMetric) [][]string {
	rows := make([][]string, 1+len(results))
	rows[0] = []string{"Time"}

	rows[0] = append(rows[0], []string{
		"State", //             State              `json:"state"`               // connection state
		// "Options", //           []Option           `json:"opts,omitempty"`      // requesting options
		// "PeerOptions", //       []Option           `json:"peer_opts,omitempty"` // options requested from peer
		"SenderMSS",        //         MaxSegSize         `json:"snd_mss"`             // maximum segment size for sender in bytes
		"ReceiverMSS",      //       MaxSegSize         `json:"rcv_mss"`             // maximum segment size for receiver in bytes
		"RTT",              //               time.Duration      `json:"rtt"`                 // round-trip time
		"RTTVar",           //            time.Duration      `json:"rttvar"`              // round-trip time variation
		"RTO",              //               time.Duration      `json:"rto"`                 // retransmission timeout
		"ATO",              //               time.Duration      `json:"ato"`                 // delayed acknowledgement timeout [Linux only]
		"LastDataSent",     //      time.Duration      `json:"last_data_sent"`      // since last data sent [Linux only]
		"LastDataReceived", //  time.Duration      `json:"last_data_rcvd"`      // since last data received [FreeBSD and Linux]
		"LastAckReceived",  //   time.Duration      `json:"last_ack_rcvd"`       // since last ack received [Linux only]
		// "FlowControl", //       *FlowControl       `json:"flow_ctl,omitempty"`  // flow control information
		// "CongestionControl", // *CongestionControl `json:"cong_ctl,omitempty"`  // congestion control information
		// "Sys", //               *SysInfo           `json:"sys,omitempty"`       // platform-specific information
	}...)

	// FlowControl
	rows[0] = append(rows[0], []string{
		"ReceiverWindow", // uint `json:"rcv_wnd"` // advertised receiver window in bytes
	}...)

	// CongestionControl
	rows[0] = append(rows[0], []string{
		"SenderSSThreshold",   //   uint `json:"snd_ssthresh"`   // slow start threshold for sender in bytes or # of segments
		"ReceiverSSThreshold", // uint `json:"rcv_ssthresh"`   // slow start threshold for receiver in bytes [Linux only]
		"SenderWindowBytes",   //   uint `json:"snd_cwnd_bytes"` // congestion window for sender in bytes [Darwin and FreeBSD]
		"SenderWindowSegs",    //    uint `json:"snd_cwnd_segs"`  // congestion window for sender in # of segments [Linux and NetBSD]
	}...)

	// Sys
	rows[0] = append(rows[0], []string{
		"PathMTU", //                 uint          `json:"path_mtu"`           // path maximum transmission unit
		// "AdvertisedMSS", //           MaxSegSize    `json:"adv_mss"`            // advertised maximum segment size
		"CAState",                 //                 CAState       `json:"ca_state"`           // state of congestion avoidance
		"Retransmissions",         //         uint          `json:"rexmits"`            // # of retranmissions on timeout invoked
		"Backoffs",                //                uint          `json:"backoffs"`           // # of times retransmission backoff timer invoked
		"WindowOrKeepAliveProbes", // uint          `json:"wnd_ka_probes"`      // # of window or keep alive probes sent
		"UnackedSegs",             //             uint          `json:"unacked_segs"`       // # of unack'd segments
		"SackedSegs",              //              uint          `json:"sacked_segs"`        // # of sack'd segments
		"LostSegs",                //                uint          `json:"lost_segs"`          // # of lost segments
		"RetransSegs",             //             uint          `json:"retrans_segs"`       // # of retransmitting segments in transmission queue
		"ForwardAckSegs",          //          uint          `json:"fack_segs"`          // # of forward ack segments in transmission queue
		"ReorderedSegs",           //           uint          `json:"reord_segs"`         // # of reordered segments allowed
		"ReceiverRTT",             //             time.Duration `json:"rcv_rtt"`            // current RTT for receiver
		"TotalRetransSegs",        //        uint          `json:"total_retrans_segs"` // # of retransmitted segments
		"PacingRate",              //              uint64        `json:"pacing_rate"`        // pacing rate
		"ThruBytesAcked",          //          uint64        `json:"thru_bytes_acked"`   // # of bytes for which cumulative acknowledgments have been received
		"ThruBytesReceived",       //       uint64        `json:"thru_bytes_rcvd"`    // # of bytes for which cumulative acknowledgments have been sent
		"SegsOut",                 //                 uint          `json:"segs_out"`           // # of segments sent
		"SegsIn",                  //                  uint          `json:"segs_in"`            // # of segments received
		"NotSentBytes",            //            uint          `json:"not_sent_bytes"`     // # of bytes not sent yet
		"MinRTT",                  //                  time.Duration `json:"min_rtt"`            // current measured minimum RTT; zero means not available
		"DataSegsOut",             //             uint          `json:"data_segs_out"`      // # of segments sent containing a positive length data segment
		"DataSegsIn",              //              uint          `json:"data_segs_in"`       // # of segments received containing a positive length data segment
	}...)

	// BBRInfo
	rows[0] = append(rows[0], []string{
		"BBRMaxBW",          //          uint64        `json:"max_bw"`      // maximum-filtered bandwidth in bps
		"BBRMinRTT",         //         time.Duration `json:"min_rtt"`     // minimum-filtered round-trip time
		"BBRPacingGain",     //     uint          `json:"pacing_gain"` // pacing gain shifted left 8 bits
		"BBRCongWindowGain", // uint          `json:"cwnd_gain"`   // congestion window gain shifted left 8 bits
	}...)

	for j := uint(0); j < uint(len(results)); j++ {
		r := results[j]

		var maxBW, minRTT, pacingGain, congWindowGain string
		if r.BBRInfo != nil {
			maxBW = strconv.FormatUint(r.BBRInfo.MaxBW, 10)
			minRTT = fmtDurationMs(r.BBRInfo.MinRTT)
			pacingGain = strconv.FormatUint(uint64(r.BBRInfo.PacingGain), 10)
			congWindowGain = strconv.FormatUint(uint64(r.BBRInfo.CongWindowGain), 10)
		}

		i := r.Info

		rows[j+1] = []string{
			r.Time.Format(time.RFC3339Nano),

			i.State.String(),
			strconv.FormatUint(uint64(i.SenderMSS), 10),
			strconv.FormatUint(uint64(i.ReceiverMSS), 10),
			fmtDurationMs(i.RTT),
			fmtDurationMs(i.RTTVar),
			fmtDurationMs(i.RTO),
			fmtDurationMs(i.ATO),
			fmtDurationMs(i.LastDataSent),
			fmtDurationMs(i.LastDataReceived),
			fmtDurationMs(i.LastAckReceived),

			strconv.FormatUint(uint64(i.FlowControl.ReceiverWindow), 10),

			strconv.FormatUint(uint64(i.CongestionControl.SenderSSThreshold), 10),
			strconv.FormatUint(uint64(i.CongestionControl.ReceiverSSThreshold), 10),
			strconv.FormatUint(uint64(i.CongestionControl.SenderWindowBytes), 10),
			strconv.FormatUint(uint64(i.CongestionControl.SenderWindowSegs), 10),

			strconv.FormatUint(uint64(i.Sys.PathMTU), 10),
			i.Sys.CAState.String(),
			strconv.FormatUint(uint64(i.Sys.Retransmissions), 10),
			strconv.FormatUint(uint64(i.Sys.Backoffs), 10),
			strconv.FormatUint(uint64(i.Sys.WindowOrKeepAliveProbes), 10),
			strconv.FormatUint(uint64(i.Sys.UnackedSegs), 10),
			strconv.FormatUint(uint64(i.Sys.SackedSegs), 10),
			strconv.FormatUint(uint64(i.Sys.LostSegs), 10),
			strconv.FormatUint(uint64(i.Sys.RetransSegs), 10),
			strconv.FormatUint(uint64(i.Sys.ForwardAckSegs), 10),
			strconv.FormatUint(uint64(i.Sys.ReorderedSegs), 10),
			fmtDurationMs(i.Sys.ReceiverRTT),
			strconv.FormatUint(uint64(i.Sys.TotalRetransSegs), 10),
			strconv.FormatUint(i.Sys.PacingRate, 10),
			strconv.FormatUint(i.Sys.ThruBytesAcked, 10),
			strconv.FormatUint(i.Sys.ThruBytesReceived, 10),
			strconv.FormatUint(uint64(i.Sys.SegsOut), 10),
			strconv.FormatUint(uint64(i.Sys.SegsIn), 10),
			strconv.FormatUint(uint64(i.Sys.NotSentBytes), 10),
			fmtDurationMs(i.Sys.MinRTT),
			strconv.FormatUint(uint64(i.Sys.DataSegsOut), 10),
			strconv.FormatUint(uint64(i.Sys.DataSegsIn), 10),

			maxBW, minRTT, pacingGain, congWindowGain,
		}

		if j == 0 {
			if len(rows[0]) != len(rows[1]) {
				panic(fmt.Sprintf("Header %v and data %v row lengths don't match", len(rows[0]), len(rows[1])))
			}
		}
	}

	return rows
}

func fmtDurationMs(d time.Duration) string {
	return strconv.FormatFloat(float64(d.Nanoseconds())/1e6, 'f', -1, 64)
}

func writeCSV(out string, rows [][]string) error {
	f := os.Stdout
	if out != "" {
		var err error
		f, err = os.Create(out)
		if err != nil {
			return err
		}
		defer f.Close()
	}

	w := csv.NewWriter(f)
	w.WriteAll(rows)

	// Write any buffered data to the underlying writer (standard output).
	w.Flush()

	return w.Error()
}

func (c *SenderClient) Summary() string {
	var b strings.Builder

	sort.Slice(c.UDPMsgsRcvd, func(i, j int) bool {
		return c.UDPMsgsRcvd[i].Seq < c.UDPMsgsRcvd[j].Seq
	})

	numSent := uint64(len(c.UDPMsgsSent))
	numRcvd := len(c.UDPMsgsRcvd)
	duration := c.Sender.GetParams().GetDuration()
	b.WriteString(fmt.Sprintf("%.3fs\t%v packets sent, %v rcvd (%.2f%% lost)",
		duration.Seconds(), numSent, numRcvd, 100.0-float64(numRcvd)/float64(numSent)*100))

	bytesRcvd := uint(0)
	for i := range c.UDPMsgsRcvd {
		bytesRcvd += c.UDPMsgsRcvd[i].Len
	}
	avgGoodput := 0.0
	if len(c.UDPMsgsRcvd) > 0 {
		avgGoodput = float64(bytesRcvd) / c.UDPMsgsRcvd[len(c.UDPMsgsRcvd)-1].TsRcvd.Sub(c.UDPMsgsRcvd[0].TsRcvd).Seconds() * 8 / 1e6
	}
	b.WriteString(fmt.Sprintf("\t%.2f Mbps", avgGoodput))

	if !c.StartAt.IsZero() {
		b.WriteString(fmt.Sprintf("\t%v", c.StartAt))
	}

	return b.String()
}

type CommandClient struct {
	Params   CommandParams
	Local    bool
	StartAt  time.Time
	LocalDir string

	ID      string
	tempDir string
}

func (c *CommandClient) Run(ctx context.Context, client *rpc.Client) error {
	if c.ID == "" {
		c.ID = GenID(c.Params.Name())
		ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))
	}

	if c.Local {
		var command Command
		switch c.Params.(type) {
		case TcpdumpParams:
			command = &TcpdumpCommand{Params_: c.Params.(TcpdumpParams)}
		default:
			return fmt.Errorf("Unknown params %v", c.Params)
		}

		var err error
		c.tempDir, err = RandDir(c.Params.Name())
		if err != nil {
			return fmt.Errorf("RunCommand: failed RandDir: %v", err.Error())
		}

		if err := waitUntil(ctx, c.StartAt); err != nil {
			return err
		}

		slog.DebugContext(ctx, "Writing results", "name", c.Params.Name(), "tempDir", c.tempDir)

		cmd, err := command.Exec(c.tempDir)
		if err != nil {
			return fmt.Errorf("RunCommand: failed command.Cmd: %v", err.Error())
		}

		if err := MonitorCommand(ctx, cmd, c.Params.Timeout()); err != nil {
			return fmt.Errorf("RunCommand: %v", err.Error())
		}
	} else {
		args := RunCommandArgs{ID: c.ID, Params: c.Params, StartAt: c.StartAt}
		var reply RunCommandReply
		if err := client.Call("Server.RunCommand", args, &reply); err != nil {
			return fmt.Errorf("Call Server.RunCommand failed: %v", err.Error())
		}
	}
	return nil
}

func (c *CommandClient) Gather(ctx context.Context, client *rpc.Client) error {
	ctx = context.WithValue(ctx, SlogIDKey{}, slog.Any("id", c.ID))

	if c.Local {
		entries, err := os.ReadDir(c.tempDir)
		if err != nil {
			return fmt.Errorf("Failed os.ReadDir(%v): %v", c.tempDir, err.Error())
		}

		for _, entry := range entries {
			path := filepath.Join(c.tempDir, entry.Name())

			if entry.IsDir() {
				slog.DebugContext(ctx, "Skipping directory", "path", path)
				continue
			}

			encPath := filepath.Join(c.LocalDir, fmt.Sprintf("%s.zst", entry.Name()))
			f, err := os.Create(encPath)
			if err != nil {
				return fmt.Errorf("Failed os.Create(%v): %v", encPath, err.Error())
			}

			fW := bufio.NewWriter(f)
			if err := CompressFile(path, fW); err != nil {
				return fmt.Errorf("Failed compression: %v", err.Error())
			}
			fW.Flush()

			if err := f.Close(); err != nil {
				return fmt.Errorf("Failed closing compressed file %v: %v", encPath, err.Error())
			}

			if err := os.Remove(path); err != nil {
				slog.WarnContext(ctx, "Failed to remove file after reading", "path", path)
			}
		}

		if err := os.Remove(c.tempDir); err != nil {
			slog.WarnContext(ctx, "Failed to remove result directory", "path", c.tempDir)
		}
	} else {
		slog.DebugContext(ctx, "Requesting results", "name", fmt.Sprintf("%T", c.Params))

		var result RequestRunCommandResultReply
		if err := client.Call("Server.RequestRunCommandResult", RequestRunCommandResultArgs{ID: c.ID}, &result); err != nil {
			return fmt.Errorf("Call Server.RunCommand failed: %v", err.Error())
		}

		slog.DebugContext(ctx, "Received results", "name", fmt.Sprintf("%T", c.Params))

		for filename, bufEnc := range result.Files {
			path := filepath.Join(c.LocalDir, fmt.Sprintf("%s.zst", filename))
			if err := os.WriteFile(path, bufEnc, 0644); err != nil {
				return fmt.Errorf("Failed writing returned file to %v: %v", path, err.Error())
			}
		}
	}
	return nil
}

func (c *CommandClient) Summary() string {
	return ""
}

// First try setting up a datagram socket for ICMP packets. This works
// without root and without additional capabilities if sysctl
// net.ipv4.ping_group_range != '1 0' (and if it includes the user's
// group ID). sudo and capabilities don't help if the sysctl setting is
// wrong.
//
// If this fails, we try setting up an ipv4 ICMP socket. This works if
// the user is root or if the binary is run with the CAP_NET_RAW capability.
func listenICMPClient(ctx context.Context, ip string) (conn net.Conn, raddr net.Addr, err error) {
	// ICMP PacketConn WriteTo expects an udp address
	if raddr, err = net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%v", ip, "0")); err != nil {
		err = fmt.Errorf("Failed resolving provided addr for ICMP: %v", err.Error())
		return
	}
	var icmpConn *icmp.PacketConn
	if icmpConn, err = icmp.ListenPacket("udp4", "0.0.0.0"); err != nil {
		slog.DebugContext(ctx, "icmp.ListenPacket failed to create dgram ICMP connecti", "error", err)

		if raddr, err = net.ResolveIPAddr("ip4", ip); err != nil {
			err = fmt.Errorf("Failed resolving ip4 addr for ICMP: %v", err.Error())
			return
		}
		var laddr *net.IPAddr
		laddr, err = net.ResolveIPAddr("ip4", "0.0.0.0")
		if err != nil {
			err = fmt.Errorf("Failed resolving catch-all ip4 addr for ICMP: %v", err.Error())
			return
		}
		if conn, err = net.ListenIP("ip4:icmp", laddr); err != nil {
			err = fmt.Errorf("net.ListenIP failed to create raw ICMP connection: %w. Configure net.ipv4.ping_group_range != '1 0', run as root, or with CAP_NET_RAW capability.", err)
			return
		}
	} else {
		conn = &ICMPMockConn{conn: icmpConn}
	}
	return
}
