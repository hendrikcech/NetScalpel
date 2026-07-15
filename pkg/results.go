package pkg

// Pure result processing and CSV output for the measurement clients: matching
// sent against received packets and rendering the UDP/TCP result rows.

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"
)

// C: Written to CSV
type MsgResult struct {
	Seq    uint64
	TsSent time.Time
	TsRcvd time.Time
	Owd    time.Duration
	Len    uint
	Lost   bool
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
