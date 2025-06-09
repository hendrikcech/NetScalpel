package pkg

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/google/uuid"
	"log"
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
	Run(client *rpc.Client) error
	Gather(client *rpc.Client) error
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
	Ip        string
	Port      uint
	Out       string
	Direction Direction
	StartAt   time.Time

	Sender Sender

	MsgsSent []MsgSent
	MsgsRcvd []MsgRcvd
	Results  []MsgResult

	id uuid.UUID
}

func (c *SenderClient) Run(client *rpc.Client) error {
	switch c.Direction {
	case UL:
		args := RequestUdpServerArgs{
			Timeout: c.Sender.GetParams().GetDuration() + time.Second,
			StartAt: c.StartAt,
			Mode:    Receive,
			Params:  c.Sender.GetParams(),
		}
		var reply RequestUdpServerReply
		if err := client.Call("Server.RequestUdpServer", args, &reply); err != nil {
			return fmt.Errorf("Call Server.RequestUdpServerReply failed: %v", err.Error())
		}
		c.id = reply.Id

		conn, _, raddr, err := OpenUdpSocket(c.Ip, reply.Port)
		if err != nil {
			return fmt.Errorf("OpenUdpSocket: %v\n", err)
		}
		defer conn.Close()

		log.Printf("Client %T %+v to %v", c.Sender, c.Sender.GetParams(), raddr)

		if err := waitUntil(c.StartAt); err != nil {
			return err
		}

		c.MsgsSent, err = c.Sender.Run(conn, raddr)
		if err != nil {
			return fmt.Errorf("send failed: %v\n", err)
		}
	case DL:
		args := RequestUdpServerArgs{
			Timeout: c.Sender.GetParams().GetDuration() + time.Second,
			StartAt: c.StartAt,
			Mode:    c.Sender.Mode(),
			Params:  c.Sender.GetParams(),
		}
		var reply RequestUdpServerReply
		if err := client.Call("Server.RequestUdpServer", args, &reply); err != nil {
			return fmt.Errorf("Call Server.RequestUdpServerReply failed: %v", err.Error())
		}
		c.id = reply.Id

		conn, laddr, raddr, err := OpenUdpSocket(c.Ip, reply.Port)
		if err != nil {
			return fmt.Errorf("OpenUdpSocket failed: %v", err.Error())
		}
		defer conn.Close()

		// TODO: implement retry loop
		probeReplyReceived := false
		for try := range 5 {
			// Send an UDP packet to the newly opened server UDP socket to poke
			// a hole into a potentially existing NAT and wait for the reply.
			if try > 0 {
				log.Printf("Sending NAT probe %v/5 from %v to %v...", try+1, laddr, raddr)
			}
			if _, err := conn.Write([]byte{}); err != nil {
				return fmt.Errorf("Failed WriteTo: %v\n", err.Error())
			}

			probeDeadline := time.Now().Add(time.Second)
			if !c.StartAt.IsZero() && probeDeadline.After(c.StartAt) {
				probeDeadline = c.StartAt
			}
			if err := conn.SetReadDeadline(probeDeadline); err != nil {
				return fmt.Errorf("Failed to probe deadline: %v\n", err.Error())
			}

			if !c.StartAt.IsZero() && time.Now().After(c.StartAt) {
				return fmt.Errorf("StartAt %v already passed before received probe reply at %v", c.StartAt, time.Now())
			}

			var buf [1500]byte
			_, _, err = conn.ReadFrom(buf[:])
			if err != nil {
				if e, ok := err.(net.Error); !ok || !e.Timeout() {
					return fmt.Errorf("Failed ReadFrom: %v", err.Error())
				}
				// Timeout occured
				continue
			}
			log.Printf("Received NAT probe %v/5 from %v", try+1, laddr)
			probeReplyReceived = true
			break
		}
		if !probeReplyReceived {
			return fmt.Errorf("No probe reply received")
		}

		// log.Printf("Wrote UDP to server at %v, receiving at %v, timeout duration is %v\n", raddr, laddr, args.Timeout)

		if err := waitUntil(c.StartAt); err != nil {
			return err
		}

		if err := conn.SetReadDeadline(time.Now().Add(args.Timeout + time.Second)); err != nil {
			return fmt.Errorf("Failed to SetReadDeadline: %v\n", err.Error())
		}

		c.MsgsRcvd, err = ReceiveFrom(conn, c.Sender.GetParams().NumPackets())
		if err != nil {
			log.Printf("Failed ReceiveFrom: %v", err.Error())
		}

		log.Printf("Received %v packets", len(c.MsgsRcvd))
	}
	return nil
}

func (c *SenderClient) Gather(client *rpc.Client) error {
	log.Printf("Requesting results for %T %v", c.Sender, c.id)

	switch c.Direction {
	case UL:
		// Gather results
		var result RequestUdpServerResultReply
		if err := client.Call("Server.RequestUdpServerResult",
			RequestUdpServerResultArgs{Id: c.id}, &result); err != nil {
			return fmt.Errorf("Call Server.RequestUdpServerResult failed: %v", err.Error())
		}
		c.MsgsRcvd = result.MsgRcvd()
	case DL:
		var result RequestUdpServerResultReply
		if err := client.Call("Server.RequestUdpServerResult",
			RequestUdpServerResultArgs{Id: c.id}, &result); err != nil {
			return fmt.Errorf("Call Server.RequestUdpServerResult failed: %v", err.Error())
		}
		c.MsgsSent = result.MsgSent()
	}
	log.Printf("Received results for %T %v", c.Sender, c.id)

	c.Results = processMessages(c.MsgsSent, c.MsgsRcvd)

	if err := writeResult(c.Out, c.Results); err != nil {
		return fmt.Errorf("writeResult failed: %v", err.Error())
	}

	return nil
}

func processMessages(sent []MsgSent, rcvd []MsgRcvd) []MsgResult {
	sort.Slice(rcvd, func(i, j int) bool {
		return rcvd[i].Seq < rcvd[j].Seq
	})

	resultIdx := 0
	num := uint64(len(sent))
	results := make([]MsgResult, 0, num)
	for seq := uint64(0); seq < num; seq++ {
		var result MsgResult
		if resultIdx > len(rcvd)-1 || seq != rcvd[resultIdx].Seq {
			result = MsgResult{
				Seq:    seq,
				TsSent: sent[seq].TsSent,
				TsRcvd: time.Time{},
				Owd:    time.Duration(0),
				Len:    sent[seq].Len,
				Lost:   true,
			}
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
	}

	return results
}

func writeResult(out string, results []MsgResult) error {
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
			owdStr = strconv.FormatFloat(float64(r.Owd.Nanoseconds())/1e6, 'f', -1, 64)
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

	sort.Slice(c.MsgsRcvd, func(i, j int) bool {
		return c.MsgsRcvd[i].Seq < c.MsgsRcvd[j].Seq
	})

	numSent := uint64(len(c.MsgsSent))
	numRcvd := len(c.MsgsRcvd)
	numPackets := c.Sender.GetParams().NumPackets()
	duration := c.Sender.GetParams().GetDuration()
	b.WriteString(fmt.Sprintf("%.3fs\t%v packets sent (target %v), %v rcvd (%.2f%% lost)",
		duration.Seconds(), numSent, numPackets, numRcvd, 100.0-float64(numRcvd)/float64(numSent)*100))

	bytesRcvd := uint(0)
	for i := range c.MsgsRcvd {
		bytesRcvd += c.MsgsRcvd[i].Len
	}
	avgGoodput := 0.0
	if len(c.MsgsRcvd) > 0 {
		avgGoodput = float64(bytesRcvd) / c.MsgsRcvd[len(c.MsgsRcvd)-1].TsRcvd.Sub(c.MsgsRcvd[0].TsRcvd).Seconds() * 8 / 1e6
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

	id      uuid.UUID
	tempDir string
}

func (c *CommandClient) Run(client *rpc.Client) error {
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
			return logErr("RunCommand: failed RandDir: %v", err.Error())
		}

		cmd, err := command.Cmd(c.tempDir)
		if err != nil {
			return logErr("RunCommand: failed command.Cmd: %v", err.Error())
		}

		log.Printf("Writing %v result to %v", c.Params.Name(), c.tempDir)

		if err := waitUntil(c.StartAt); err != nil {
			return err
		}

		if err := RunCommand(cmd, c.Params.Timeout()); err != nil {
			return fmt.Errorf("RunCommand: %v", err.Error())
		}
	} else {
		args := RunCommandArgs{Params: c.Params, StartAt: c.StartAt}
		var reply RunCommandReply
		if err := client.Call("Server.RunCommand", args, &reply); err != nil {
			return fmt.Errorf("Call Server.RunCommand failed: %v", err.Error())
		}
		c.id = reply.Id
	}
	return nil
}

func (c *CommandClient) Gather(client *rpc.Client) error {
	if c.Local {
		entries, err := os.ReadDir(c.tempDir)
		if err != nil {
			return logErr("Failed os.ReadDir(%v): %v", c.tempDir, err.Error())
		}

		for _, entry := range entries {
			path := filepath.Join(c.tempDir, entry.Name())

			if entry.IsDir() {
				log.Printf("Skipping directory %v", path)
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
				log.Printf("Failed to remove %v after reading", path)
			}
		}

		if err := os.Remove(c.tempDir); err != nil {
			log.Printf("Failed to remove result directory %v", c.tempDir)
		}
	} else {
		if c.id == uuid.Nil {
			return fmt.Errorf("No command ID set")
		}

		log.Printf("Requesting results for %T %v", c.Params, c.id)

		var result RequestRunCommandResultReply
		if err := client.Call("Server.RequestRunCommandResult", RequestRunCommandResultArgs{Id: c.id}, &result); err != nil {
			return fmt.Errorf("Call Server.RunCommand failed: %v", err.Error())
		}

		log.Printf("Received results for %T %v", c.Params, c.id)

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
