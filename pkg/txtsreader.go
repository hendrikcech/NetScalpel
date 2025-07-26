package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"syscall"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

// Reads transmission timestamps fromm conn until the context is cancelled.
// At that point, it waits for 100 ms and checks if any more packets have been sent.
// If not, the results are written to C. If so, it reads the timestamps, waits
// another 100 ms and checks again.
type TxTsReader struct {
	C chan []MsgSent
}

func NewTxTsReader() *TxTsReader {
	return &TxTsReader{
		C: make(chan []MsgSent, 1),
	}
}

func (t *TxTsReader) Run(ctx context.Context, conn net.Conn) error {
	sentMsgs := make([]MsgSent, 0, 1024)
	defer func() {
		t.C <- sentMsgs
	}()

	go func() {
		<-ctx.Done()
		// Wait to give the reader some time to process the remaining messages
		duration := 100 * time.Millisecond
		slog.DebugContext(ctx, "Setting read deadline to exit from TxTsReader", "duration", duration)
		conn.SetReadDeadline(time.Now().Add(duration))
	}()

	// batch size
	rx := make([]mmsg.Message, 1024)
	for i := range rx {
		rx[i].Buffers = [][]byte{make([]byte, 1500)}
		rx[i].OOB = make([]byte, 500)
	}

	mconn, err := mmsg.NewConn(conn)
	if err != nil {
		return fmt.Errorf("Failed mmsg.NewConn: %v", err.Error())
	}

	for {
		n, err := mconn.RecvMsgs(rx, unix.MSG_ERRQUEUE)
		if err != nil {
			if n > 0 {
				panic(fmt.Sprintf("n != 0 but %v", n))
			}
			if errors.Is(err, net.ErrClosed) {
				slog.DebugContext(ctx, "Returning from TxTsReader due to closed conn")
				return nil
			}

			if errors.Is(err, os.ErrDeadlineExceeded) {
				if ctx.Err() != nil {
					slog.DebugContext(ctx, "Returning from TxTsReader due to deadline and ctx.Err()")
					return nil
				} else {
					slog.DebugContext(ctx, "Extending TxTsReader conn deadline")
					conn.SetReadDeadline(time.Now().Add(1000 * time.Hour))
				}
				continue
			}

			slog.ErrorContext(ctx, "TxTsReader: mconn.RecvMsgs errored", "error", err)
			return fmt.Errorf("TxTsReader: mconn.RecvMsgs errored: %v", err.Error())
		}
		// slog.DebugContext(ctx, "mconn.RecvMsgs() returned", "n", n)

		for i := 0; i < n; i++ {
			sentMsg, err := t.parseOOB(ctx, rx[i])
			if err != nil {
				slog.ErrorContext(ctx, "parseOOB", "error", err)
				continue
			}
			// slog.DebugContext(ctx, "", "sentMsg", sentMsg)
			sentMsgs = append(sentMsgs, sentMsg)
		}
	}
}

func (t *TxTsReader) parseOOB(ctx context.Context, msg mmsg.Message) (MsgSent, error) {
	sentMsg := MsgSent{Len: uint(msg.N)}

	cms, err := unix.ParseSocketControlMessage(msg.OOB[:msg.NN])
	if err != nil {
		return sentMsg, fmt.Errorf("TxTsReader: Failed parsing cmsg: %v", err.Error())
	}

	tsSet := false
	seqSet := false
	for _, cm := range cms {
		if cm.Header.Level == syscall.SOL_SOCKET && cm.Header.Type == syscall.SCM_TIMESTAMPING {
			var times unix.ScmTimestamping
			tsBuf := bytes.NewReader(cm.Data)
			binary.Read(tsBuf, binary.LittleEndian, &times)
			ts := times.Ts[0]
			sentMsg.TsSent = time.Unix(ts.Sec, ts.Nsec)
			tsSet = true
		} else if (cm.Header.Level == syscall.SOL_IP || cm.Header.Level == syscall.SOL_IPV6) &&
			(cm.Header.Type == syscall.IP_RECVERR || cm.Header.Type == syscall.IPV6_RECVERR) {
			var sockErr unix.SockExtendedErr
			sockErrBuf := bytes.NewReader(cm.Data)
			binary.Read(sockErrBuf, binary.LittleEndian, &sockErr)
			if sockErr.Errno == uint32(syscall.ENOMSG) { // expected for timestamps
				sentMsg.Seq = uint64(sockErr.Data)
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
			"cms", cms,
			"oob", fmt.Sprintf("%x", msg.OOB[:msg.NN]),
		)
		return sentMsg, fmt.Errorf("TxTsReader: Missing data in cm")
	}

	return sentMsg, nil
}

// The caller must call the cancel func to stop the reader
func startTxTsReader(ctx context.Context, conn *net.UDPConn) (*TxTsReader, context.CancelFunc) {
	var reader *TxTsReader
	ctxReader, cancelReader := context.WithCancel(ctx)
	if err := enableTxTimestamping(conn); err != nil {
		slog.WarnContext(ctx, "Failed enabling tx timestamping", "error", err.Error())
	} else {
		reader = NewTxTsReader()
		go reader.Run(ctxReader, conn)
	}
	return reader, cancelReader
}

// Uses the result of TxTsReader if it was run. Uses the packet size from sentMsgs if unix.SOF_TIMESTAMPING_OPT_TSONLY was set
func selectMsgsSent(ctx context.Context, reader *TxTsReader, cancelReader context.CancelFunc, sentMsgs []MsgSent) []MsgSent {
	if reader == nil {
		slog.DebugContext(ctx, "TxTsReader was not used, use sentMsgs from Run func")
		return sentMsgs
	}
	cancelReader()
	slog.DebugContext(ctx, "selectMsgsSent: waiting on TxTsReader.C")
	tsMsgs := <-reader.C
	if len(sentMsgs) != len(tsMsgs) {
		slog.ErrorContext(ctx, fmt.Sprintf("run function returned %v, txTsReader %v msgs",
			len(sentMsgs), len(tsMsgs)))
	}
	for i := range tsMsgs {
		tsMsgs[i].Len = sentMsgs[0].Len
	}
	slog.DebugContext(ctx, "selectMsgsSent: returning with tsMsgs")
	return tsMsgs
}
