package pkg

import (
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
	C        chan []MsgSent
	sentMsgs []MsgSent
	runErr   error
	invalid  uint64
}

func NewTxTsReader() *TxTsReader {
	return newTxTsReader(1024)
}

// Allocate before starting the reader goroutine so large runs do not repeatedly
// copy their timestamp history while the sender is being measured. This is a
// capacity hint, not a limit: unexpected extra timestamps can still be appended.
func newTxTsReader(expectedPackets uint) *TxTsReader {
	return &TxTsReader{
		C:        make(chan []MsgSent, 1),
		sentMsgs: make([]MsgSent, 0, expectedPackets),
	}
}

func (t *TxTsReader) Run(ctx context.Context, conn net.Conn) (runErr error) {
	defer func() {
		t.runErr = runErr
		t.C <- t.sentMsgs
		close(t.C)
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
		return fmt.Errorf("Failed mmsg.NewConn: %w", err)
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
					slog.DebugContext(ctx, "Returning from TxTsReader due to deadline and ctx.Err()", "ctx.Err()", ctx.Err())
					return nil
				} else {
					slog.DebugContext(ctx, "Clearing TxTsReader conn deadline")
					conn.SetReadDeadline(time.Time{})
				}
				continue
			}

			slog.ErrorContext(ctx, "TxTsReader: mconn.RecvMsgs errored", "error", err)
			return fmt.Errorf("TxTsReader: mconn.RecvMsgs errored: %w", err)
		}
		// slog.DebugContext(ctx, "mconn.RecvMsgs() returned", "n", n)

		for i := 0; i < n; i++ {
			sentMsg, err := t.parseOOB(ctx, rx[i])
			if err != nil {
				slog.ErrorContext(ctx, "parseOOB", "error", err)
				t.invalid++
				continue
			}
			// slog.DebugContext(ctx, "", "sentMsg", sentMsg)
			t.sentMsgs = append(t.sentMsgs, sentMsg)
		}
	}
}

func (t *TxTsReader) parseOOB(ctx context.Context, msg mmsg.Message) (MsgSent, error) {
	sentMsg := MsgSent{Len: uint(msg.N)}

	if msg.Flags&unix.MSG_CTRUNC != 0 {
		return sentMsg, fmt.Errorf("TxTsReader: truncated ancillary data")
	}
	if msg.NN < 0 || msg.NN > len(msg.OOB) {
		return sentMsg, fmt.Errorf("TxTsReader: invalid OOB length %d", msg.NN)
	}

	tsSet := false
	timestampSeen := false
	seqSet := false
	remaining := msg.OOB[:msg.NN]
	for len(remaining) > 0 {
		if len(remaining) < unix.CmsgLen(0) {
			return sentMsg, fmt.Errorf("TxTsReader: partial trailing ancillary header")
		}
		hdr, data, rest, err := unix.ParseOneSocketControlMessage(remaining)
		if err != nil {
			return sentMsg, fmt.Errorf("TxTsReader: Failed parsing cmsg: %w", err)
		}
		remaining = rest
		if hdr.Level == syscall.SOL_SOCKET && hdr.Type == syscall.SCM_TIMESTAMPING {
			if timestampSeen {
				return sentMsg, fmt.Errorf("TxTsReader: duplicate SCM_TIMESTAMPING control message")
			}
			timestampSeen = true
			sentMsg.TsSent, err = parseTimestampingTime(data, 0)
			if err != nil {
				return sentMsg, fmt.Errorf("TxTsReader: %w", err)
			}
			tsSet = !sentMsg.TsSent.IsZero()
		} else if (hdr.Level == syscall.SOL_IP && hdr.Type == syscall.IP_RECVERR) ||
			(hdr.Level == syscall.SOL_IPV6 && hdr.Type == syscall.IPV6_RECVERR) {
			// sock_extended_err has a fixed 16-byte layout on Linux.
			if len(data) < 16 {
				return sentMsg, fmt.Errorf("TxTsReader: short sock_extended_err")
			}
			if binary.NativeEndian.Uint32(data) != uint32(syscall.ENOMSG) || data[4] != unix.SO_EE_ORIGIN_TIMESTAMPING {
				return sentMsg, fmt.Errorf("TxTsReader: non-timestamping socket error")
			}
			if seqSet {
				return sentMsg, fmt.Errorf("TxTsReader: duplicate timestamping socket error")
			}
			if binary.NativeEndian.Uint32(data[8:12]) != unix.SCM_TSTAMP_SND {
				return sentMsg, fmt.Errorf("TxTsReader: unexpected timestamp event type %d", binary.NativeEndian.Uint32(data[8:12]))
			}
			sentMsg.Seq = uint64(binary.NativeEndian.Uint32(data[12:16]))
			seqSet = true
		} else {
			slog.WarnContext(ctx, "TxTsReader: Unknown cm", "header", hdr)
		}
	}

	if !tsSet || !seqSet {
		return sentMsg, fmt.Errorf("TxTsReader: Missing data in cm")
	}
	if sentMsg.TsSent.IsZero() {
		return sentMsg, fmt.Errorf("TxTsReader: zero TX timestamp")
	}

	return sentMsg, nil
}

// beginTxTimestampCollection selects and enables the source before sending.
// Hardware TX currently uses software kernel timestamps; collection failures do
// not trigger another source selection.
func beginTxTimestampCollection(ctx context.Context, conn *net.UDPConn, expectedPackets uint, requested TimestampSource) (*TxTsReader, context.CancelFunc, TimestampDirectionReport, error) {
	selection, err := selectTimestampSource(ctx, requested, true)
	if err != nil {
		return nil, func() {}, selection, err
	}
	if selection.Source == TimestampApp {
		return nil, func() {}, selection, nil
	}
	if err := enableTxTimestamping(conn); err != nil {
		return nil, func() {}, selection, fmt.Errorf("could not enable selected %s TX timestamps: %w", selection.Source, err)
	}
	reader := newTxTsReader(expectedPackets)
	readerCtx, cancel := context.WithCancel(ctx)
	go reader.Run(readerCtx, conn)
	return reader, cancel, selection, nil
}

func matchTxTimestamps(sentMsgs, tsMsgs []MsgSent) ([]MsgSent, error) {
	if len(sentMsgs) != len(tsMsgs) {
		return nil, fmt.Errorf("TX timestamp count %d does not match sent packet count %d", len(tsMsgs), len(sentMsgs))
	}
	byID := make(map[uint64]MsgSent, len(tsMsgs))
	for _, tsMsg := range tsMsgs {
		if tsMsg.TsSent.IsZero() {
			return nil, fmt.Errorf("TX timestamp ID %d has a zero timestamp", tsMsg.Seq)
		}
		if _, duplicate := byID[tsMsg.Seq]; duplicate {
			return nil, fmt.Errorf("duplicate TX timestamp ID %d", tsMsg.Seq)
		}
		byID[tsMsg.Seq] = tsMsg
	}
	matched := make([]MsgSent, len(sentMsgs))
	seen := make(map[uint64]struct{}, len(sentMsgs))
	for i, sent := range sentMsgs {
		if _, duplicate := seen[sent.Seq]; duplicate {
			return nil, fmt.Errorf("duplicate sent packet ID %d", sent.Seq)
		}
		seen[sent.Seq] = struct{}{}
		tsMsg, ok := byID[sent.Seq]
		if !ok {
			return nil, fmt.Errorf("missing TX timestamp ID %d", sent.Seq)
		}
		matched[i] = MsgSent{Seq: sent.Seq, TsSent: tsMsg.TsSent, Len: sent.Len}
	}
	return matched, nil
}

// selectTxTimestamps applies the pre-measurement selection to the whole run.
// Missing evidence and reader errors never switch sources.
func selectTxTimestamps(ctx context.Context, reader *TxTsReader, cancel context.CancelFunc, sentMsgs []MsgSent, selection TimestampDirectionReport) ([]MsgSent, TimestampDirectionReport, error) {
	packets := uint64(len(sentMsgs))
	selection.PacketCount = packets
	if selection.Source == TimestampApp {
		for _, sent := range sentMsgs {
			if !sent.TsSent.IsZero() {
				continue
			}
			selection.MissingCount++
		}
		if selection.MissingCount != 0 {
			return nil, selection, fmt.Errorf("selected app TX timestamps are missing for %d of %d packets", selection.MissingCount, packets)
		}
		return sentMsgs, selection, nil
	}
	if reader == nil {
		return failSelectedTx(selection, fmt.Errorf("selected TX timestamp reader was not started"), packets, 0, 0)
	}
	// The reader is drained after sending stops, and the cancellation grace is
	// needed to collect the final error-queue notifications.
	cancel()
	tsMsgs, ok := <-reader.C
	stamped := uint64(len(tsMsgs))
	var err error
	if !ok {
		err = fmt.Errorf("TX timestamp reader returned no result")
	} else if reader.runErr != nil {
		err = reader.runErr
	} else if reader.invalid > 0 {
		err = fmt.Errorf("TX timestamp reader rejected %d malformed timestamp notifications", reader.invalid)
	} else {
		var matched []MsgSent
		matched, err = matchTxTimestamps(sentMsgs, tsMsgs)
		if err == nil {
			selection.MissingCount = 0
			slog.DebugContext(ctx, "selected software TX timestamps", "packets", packets)
			return matched, selection, nil
		}
	}
	return failSelectedTx(selection, err, packets, stamped, reader.invalid)
}

func failSelectedTx(selection TimestampDirectionReport, cause error, packets, stamped, invalid uint64) ([]MsgSent, TimestampDirectionReport, error) {
	selection.PacketCount = packets
	selection.MissingCount = packets - min(packets, stamped)
	selection.InvalidCount = invalid
	return nil, selection, fmt.Errorf("%s TX timestamp selection failed: %w", selection.Source, cause)
}
