package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"syscall"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

type Receiver interface {
	Init()
	Run(ctx context.Context, conn *net.UDPConn, expectedNumPackets uint) ([]MsgRcvd, error)
}

type UDPReceiver struct {
}

func (r *UDPReceiver) Init() {
}

// Receives until ctx is cancelled. ReadDeadline = time.Now() is set to wake up and return from ReceiveFrom.
func (r *UDPReceiver) Run(ctx context.Context, conn *net.UDPConn, expectedNumPackets uint) ([]MsgRcvd, error) {
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
				return nil, fmt.Errorf("ReceiveFrom: ReadMsgs: %v", err.Error())
			}
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "RecvMsgs returned Timeout error from unknown origin -> extending",
					"error", err,
					"ctxErr", ctx.Err())
				conn.SetReadDeadline(time.Now().Add(1000 * time.Hour))
				continue
			}
			if time.Since(tsRcvd) < 250*time.Millisecond {
				msg := "Wanted to stop but previous packet was received very recently; trying again in 100 ms"
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
