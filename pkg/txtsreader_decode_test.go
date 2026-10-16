package pkg

import (
	"bytes"
	"context"
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

func timestampCmsg(level, kind int32, value any) []byte {
	var data bytes.Buffer
	if err := binary.Write(&data, binary.NativeEndian, value); err != nil {
		panic(err)
	}
	hdr := unix.Cmsghdr{Level: level, Type: kind}
	hdr.SetLen(unix.CmsgLen(data.Len()))
	var header bytes.Buffer
	if err := binary.Write(&header, binary.NativeEndian, hdr); err != nil {
		panic(err)
	}
	out := make([]byte, unix.CmsgSpace(data.Len()))
	copy(out, header.Bytes())
	copy(out[unix.CmsgLen(0):], data.Bytes())
	return out
}

func timestampMessage(ipv6 bool) mmsg.Message {
	times := unix.ScmTimestamping{}
	times.Ts[0] = unix.NsecToTimespec(1234567890123)
	oob := timestampCmsg(syscall.SOL_SOCKET, syscall.SCM_TIMESTAMPING, times)
	level, kind := int32(syscall.SOL_IP), int32(syscall.IP_RECVERR)
	if ipv6 {
		level, kind = syscall.SOL_IPV6, syscall.IPV6_RECVERR
	}
	oob = append(oob, timestampCmsg(level, kind, unix.SockExtendedErr{
		Errno: uint32(syscall.ENOMSG), Origin: unix.SO_EE_ORIGIN_TIMESTAMPING, Data: 42,
	})...)
	return mmsg.Message{OOB: oob, NN: len(oob), N: 1500}
}

func TestParseTimestamp(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		msg := timestampMessage(ipv6)
		got, err := NewTxTsReader().parseOOB(context.Background(), msg)
		if err != nil || got.Seq != 42 || got.Len != 1500 || !got.TsSent.Equal(time.Unix(1234, 567890123)) {
			t.Fatalf("ipv6=%v: got %+v, err=%v", ipv6, got, err)
		}
	}
}

func BenchmarkParseTimestamp(b *testing.B) {
	r := NewTxTsReader()
	msg := timestampMessage(false)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := r.parseOOB(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}

func TestParseTimestampMalformed(t *testing.T) {
	valid := timestampMessage(false)
	for n := 0; n < len(valid.OOB); n++ {
		msg := valid
		msg.NN = n
		if _, err := NewTxTsReader().parseOOB(context.Background(), msg); err == nil {
			t.Fatalf("accepted truncated OOB of length %d", n)
		}
	}
	for _, msg := range []mmsg.Message{
		{OOB: valid.OOB, NN: valid.NN, Flags: unix.MSG_CTRUNC},
		{OOB: valid.OOB, NN: -1},
		{OOB: valid.OOB, NN: len(valid.OOB) + 1},
		{OOB: timestampCmsg(syscall.SOL_SOCKET, syscall.SCM_TIMESTAMPING, uint64(1))},
		{OOB: timestampCmsg(syscall.SOL_IP, syscall.IP_RECVERR, uint32(syscall.ENOMSG))},
	} {
		if msg.NN == 0 {
			msg.NN = len(msg.OOB)
		}
		if _, err := NewTxTsReader().parseOOB(context.Background(), msg); err == nil {
			t.Fatal("accepted malformed OOB")
		}
	}
}

func TestParseTimestampRejectsNonTimestampErrors(t *testing.T) {
	msg := timestampMessage(false)
	// Replace only the error cmsg, keeping a valid timestamp present.
	msg.OOB = msg.OOB[:unix.CmsgSpace(binary.Size(unix.ScmTimestamping{}))]
	for _, sockErr := range []unix.SockExtendedErr{
		{Errno: uint32(syscall.ENOMSG), Origin: unix.SO_EE_ORIGIN_ICMP},
		{Errno: uint32(syscall.ECONNREFUSED), Origin: unix.SO_EE_ORIGIN_TIMESTAMPING},
	} {
		oob := append(append([]byte(nil), msg.OOB...), timestampCmsg(syscall.SOL_IP, syscall.IP_RECVERR, sockErr)...)
		if _, err := NewTxTsReader().parseOOB(context.Background(), mmsg.Message{OOB: oob, NN: len(oob)}); err == nil {
			t.Fatal("accepted non-timestamp error")
		}
	}
}

func FuzzParseTimestamp(f *testing.F) {
	f.Add(timestampMessage(false).OOB)
	f.Add(timestampMessage(true).OOB)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = NewTxTsReader().parseOOB(context.Background(), mmsg.Message{OOB: data, NN: len(data)})
	})
}
