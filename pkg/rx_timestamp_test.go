package pkg

import (
	"encoding/binary"
	"strings"
	"testing"
	"time"
	"unsafe"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

func TestParseRxTimestampOOBUsesOnlySelectedTimestamp(t *testing.T) {
	software := time.Unix(1700000000, 123456789)
	rawHardware := time.Unix(10, 987654321)
	var times unix.ScmTimestamping
	times.Ts[0] = unix.NsecToTimespec(software.UnixNano())
	times.Ts[2] = unix.NsecToTimespec(rawHardware.UnixNano())
	oob := timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, times)
	packet := mmsg.Message{OOB: oob, NN: len(oob)}

	gotSoftware, err := parseRxTimestampOOB(packet, TimestampSoftware)
	if err != nil || !gotSoftware.Equal(software) {
		t.Fatalf("software selection = %v, %v; want %v", gotSoftware, err, software)
	}
	gotHardware, err := parseRxTimestampOOB(packet, TimestampHardware)
	if err != nil || !gotHardware.Equal(rawHardware) {
		t.Fatalf("raw hardware selection = %v, %v; want unaltered %v", gotHardware, err, rawHardware)
	}

	// A malformed non-selected timespec does not invalidate the selected source.
	times.Ts[2] = unix.Timespec{Sec: 10, Nsec: int64(time.Second)}
	oob = timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, times)
	packet = mmsg.Message{OOB: oob, NN: len(oob)}
	if got, err := parseRxTimestampOOB(packet, TimestampSoftware); err != nil || !got.Equal(software) {
		t.Fatalf("invalid raw hardware field affected software selection: %v, %v", got, err)
	}
	if _, err := parseRxTimestampOOB(packet, TimestampHardware); err == nil {
		t.Fatal("invalid selected raw hardware timespec was accepted")
	}
}

func TestParseRxTimestampOOBRejectsMalformedAncillary(t *testing.T) {
	var times unix.ScmTimestamping
	times.Ts[0] = unix.NsecToTimespec(time.Now().UnixNano())
	times.Ts[2] = unix.NsecToTimespec(time.Now().UnixNano())
	oob := timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, times)
	for _, packet := range []mmsg.Message{
		{OOB: oob, NN: len(oob), Flags: unix.MSG_CTRUNC},
		{OOB: oob, NN: -1},
		{OOB: oob, NN: len(oob) + 1},
		{OOB: timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, uint64(1)), NN: unix.CmsgSpace(8)},
		{OOB: timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, unix.Timespec{Sec: 1, Nsec: 2})},
	} {
		if _, err := parseRxTimestampOOB(packet, TimestampSoftware); err == nil {
			t.Fatalf("accepted malformed ancillary message %+v", packet)
		}
	}
	duplicate := append(append([]byte(nil), oob...), oob...)
	if _, err := parseRxTimestampOOB(mmsg.Message{OOB: duplicate, NN: len(duplicate)}, TimestampHardware); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate timestamp cmsg error = %v", err)
	}
	if _, err := parseRxTimestampOOB(mmsg.Message{}, TimestampHardware); err == nil {
		t.Fatal("missing selected ancillary timestamp was accepted")
	}

	zero, err := parseNativeTimespec(make([]byte, int(unsafe.Sizeof(unix.Timespec{}))))
	if err != nil || !zero.IsZero() {
		t.Fatalf("zero timespec = %v, %v; want zero timestamp", zero, err)
	}
	bad := make([]byte, int(unsafe.Sizeof(unix.Timespec{})))
	if len(bad) == 16 {
		binary.NativeEndian.PutUint64(bad[8:], uint64(time.Second))
	} else {
		binary.NativeEndian.PutUint32(bad[4:], uint32(time.Second))
	}
	if _, err := parseNativeTimespec(bad); err == nil {
		t.Fatal("accepted out-of-range native timespec nanoseconds")
	}
	negative := make([]byte, int(unsafe.Sizeof(unix.Timespec{})))
	negativeSeconds := int64(-1)
	if len(negative) == 16 {
		binary.NativeEndian.PutUint64(negative, uint64(negativeSeconds))
		binary.NativeEndian.PutUint64(negative[8:], 42)
	} else {
		binary.NativeEndian.PutUint32(negative, uint32(int32(negativeSeconds)))
		binary.NativeEndian.PutUint32(negative[4:], 42)
	}
	gotNegative, err := parseNativeTimespec(negative)
	if err != nil || !gotNegative.Equal(time.Unix(-1, 42)) {
		t.Fatalf("negative timespec seconds were not preserved: %v, %v", gotNegative, err)
	}
}

func TestParseRxTimestampDoesNotFallBackBetweenSources(t *testing.T) {
	software := time.Unix(1700000000, 123456789)
	var times unix.ScmTimestamping
	times.Ts[0] = unix.NsecToTimespec(software.UnixNano())
	oob := timestampCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPING, times)
	packet := mmsg.Message{OOB: oob, NN: len(oob)}
	if got, err := parseRxTimestampOOB(packet, TimestampHardware); err == nil || !got.IsZero() {
		t.Fatalf("hardware selection fell back to software timestamp: %v, %v", got, err)
	}
	if _, err := parseRxTimestampOOB(packet, TimestampApp); err == nil {
		t.Fatal("application source was accepted by kernel ancillary parser")
	}
}
