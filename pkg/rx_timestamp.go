package pkg

import (
	"encoding/binary"
	"fmt"
	"syscall"
	"time"
	"unsafe"

	mmsg "github.com/anacrolix/mmsg/socket"
	"golang.org/x/sys/unix"
)

func parseRxTimestampOOB(packet mmsg.Message, source TimestampSource) (time.Time, error) {
	if packet.Flags&unix.MSG_CTRUNC != 0 {
		return time.Time{}, fmt.Errorf("truncated ancillary data")
	}
	if packet.NN < 0 || packet.NN > len(packet.OOB) {
		return time.Time{}, fmt.Errorf("invalid ancillary length %d", packet.NN)
	}
	index := 0
	if source == TimestampHardware {
		index = 2
	} else if source != TimestampSoftware {
		return time.Time{}, fmt.Errorf("unsupported kernel RX timestamp source %q", source)
	}
	timestampSeen := false
	var timestamp time.Time
	remaining := packet.OOB[:packet.NN]
	for len(remaining) > 0 {
		if len(remaining) < unix.CmsgLen(0) {
			return time.Time{}, fmt.Errorf("partial trailing ancillary header")
		}
		header, data, rest, err := unix.ParseOneSocketControlMessage(remaining)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse ancillary data: %w", err)
		}
		remaining = rest
		if header.Level != syscall.SOL_SOCKET || header.Type != syscall.SCM_TIMESTAMPING {
			continue
		}
		if timestampSeen {
			return time.Time{}, fmt.Errorf("duplicate SCM_TIMESTAMPING control message")
		}
		timestampSeen = true
		timestamp, err = parseTimestampingTime(data, index)
		if err != nil {
			return time.Time{}, err
		}
	}
	if !timestampSeen {
		return time.Time{}, fmt.Errorf("missing SCM_TIMESTAMPING control message")
	}
	if timestamp.IsZero() {
		return time.Time{}, fmt.Errorf("missing %s RX timestamp", source)
	}
	return timestamp, nil
}

func parseTimestampingTime(data []byte, index int) (time.Time, error) {
	const tsSize = int(unsafe.Sizeof(unix.Timespec{}))
	if index < 0 || index > 2 || len(data) < 3*tsSize {
		return time.Time{}, fmt.Errorf("short SCM_TIMESTAMPING control message: %d bytes", len(data))
	}
	timestamp, err := parseNativeTimespec(data[index*tsSize : (index+1)*tsSize])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s timestamp: %w", []string{"software", "transformed hardware", "raw hardware"}[index], err)
	}
	return timestamp, nil
}

func parseNativeTimespec(data []byte) (time.Time, error) {
	const tsSize = int(unsafe.Sizeof(unix.Timespec{}))
	if len(data) < tsSize {
		return time.Time{}, fmt.Errorf("short timespec: %d bytes", len(data))
	}
	var sec, nsec int64
	if tsSize == 16 {
		sec = int64(binary.NativeEndian.Uint64(data[:8]))
		nsec = int64(binary.NativeEndian.Uint64(data[8:16]))
	} else {
		sec = int64(int32(binary.NativeEndian.Uint32(data[:4])))
		nsec = int64(int32(binary.NativeEndian.Uint32(data[4:8])))
	}
	if sec == 0 && nsec == 0 {
		return time.Time{}, nil
	}
	if nsec < 0 || nsec >= int64(time.Second) {
		return time.Time{}, fmt.Errorf("nanoseconds %d out of range", nsec)
	}
	return time.Unix(sec, nsec), nil
}
