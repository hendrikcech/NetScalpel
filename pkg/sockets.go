package pkg

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

func setSocketBuffers(ctx context.Context, conn *net.UDPConn) {
	udpBufferSize := 30_000_000
	sndPrev, rcvPrev, _ := getSocketBuffers(conn)
	errWrite := conn.SetWriteBuffer(udpBufferSize)
	errRead := conn.SetReadBuffer(udpBufferSize)
	sndBuf, rcvBuf, errGet := getSocketBuffers(conn)
	if errWrite != nil || errRead != nil || errGet != nil || sndBuf < udpBufferSize || rcvBuf < udpBufferSize {
		errForce := forceSetSocketBuffers(conn, udpBufferSize)
		sndBuf, rcvBuf, errGet = getSocketBuffers(conn)
		if sndBuf < udpBufferSize || rcvBuf < udpBufferSize {
			slog.WarnContext(ctx, "Failed setting UDP socket buffers",
				"goal", udpBufferSize,
				"sndPrev", sndPrev,
				"sndNow", sndBuf,
				"rcvPrev", rcvPrev,
				"rcvNow", rcvBuf,
				"errorWrite", errWrite,
				"errorGet", errGet,
				"errorForce", errForce)
		}
	}
}

func getSocketBuffers(conn *net.UDPConn) (int, int, error) {
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

func forceSetSocketBuffers(conn *net.UDPConn, size int) error {
	fd, err := conn.File()
	defer fd.Close()
	if err != nil {
		return err
	}
	// Necessary to continue make Deadline work on conn: https://stackoverflow.com/a/74886460
	defer syscall.SetNonblock(int(fd.Fd()), true)

	if err := syscall.SetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_SNDBUFFORCE, size); err != nil {
		return err
	}
	if err := syscall.SetsockoptInt(int(fd.Fd()), syscall.SOL_SOCKET, syscall.SO_RCVBUFFORCE, size); err != nil {
		return err
	}

	return nil
}

func setMaxPacingRate(conn *net.UDPConn, rate uint) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Control(func(fd uintptr) {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_MAX_PACING_RATE, int(rate)); err != nil {
			err = fmt.Errorf("Failed setting SO_MAX_PACING_RATE: %v", err.Error())
		}
	})

	return err
}

func enableTxTimestamping(conn syscall.Conn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Control(func(fd uintptr) {
		flags := unix.SOF_TIMESTAMPING_TX_SOFTWARE |
			unix.SOF_TIMESTAMPING_TX_HARDWARE |
			unix.SOF_TIMESTAMPING_SOFTWARE |
			unix.SOF_TIMESTAMPING_RAW_HARDWARE |
			unix.SOF_TIMESTAMPING_OPT_ID |
			unix.SOF_TIMESTAMPING_OPT_TSONLY // needed to determine size of packet

		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPING, flags)

		var flag int
		flag, err = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPING)
		if err != nil {
			return
		}
		if flag != flags {
			err = fmt.Errorf("TxTimestamping flags not set as expected: %v != %v", flags, flag)
		}
	})

	return err
}

func enableRxTimestamping(conn syscall.Conn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	err = rawConn.Control(func(fd uintptr) {
		flags := unix.SOF_TIMESTAMPING_RX_SOFTWARE |
			unix.SOF_TIMESTAMPING_RX_HARDWARE |
			unix.SOF_TIMESTAMPING_SOFTWARE |
			unix.SOF_TIMESTAMPING_RAW_HARDWARE |
			unix.SOF_TIMESTAMPING_OPT_ID |
			unix.SOF_TIMESTAMPING_OPT_TSONLY // needed to determine size of packet

		err = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_TIMESTAMPING, flags)
	})

	return err
}

func setTCPCC(ctx context.Context, conn syscall.Conn, cc string) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	var cbErr error
	err = rawConn.Control(func(fd uintptr) {
		cbErr = syscall.SetsockoptString(int(fd), syscall.IPPROTO_TCP, syscall.TCP_CONGESTION, cc)
		if cbErr != nil {
			return
		}

		var buf [256]byte
		cbErr = GetsockoptString(int(fd), syscall.IPPROTO_TCP, syscall.TCP_CONGESTION, buf[:])
		if cbErr != nil {
			return
		}

		newCC := string(buf[:])
		if !strings.HasPrefix(newCC, cc) {
			cbErr = fmt.Errorf("NewCC differs: %v != %v", newCC, cc)
		}

		slog.Debug("Set TCP socket CC", "cc", newCC[:10], "reqCC", cc)
	})

	if err != nil {
		return err
	}
	if cbErr != nil {
		return cbErr
	}
	return nil
}

// syscall.GetsockoptString emulation
func GetsockoptString(fd, level, opt int, buf []byte) error {
	var size = uint32(len(buf))
	return getsockopt(fd, level, opt, unsafe.Pointer(&buf[0]), &size)
}

func getsockopt(fd, level, opt int, val unsafe.Pointer, vallen *uint32) (err error) {
	_, _, e1 := syscall.Syscall6(syscall.SYS_GETSOCKOPT, uintptr(fd), uintptr(level), uintptr(opt), uintptr(val), uintptr(unsafe.Pointer(vallen)), 0)
	if e1 != 0 {
		err = os.NewSyscallError("getsockopt", e1)
	}
	return
}
