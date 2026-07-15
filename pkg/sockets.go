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
	if err != nil {
		return -1, -1, err
	}
	defer fd.Close()
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
	if err != nil {
		return err
	}
	defer fd.Close()
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

// Used by the application
func applyTCPCCA(ctx context.Context, conn syscall.Conn, cca TCPCCA) error {
	kernelName, err := cca.KernelName()
	if err != nil {
		return fmt.Errorf("Failed setting TCPCCA %v (%v): %w", cca.String(), kernelName, err)
	}
	if err := setTCPCC(ctx, conn, kernelName); err != nil {
		return fmt.Errorf("Failed setting TCPCCA %v (%v): %w", cca.String(), kernelName, err)
	}

	if cca == CUBIC || cca == CUBIC_NO_HYSTART {
		hy, err := hystartEnabled()
		if err != nil {
			return err
		}
		if cca == CUBIC && !hy {
			if err := enableHystart(true); err != nil {
				return err
			}
		}
		if cca == CUBIC_NO_HYSTART && hy {
			if err := enableHystart(false); err != nil {
				return err
			}
		}
	}

	// if cca == LEOCC {
	// 	if err := limitTCPMSS(ctx, conn); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

// Set TCP congestion control of an TCP socket
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

// MSS used for LeoCC connections: makes space for the TCP option that the
// LeoCC kernel module adds.
const LeoCCMSS = 1400

// LimitTCPMSS clamps the TCP MSS of a not-yet-connected socket via
// TCP_MAXSEG and verifies the kernel accepted the value. Must run before
// connect (e.g. from a net.Dialer.Control); afterwards the kernel reports
// the negotiated MSS, which is smaller by the TCP option space.
func LimitTCPMSS(rawConn syscall.RawConn, mss int) error {
	var cbErr error
	err := rawConn.Control(func(fd uintptr) {
		cbErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG, mss)
		if cbErr != nil {
			return
		}

		var newMSS int
		newMSS, cbErr = syscall.GetsockoptInt(int(fd), syscall.IPPROTO_TCP, syscall.TCP_MAXSEG)
		if cbErr != nil {
			return
		}

		if newMSS != mss {
			cbErr = fmt.Errorf("Unexpected TCP MSS after setting: %v != %v", newMSS, mss)
		}
	})

	if err != nil {
		return err
	}
	return cbErr
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

const hystartPath = "/sys/module/tcp_cubic/parameters/hystart"

func enableHystart(enable bool) error {
	file, err := os.OpenFile(hystartPath, os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("Failed to open hystart parameter: %w", err)
	}
	defer file.Close()

	content := "0"
	if enable {
		content = "1"
	}

	if _, err = file.WriteString(content); err != nil {
		return fmt.Errorf("Failed to write to hystart parameter: %w", err)
	}

	return nil
}

// reads the current value of tcp_cubic's hystart parameter
func hystartEnabled() (bool, error) {
	data, err := os.ReadFile(hystartPath)
	if err != nil {
		return false, fmt.Errorf("failed to read hystart parameter: %w", err)
	}

	value := strings.TrimSpace(string(data))
	switch value {
	case "1":
		return true, nil
	case "0":
		return false, nil
	default:
		return false, fmt.Errorf("unexpected hystart value: %s", value)
	}
}

func readAvailableKernelCCAs() (string, error) {
	path := "/proc/sys/net/ipv4/tcp_available_congestion_control"
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read available congestion controls: %w", err)
	}
	return string(data), nil
}
