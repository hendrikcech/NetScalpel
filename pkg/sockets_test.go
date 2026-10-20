package pkg

import (
	"errors"
	"net"
	"syscall"
	"testing"
)

type timestampingConnStub struct {
	raw syscall.RawConn
	err error
}

func (c timestampingConnStub) SyscallConn() (syscall.RawConn, error) {
	return c.raw, c.err
}

type timestampingRawConnStub struct {
	control func(func(uintptr)) error
}

func (c timestampingRawConnStub) Control(f func(uintptr)) error  { return c.control(f) }
func (c timestampingRawConnStub) Read(func(uintptr) bool) error  { panic("unexpected Read") }
func (c timestampingRawConnStub) Write(func(uintptr) bool) error { panic("unexpected Write") }

func TestTimestampingErrors(t *testing.T) {
	for _, setup := range []struct {
		name   string
		enable func(syscall.Conn) error
	}{
		{name: "tx", enable: enableTxTimestamping},
		{name: "rx", enable: enableRxTimestamping},
	} {
		t.Run(setup.name, func(t *testing.T) {
			connErr := errors.New("SyscallConn failed")
			controlErr := errors.New("Control failed")
			for _, tc := range []struct {
				name string
				conn timestampingConnStub
				want error
			}{
				{name: "syscall_conn", conn: timestampingConnStub{err: connErr}, want: connErr},
				{name: "control", conn: timestampingConnStub{raw: timestampingRawConnStub{
					control: func(func(uintptr)) error { return controlErr },
				}}, want: controlErr},
				{name: "socket_option", conn: timestampingConnStub{raw: timestampingRawConnStub{
					control: func(f func(uintptr)) error {
						f(^uintptr(0)) // Invalid descriptor: SetsockoptInt must fail.
						return nil
					},
				}}, want: syscall.EBADF},
			} {
				t.Run(tc.name, func(t *testing.T) {
					if err := setup.enable(tc.conn); !errors.Is(err, tc.want) {
						t.Fatalf("timestamping setup error = %v, want %v", err, tc.want)
					}
				})
			}
		})
	}
}

func TestTimestampingSetup(t *testing.T) {
	for _, setup := range []struct {
		name   string
		enable func(syscall.Conn) error
	}{
		{name: "tx", enable: enableTxTimestamping},
		{name: "rx", enable: enableRxTimestamping},
	} {
		t.Run(setup.name, func(t *testing.T) {
			conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := setup.enable(conn); err != nil {
				t.Fatalf("timestamping setup failed: %v", err)
			}
		})
	}
}

// LimitTCPMSS must set and verify the same value. Its predecessor set 1400
// but compared against 1350, logging a spurious error on every LeoCC UL
// connection.
func TestLimitTCPMSS(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			conn.Close()
		}
	}()

	dialer := net.Dialer{Control: func(network, address string, rc syscall.RawConn) error {
		return LimitTCPMSS(rc, LeoCCMSS)
	}}
	conn, err := dialer.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial with LimitTCPMSS control failed: %v", err)
	}
	conn.Close()
}
