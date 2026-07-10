package pkg

import (
	"net"
	"syscall"
	"testing"
)

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
