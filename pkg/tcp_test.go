package pkg

import (
	"context"
	"net"
	"strings"
	"testing"
)

// TCPMonitor.Run on a conn that does not support TCP_INFO (here: a net.Pipe
// end) must report the tcp.NewConn failure and return. The error branches
// used to fall through with a nil tcp.Conn, overwriting Err with follow-up
// errors from calls that could not succeed anymore.
func TestTCPMonitorRunNonTCPConn(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	m := NewTCPMonitor()
	m.Run(context.Background(), c1, false)

	if m.Err == nil {
		t.Fatal("expected Err to be set for a non-TCP conn")
	}
	if !strings.Contains(m.Err.Error(), "Failed opening tcp info conn") {
		t.Errorf("expected the tcp.NewConn error, got: %v", m.Err)
	}
	select {
	case <-m.C:
	default:
		t.Error("expected C to be closed after Run returned")
	}
}
