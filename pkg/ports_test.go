package pkg

import (
	"net"
	"testing"
)

func TestSenderClientPortsFollowDirection(t *testing.T) {
	for _, tc := range []struct {
		direction                       Direction
		source, destination             uint
		remoteSource, remoteDestination uint
	}{
		{UL, 40001, 40002, 40003, 40002},
		{DL, 40002, 40001, 40002, 40003},
	} {
		c := &SenderClient{Direction: tc.direction, localPort: 40001, port: 40002, peerPort: 40003}
		if got := c.SourcePort(); got != tc.source {
			t.Fatalf("%v source port = %d, want %d", tc.direction, got, tc.source)
		}
		if got := c.DestinationPort(); got != tc.destination {
			t.Fatalf("%v destination port = %d, want %d", tc.direction, got, tc.destination)
		}
		if got := c.RemoteSourcePort(); got != tc.remoteSource {
			t.Fatalf("%v remote source port = %d, want %d", tc.direction, got, tc.remoteSource)
		}
		if got := c.RemoteDestinationPort(); got != tc.remoteDestination {
			t.Fatalf("%v remote destination port = %d, want %d", tc.direction, got, tc.remoteDestination)
		}
	}
}

func TestAddrPort(t *testing.T) {
	if got := addrPort(&net.UDPAddr{Port: 40001}); got != 40001 {
		t.Fatalf("UDP port = %d", got)
	}
	if got := addrPort(&net.TCPAddr{Port: 40002}); got != 40002 {
		t.Fatalf("TCP port = %d", got)
	}
	if got := addrPort(&net.IPAddr{}); got != 0 {
		t.Fatalf("IP port = %d", got)
	}
}

func TestSetLocalIP(t *testing.T) {
	bound := net.IPv4(192, 0, 2, 1)
	c := &SenderClient{IP: "127.0.0.1"}
	c.setLocalIP(&net.TCPAddr{IP: bound})
	if c.LocalIP() != bound.String() {
		t.Fatalf("bound local IP = %q, want %q", c.LocalIP(), bound)
	}

	c = &SenderClient{IP: "127.0.0.1"}
	c.setLocalIP(&net.UDPAddr{IP: net.IPv4zero})
	if c.LocalIP() != "127.0.0.1" {
		t.Fatalf("routed local IP = %q, want 127.0.0.1", c.LocalIP())
	}
}
