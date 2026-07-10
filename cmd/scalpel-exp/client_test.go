package main

import (
	"context"
	"net"
	"testing"
)

// dialRpcClient must return an error on a failed dial. It used to os.Exit(1)
// from a helper goroutine — including after the caller had already returned
// on a cancelled context, killing the process during shutdown.
func TestDialRpcClientReturnsErrorOnRefusedConn(t *testing.T) {
	// Grab a port that is guaranteed to be closed.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint(ln.Addr().(*net.TCPAddr).Port)
	ln.Close()

	client, err := dialRpcClient(context.Background(), "127.0.0.1", port)
	if err == nil {
		client.Close()
		t.Fatal("expected an error dialing a closed port")
	}
}

func TestDialRpcClientReturnsErrorOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	client, err := dialRpcClient(ctx, "127.0.0.1", 1)
	if err == nil {
		client.Close()
		t.Fatal("expected an error dialing with a cancelled context")
	}
}
