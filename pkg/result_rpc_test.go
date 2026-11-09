package pkg

import (
	"context"
	"errors"
	"testing"
)

func TestResultRPCsReturnTerminalFailuresInReplies(t *testing.T) {
	t.Run("server result", func(t *testing.T) {
		const id = "failed-server-result"
		server := NewServer(context.Background(), nil)
		server.resultC[id] = make(chan *Result, 1)
		server.resultC[id] <- &Result{Err: errors.New("measurement failed")}

		client := rpcTestClient(t, server)
		var reply RequestServerResultReply
		err := client.Call("Server.RequestServerResult", RequestServerResultArgs{ID: id}, &reply)
		if err != nil {
			t.Fatalf("RequestServerResult RPC returned error: %v", err)
		}
		if !reply.Complete || reply.Error == "" {
			t.Fatalf("expected confirmed terminal failure, got %+v", reply)
		}
	})

	t.Run("command result", func(t *testing.T) {
		const id = "failed-command-result"
		server := NewServer(context.Background(), nil)
		server.resultC[id] = make(chan *Result, 1)
		server.resultC[id] <- &Result{Err: errors.New("command failed")}

		client := rpcTestClient(t, server)
		var reply RequestRunCommandResultReply
		err := client.Call("Server.RequestRunCommandResult", RequestRunCommandResultArgs{ID: id}, &reply)
		if err != nil {
			t.Fatalf("RequestRunCommandResult RPC returned error: %v", err)
		}
		if !reply.Complete || reply.Error == "" {
			t.Fatalf("expected confirmed terminal failure, got %+v", reply)
		}
	})
}

func TestRequestServerResultReturnsPeerPort(t *testing.T) {
	const id = "peer-port-result"
	server := NewServer(context.Background(), nil)
	server.resultC[id] = make(chan *Result, 1)
	server.resultC[id] <- &Result{PeerPort: 43210}

	client := rpcTestClient(t, server)
	var reply RequestServerResultReply
	if err := client.Call("Server.RequestServerResult", RequestServerResultArgs{ID: id}, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.PeerPort != 43210 {
		t.Fatalf("peer port = %d, want 43210", reply.PeerPort)
	}
}
