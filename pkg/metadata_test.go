package pkg

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/rpc"
	"strings"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/internal/buildinfo"
)

type OldMetadataServer struct{}

func (*OldMetadataServer) Ping(_ MetadataArgs, _ *MetadataReply) error { return nil }

func metadataRPC(t *testing.T, receiver any) *rpc.Client {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("Server", receiver); err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() { server.ServeConn(a); close(done) }()
	c := rpc.NewClient(b)
	t.Cleanup(func() { c.Close(); <-done })
	return c
}

func TestMetadataReportsRunningServer(t *testing.T) {
	c := metadataRPC(t, NewServer(context.Background(), nil))
	m, err := FetchMetadata(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := json.Marshal(buildinfo.Read())
	got, _ := json.Marshal(m.Build)
	if string(want) != string(got) || m.Host.Hostname == "" || m.Host.LogicalCPUs < 1 || m.Load.Time.IsZero() {
		t.Fatalf("bad server metadata: %+v", m)
	}
}

func TestMetadataOldServerAndBoundedCall(t *testing.T) {
	c := metadataRPC(t, &OldMetadataServer{})
	if _, err := FetchMetadata(context.Background(), c); err == nil || !strings.Contains(err.Error(), "method") {
		t.Fatalf("old server error: %v", err)
	}
	a, b := net.Pipe()
	stalled := rpc.NewClient(b)
	defer stalled.Close()
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := a.Read(buf); err != nil {
				close(done)
				return
			}
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := FetchMetadata(ctx, stalled); err == nil {
		t.Fatal("stalled metadata succeeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("metadata ignored context")
	}
	a.Close()
	<-done
}

func TestMetadataBoundsUnreadTransport(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	c := rpc.NewClient(b)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := FetchMetadata(ctx, c); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error: %v", err)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("blocked transport write exceeded deadline")
	}
	if err := c.Call("Server.Metadata", MetadataArgs{}, &MetadataReply{}); err == nil {
		t.Fatal("cancelled connection remained usable")
	}
}

func TestBoundedCallReleasesBlockedRequestLock(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	c := rpc.NewClient(b)
	defer c.Close()
	first := make(chan error, 1)
	go func() { first <- c.Call("Server.Metadata", MetadataArgs{}, &MetadataReply{}) }()
	// Read just part of the first request: it now holds the request lock while
	// blocked writing its remaining gob payload to the unread pipe.
	buf := make([]byte, 1)
	if _, err := a.Read(buf); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := BoundedCall(ctx, c, "Server.Metadata", MetadataArgs{}, &MetadataReply{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error: %v", err)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("request lock exceeded deadline")
	}
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("original blocked writer leaked")
	}
}
