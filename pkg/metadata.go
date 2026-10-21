package pkg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/rpc"
	"time"

	"github.com/hendrikcech/netscalpel/internal/buildinfo"
	"github.com/hendrikcech/netscalpel/internal/hostinfo"
)

type Metadata struct {
	Build buildinfo.Info    `json:"build"`
	Host  hostinfo.Host     `json:"host"`
	Load  hostinfo.Snapshot `json:"load"`
}
type MetadataArgs struct{}

// JSON preserves nullable booleans across gob (which omits zero values).
type MetadataReply struct{ JSON []byte }

func LocalMetadata() Metadata {
	return Metadata{Build: buildinfo.Read(), Host: hostinfo.Basics(), Load: hostinfo.Load()}
}

func (s *Server) Metadata(_ MetadataArgs, reply *MetadataReply) error {
	b, err := json.Marshal(LocalMetadata())
	reply.JSON = b
	return err
}

// FetchMetadata is optional and bounded, including for old or stalled peers.
func FetchMetadata(ctx context.Context, client *rpc.Client) (*Metadata, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var reply MetadataReply
	if err := BoundedCall(ctx, client, "Server.Metadata", MetadataArgs{}, &reply); err != nil {
		return nil, err
	}
	var m Metadata
	if err := json.Unmarshal(reply.JSON, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// BoundedCall bounds request-lock acquisition, transport writes and replies.
// On context cancellation it closes the connection and joins the call goroutine;
// callers must reconnect before reusing the control channel. A normal RPC error
// (including an unsupported method on an older server) leaves it usable.
func BoundedCall(ctx context.Context, client *rpc.Client, method string, args, reply any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- client.Call(method, args, reply) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		client.Close()
		<-done
		return fmt.Errorf("RPC connection closed on cancellation: %w", ctx.Err())
	}
}
