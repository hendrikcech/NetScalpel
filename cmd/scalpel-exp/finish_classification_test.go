package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/pkg"
)

type finishClassificationRPCServer struct {
	log    string
	logErr error
}

func (s *finishClassificationRPCServer) RequestSlog(_ pkg.RequestSlogArgs, reply *pkg.RequestSlogReply) error {
	if s.logErr != nil {
		return s.logErr
	}
	reply.Log = s.log
	return nil
}

type finishClassificationClient struct {
	runErr    error
	gatherErr error
	gather    func() error
}

func (c *finishClassificationClient) Run(context.Context, *rpc.Client) error { return c.runErr }
func (c *finishClassificationClient) Gather(context.Context, *rpc.Client) error {
	if c.gather != nil {
		return c.gather()
	}
	return c.gatherErr
}
func (*finishClassificationClient) Abort(context.Context, *rpc.Client) error { return nil }
func (*finishClassificationClient) Summary() string                          { return "test" }
func (*finishClassificationClient) StartTime() time.Time                     { return time.Time{} }
func (*finishClassificationClient) SetStartTime(time.Time)                   {}

func newFinishClassificationRPC(t *testing.T, service *finishClassificationRPCServer) *rpc.Client {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("Server", service); err != nil {
		t.Fatal(err)
	}
	serverConn, clientConn := net.Pipe()
	done := make(chan struct{})
	go func() {
		server.ServeConn(serverConn)
		close(done)
	}()
	client := rpc.NewClient(clientConn)
	t.Cleanup(func() {
		_ = client.Close()
		<-done
	})
	return client
}

func finishClassification(t *testing.T, ctx context.Context, rpcClient *rpc.Client, resultPath string, gatherErrors ...error) invocationFinish {
	t.Helper()
	clients := make([]pkg.Client, len(gatherErrors))
	for i, gatherErr := range gatherErrors {
		clients[i] = &finishClassificationClient{gatherErr: gatherErr}
	}
	return finishClassificationClients(t, ctx, rpcClient, resultPath, clients...)
}

func finishClassificationClients(t *testing.T, ctx context.Context, rpcClient *rpc.Client, resultPath string, clients ...pkg.Client) invocationFinish {
	t.Helper()
	e := experiment.NewExecutor(ctx, "", rpcClient)
	e.Manifest = &experiment.Manifest{SchemaVersion: 1, Status: "running"}
	for _, client := range clients {
		e.RunClient(client)
	}
	c := &Client{Results: resultPath, Rounds: 1}
	t.Cleanup(func() {
		if c.slogFile != nil {
			_ = c.slogFile.Close()
		}
	})
	return c.finishProcedureInvocationClassified(ctx, e, rpcClient, resultPath)
}

func TestFinishClassifiedCombinedGatherFailuresRemainFatalAndConfirmed(t *testing.T) {
	remoteErr := &pkg.RemoteExecutionError{Err: errors.New("remote execution failed")}
	persistenceErr := &pkg.ResultPersistenceError{Err: errors.New("result write failed")}
	rpcClient := newFinishClassificationRPC(t, &finishClassificationRPCServer{})
	result := finishClassification(t, context.Background(), rpcClient, t.TempDir(), remoteErr, persistenceErr)

	var gotPersistence *pkg.ResultPersistenceError
	if !errors.As(result.err, &gotPersistence) || gotPersistence != persistenceErr {
		t.Fatalf("finish error lost persistence failure: %v", result.err)
	}
	if !result.fatal || !result.failed || !result.remoteConfirmed {
		t.Fatalf("unexpected finish classification: %+v", result)
	}
}

func TestFinishClassifiedServerLogRPCFailureIsNotFatalWhenConfirmed(t *testing.T) {
	rpcClient := newFinishClassificationRPC(t, &finishClassificationRPCServer{logErr: errors.New("log retrieval unavailable")})
	result := finishClassification(t, context.Background(), rpcClient, t.TempDir(), nil)

	if result.err == nil || result.fatal || !result.failed || !result.remoteConfirmed {
		t.Fatalf("unexpected finish classification: %+v", result)
	}
}

func TestFinishClassifiedServerLogWriteFailureIsFatal(t *testing.T) {
	rpcClient := newFinishClassificationRPC(t, &finishClassificationRPCServer{log: "server log"})
	resultPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(resultPath, "scalpel_server.log"), 0700); err != nil {
		t.Fatal(err)
	}
	result := finishClassification(t, context.Background(), rpcClient, resultPath, nil)

	var persistenceErr *pkg.ResultPersistenceError
	if !errors.As(result.err, &persistenceErr) {
		t.Fatalf("finish error does not identify local persistence failure: %v", result.err)
	}
	if !result.fatal || !result.failed || !result.remoteConfirmed {
		t.Fatalf("unexpected finish classification: %+v", result)
	}
}

func TestFinishClassifiedCancellationDoesNotMaskGatherFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		failed bool
	}{
		{"remote failure", &pkg.RemoteExecutionError{Err: errors.New("remote failed")}, true},
		{"persistence failure", &pkg.ResultPersistenceError{Err: errors.New("disk full")}, true},
		{"cancellation only", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rpcClient := newFinishClassificationRPC(t, &finishClassificationRPCServer{})
			result := finishClassificationClients(t, ctx, rpcClient, t.TempDir(),
				&finishClassificationClient{gatherErr: tc.err},
				&finishClassificationClient{gather: func() error {
					cancel()
					return fmt.Errorf("gather interrupted: %w", ctx.Err())
				}},
			)
			if result.failed != tc.failed || !result.cancelled {
				t.Fatalf("failure must take precedence over concurrent cancellation: %+v", result)
			}
			if tc.err != nil && !errors.Is(result.err, tc.err) {
				t.Fatalf("independent failure lost: %v", result.err)
			}
		})
	}
}

func TestFinishClassifiedWaitRetainsEveryRunFailure(t *testing.T) {
	rpcClient := newFinishClassificationRPC(t, &finishClassificationRPCServer{})
	failure := errors.New("independent measurement failure")
	result := finishClassificationClients(t, context.Background(), rpcClient, t.TempDir(),
		&finishClassificationClient{runErr: context.Canceled},
		&finishClassificationClient{runErr: failure},
	)
	if !errors.Is(result.err, failure) || !errors.Is(result.err, context.Canceled) || !result.failed {
		t.Fatalf("concurrent run failures lost: %+v", result)
	}
}
