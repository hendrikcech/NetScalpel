package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/pkg"
)

var cleanupGobRegistration sync.Once

type cleanupRPCService struct {
	abortCalls  atomic.Int32
	resultCalls atomic.Int32

	abortGate  <-chan struct{}
	resultGate <-chan struct{}
	abortErr   string
	requestErr string
}

func (s *cleanupRPCService) RequestServer(args pkg.RequestServerArgs, reply *pkg.RequestServerReply) error {
	if s.requestErr != "" {
		return errors.New(s.requestErr)
	}
	reply.Port = 1
	return nil
}

func (s *cleanupRPCService) Abort(_ pkg.AbortArgs, _ *pkg.AbortReply) error {
	s.abortCalls.Add(1)
	if s.abortGate != nil {
		<-s.abortGate
	}
	if s.abortErr != "" {
		return errors.New(s.abortErr)
	}
	return nil
}

func (s *cleanupRPCService) RequestServerResult(_ pkg.RequestServerResultArgs, reply *pkg.RequestServerResultReply) error {
	s.resultCalls.Add(1)
	if s.resultGate != nil {
		<-s.resultGate
	}
	reply.Complete = true
	if s.requestErr != "" {
		remoteConfig, _ := (pkg.TimestampConfig{RX: pkg.TimestampHardware}).Normalize()
		reply.Error = s.requestErr
		reply.TimestampReport = pkg.TimestampReport{
			LocalConfig: remoteConfig,
			RX: pkg.TimestampDirectionReport{
				Requested: pkg.TimestampHardware, Source: pkg.TimestampHardware,
				ClockDomain: "nic_phc",
			},
		}
		return nil
	}
	reply.Result = []pkg.MsgRcvd{}
	remoteConfig, _ := (pkg.TimestampConfig{}).Normalize()
	reply.TimestampReport = pkg.TimestampReport{
		LocalConfig: remoteConfig,
		RX: pkg.TimestampDirectionReport{
			Requested:   pkg.TimestampSoftware,
			Source:      pkg.TimestampSoftware,
			ClockDomain: "system_realtime",
			PacketCount: 0,
		},
	}
	return nil
}

func newCleanupRPCClient(t *testing.T, service *cleanupRPCService) *rpc.Client {
	t.Helper()
	cleanupGobRegistration.Do(pkg.RegisterGob)

	server := rpc.NewServer()
	if err := server.RegisterName("Server", service); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.Accept(listener)
	client, err := rpc.Dial("tcp", listener.Addr().String())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.Close()
		_ = listener.Close()
	})
	return client
}

func newStartedCleanupClient(t *testing.T, rpcClient *rpc.Client) *pkg.SenderClient {
	t.Helper()
	config, err := (pkg.TimestampConfig{}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	client := &pkg.SenderClient{
		IP:        "127.0.0.1",
		Out:       filepath.Join(t.TempDir(), "result.csv"),
		Direction: pkg.UL,
		StartAt:   time.Now().Add(-time.Second),
		Sender: &pkg.BurstSender{Params: pkg.BurstParams{
			Timeout: time.Second,
			Num:     1,
		}},
		TimestampConfig: config,
	}
	err = client.Run(context.Background(), rpcClient)
	if err == nil || !strings.Contains(err.Error(), "already passed") {
		t.Fatalf("Run error = %v, want the deliberate local start-time failure", err)
	}
	if !client.RemoteStarted() {
		t.Fatal("successful RequestServer reply did not mark remote work started")
	}
	return client
}

func TestRunFailureAttemptsAbortAndGatherRetainingEvidence(t *testing.T) {
	service := &cleanupRPCService{abortErr: "injected abort failure"}
	rpcClient := newCleanupRPCClient(t, service)
	config, err := (pkg.TimestampConfig{}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	client := &pkg.SenderClient{
		IP:        "127.0.0.1",
		Out:       filepath.Join(t.TempDir(), "result.csv"),
		Direction: pkg.UL,
		StartAt:   time.Now().Add(-time.Second),
		Sender: &pkg.BurstSender{Params: pkg.BurstParams{
			Timeout: time.Second,
			Num:     1,
		}},
		TimestampConfig: config,
	}
	err = runSenderAndGather(context.Background(), rpcClient, client)
	if err == nil || !strings.Contains(err.Error(), "already passed") {
		t.Errorf("joined error lost original Run failure: %v", err)
	}
	for _, want := range []string{"already passed", "injected abort failure", "local measurement failed; CSV output withheld"} {
		if !strings.Contains(fmt.Sprint(err), want) {
			t.Errorf("joined cleanup error missing %q: %v", want, err)
		}
	}
	if service.abortCalls.Load() != 1 || service.resultCalls.Load() != 1 {
		t.Fatalf("cleanup calls: abort=%d gather=%d", service.abortCalls.Load(), service.resultCalls.Load())
	}
	if got := client.TimestampReport().RX; got.Source != pkg.TimestampSoftware || got.ClockDomain != "system_realtime" {
		t.Fatalf("remote timestamp evidence was not retained: %+v", got)
	}
	if _, err := os.Stat(client.Out); !os.IsNotExist(err) {
		t.Fatalf("local Run failure produced CSV output: %v", err)
	}
}

func TestRequestServerRejectionDoesNotAbortOrGatherUnprovenID(t *testing.T) {
	service := &cleanupRPCService{requestErr: "duplicate measurement ID"}
	rpcClient := newCleanupRPCClient(t, service)
	config, err := (pkg.TimestampConfig{}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	client := &pkg.SenderClient{
		IP: "127.0.0.1", Out: filepath.Join(t.TempDir(), "result.csv"), Direction: pkg.UL,
		Sender:          &pkg.BurstSender{Params: pkg.BurstParams{Timeout: time.Second, Num: 1}},
		TimestampConfig: config,
	}

	err = runSenderAndGather(context.Background(), rpcClient, client)
	if err == nil || !strings.Contains(err.Error(), "duplicate measurement ID") {
		t.Fatalf("Run error = %v, want the server rejection", err)
	}
	if client.RemoteStarted() {
		t.Fatal("server error reply was incorrectly treated as proof that remote work was registered")
	}
	if service.abortCalls.Load() != 0 || service.resultCalls.Load() != 0 {
		t.Fatalf("cleanup calls: abort=%d gather=%d", service.abortCalls.Load(), service.resultCalls.Load())
	}
}

func TestCleanupDoesNotGatherWithoutRemoteStartEvidence(t *testing.T) {
	service := &cleanupRPCService{}
	rpcClient := newCleanupRPCClient(t, service)
	client := &pkg.SenderClient{ID: "caller-assigned-id"}
	if err := cleanupRemoteAfterRunFailure(rpcClient, client, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if service.abortCalls.Load() != 0 || service.resultCalls.Load() != 0 {
		t.Fatalf("cleanup used an unproven remote ID: abort=%d gather=%d", service.abortCalls.Load(), service.resultCalls.Load())
	}
}

func TestCleanupRPCsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name        string
		blockAbort  bool
		blockResult bool
	}{
		{name: "abort timeout", blockAbort: true},
		{name: "gather timeout", blockResult: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := make(chan struct{})
			service := &cleanupRPCService{}
			if tc.blockAbort {
				service.abortGate = gate
			}
			if tc.blockResult {
				service.resultGate = gate
			}
			rpcClient := newCleanupRPCClient(t, service)
			client := newStartedCleanupClient(t, rpcClient)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate) }) }
			defer release()

			started := time.Now()
			err := cleanupRemoteAfterRunFailure(rpcClient, client, 30*time.Millisecond)
			elapsed := time.Since(started)
			if err == nil {
				t.Fatal("expected bounded cleanup RPC to report an error")
			}
			if elapsed > 500*time.Millisecond {
				t.Fatalf("cleanup exceeded its bound: %v", elapsed)
			}
			if service.abortCalls.Load() != 1 {
				t.Fatalf("abort calls = %d, want one", service.abortCalls.Load())
			}
			if tc.blockResult && service.resultCalls.Load() != 1 {
				t.Fatalf("gather calls = %d, want one", service.resultCalls.Load())
			}
		})
	}
}
