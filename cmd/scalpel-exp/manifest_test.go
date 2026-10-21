package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/experiment"
	"github.com/hendrikcech/netscalpel/cmd/scalpel-exp/procedures"
	"github.com/hendrikcech/netscalpel/pkg"
)

type LegacyServer struct{}

func (*LegacyServer) RequestSlog(_ pkg.RequestSlogArgs, _ *pkg.RequestSlogReply) error { return nil }

type DifferentVersionServer struct{ LegacyServer }

func (*DifferentVersionServer) Metadata(_ pkg.MetadataArgs, reply *pkg.MetadataReply) error {
	m := pkg.LocalMetadata()
	m.Build.Version = "1.9.0"
	b, err := json.Marshal(m)
	reply.JSON = b
	return err
}

type manifestCheckClient struct {
	dir     string
	initial chan error
}

func (c *manifestCheckClient) Run(context.Context, *rpc.Client) error {
	b, err := os.ReadFile(filepath.Join(c.dir, "info.json"))
	if err == nil {
		var m experiment.Manifest
		err = json.Unmarshal(b, &m)
		if err == nil && (m.Status != "scheduled" || len(m.Measurements) != 1 || m.Measurements[0].ExecutionStart != nil || m.Parameters["duration"] != float64(17)) {
			err = errors.New("initial manifest missing schedule/defaults")
		}
	}
	c.initial <- err
	return nil
}
func (*manifestCheckClient) Gather(context.Context, *rpc.Client) error { return nil }
func (*manifestCheckClient) Abort(context.Context, *rpc.Client) error  { return nil }
func (*manifestCheckClient) Summary() string                           { return "summary" }

func TestInvocationManifestOldServerFallback(t *testing.T) {
	s := rpc.NewServer()
	if err := s.RegisterName("Server", &LegacyServer{}); err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	done := make(chan struct{})
	go func() { s.ServeConn(a); close(done) }()
	rpcClient := rpc.NewClient(b)
	defer func() { rpcClient.Close(); <-done }()
	ctx := context.Background()
	e := experiment.NewExecutor(ctx, "127.0.0.1", rpcClient)
	check := &manifestCheckClient{initial: make(chan error, 1)}
	proc := procedures.Procedure{Name: "test", Mode: procedures.OncePerRound, Params: []procedures.ParamSpec{{Name: "duration", Default: uint(17)}}, Run: func(e *experiment.Executor, _ time.Time, path string, _ experiment.ParamMap) error {
		check.dir = path
		e.RunClient(check)
		return nil
	}}
	c := &Client{Procedure: "test", Results: t.TempDir(), Rounds: 1}
	defer func() {
		if c.slogFile != nil {
			c.slogFile.Close()
		}
	}()
	path, scheduleErr := c.scheduleProcedureInvocation(ctx, e, time.Now(), proc, nil)
	if scheduleErr != nil {
		t.Fatal(scheduleErr)
	}
	c.finishProcedureInvocation(ctx, e, rpcClient, path)
	if err := <-check.initial; err != nil {
		t.Fatal(err)
	}
	bts, err := os.ReadFile(filepath.Join(path, "info.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m experiment.Manifest
	if err := json.Unmarshal(bts, &m); err != nil {
		t.Fatal(err)
	}
	if m.Status != "complete" || m.ExperimentID == "" || m.InvocationID == "" || m.Round != 1 || m.Client.Metadata == nil || m.Client.After == nil || m.Server.Metadata != nil || !strings.Contains(strings.Join(m.Warnings, " "), "older server") || len(m.Errors) != 0 {
		t.Fatalf("bad fallback: %s", bts)
	}
	if m.ExecutionEnd == nil || m.Client.After.Time.Before(*m.ExecutionEnd) {
		t.Fatal("AFTER snapshot precedes execution end")
	}
}

func TestInvocationVersionMismatchAndSchedulingFailure(t *testing.T) {
	for _, scheduleFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "mismatch_not_fatal", true: "schedule_failure_finalized"}[scheduleFails], func(t *testing.T) {
			s := rpc.NewServer()
			if err := s.RegisterName("Server", &DifferentVersionServer{}); err != nil {
				t.Fatal(err)
			}
			a, b := net.Pipe()
			done := make(chan struct{})
			go func() { s.ServeConn(a); close(done) }()
			rpcClient := rpc.NewClient(b)
			defer func() { rpcClient.Close(); <-done }()
			ctx := context.Background()
			e := experiment.NewExecutor(ctx, "127.0.0.1", rpcClient)
			proc := procedures.Procedure{Name: "test", Mode: procedures.OncePerRound, Run: func(*experiment.Executor, time.Time, string, experiment.ParamMap) error {
				if scheduleFails {
					return errors.New("schedule failed")
				}
				return nil
			}}
			c := &Client{Procedure: "test", Results: t.TempDir(), Rounds: 1}
			defer func() {
				if c.slogFile != nil {
					c.slogFile.Close()
				}
			}()
			path, scheduleErr := c.scheduleProcedureInvocation(ctx, e, time.Now(), proc, nil)
			finishErr := c.finishProcedureInvocation(ctx, e, rpcClient, path)
			if (scheduleErr != nil) != scheduleFails || (finishErr != nil) != scheduleFails {
				t.Fatalf("schedule=%v finish=%v", scheduleErr, finishErr)
			}
			bts, err := os.ReadFile(filepath.Join(path, "info.json"))
			if err != nil {
				t.Fatal(err)
			}
			var m experiment.Manifest
			if err := json.Unmarshal(bts, &m); err != nil {
				t.Fatal(err)
			}
			want := "complete"
			if scheduleFails {
				want = "failed"
			}
			if m.Status != want || m.Client.Role != "client" || m.Server.Role != "server" || m.Server.Metadata == nil || m.Server.Metadata.Build.Version != "1.9.0" || m.Client.After == nil || m.Server.After == nil || !strings.Contains(strings.Join(m.Warnings, " "), "version mismatch") {
				t.Fatalf("bad mismatch handling: %s", bts)
			}
			if scheduleFails && (len(m.Errors) != 1 || m.Errors[0].Phase != "schedule" || e.Started) {
				t.Fatalf("bad scheduling failure: %s", bts)
			}
		})
	}
}
