package experiment

import (
	"context"
	"errors"
	"net/rpc"
	"testing"
	"time"

	"github.com/hendrikcech/netscalpel/pkg"
)

type gatherResultsTestClient struct {
	gather func(context.Context) error
}

func (c *gatherResultsTestClient) Run(context.Context, *rpc.Client) error { return nil }
func (c *gatherResultsTestClient) Gather(ctx context.Context, _ *rpc.Client) error {
	return c.gather(ctx)
}
func (*gatherResultsTestClient) Abort(context.Context, *rpc.Client) error { return nil }
func (*gatherResultsTestClient) Summary() string                          { return "test" }
func (*gatherResultsTestClient) StartTime() time.Time                     { return time.Time{} }
func (*gatherResultsTestClient) SetStartTime(time.Time)                   {}

func TestGatherResultsJoinsErrorsInClientOrderAndConfirmsConsumedResults(t *testing.T) {
	remoteErr := &pkg.RemoteExecutionError{Err: errors.New("remote execution failed")}
	persistenceErr := &pkg.ResultPersistenceError{Err: errors.New("result write failed")}

	e := NewExecutor(context.Background(), "", nil)
	e.RunClient(&gatherResultsTestClient{gather: func(context.Context) error { return remoteErr }})
	e.RunClient(&gatherResultsTestClient{gather: func(context.Context) error { return persistenceErr }})
	if err := e.G.Wait(); err != nil {
		t.Fatal(err)
	}

	err := e.GatherResults()
	if err == nil {
		t.Fatal("GatherResults returned nil")
	}
	if want := errors.Join(remoteErr, persistenceErr).Error(); err.Error() != want {
		t.Fatalf("joined errors = %q, want client-ordered %q", err, want)
	}
	var gotPersistence *pkg.ResultPersistenceError
	if !errors.As(err, &gotPersistence) || gotPersistence != persistenceErr {
		t.Fatalf("joined error does not expose persistence failure: %v", err)
	}
	if !e.ResultsConfirmed() {
		t.Fatal("terminal results were not confirmed")
	}
	for i, record := range e.records {
		if record.GatherStatus != "complete" || record.Status != "failed" || len(record.Errors) != 1 || record.Errors[0].Phase != "gather" {
			t.Errorf("client %d record lost terminal failure: %+v", i, record)
		}
	}
}

func TestGatherResultsCancellationRemainsUnconfirmed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	e := NewExecutor(ctx, "", nil)
	e.RunClient(&gatherResultsTestClient{gather: func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}})
	if err := e.G.Wait(); err != nil {
		t.Fatal(err)
	}

	gatherDone := make(chan error, 1)
	go func() { gatherDone <- e.GatherResults() }()
	<-started
	cancel()
	err := <-gatherDone
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GatherResults error = %v, want context cancellation", err)
	}
	if e.ResultsConfirmed() || e.records[0].GatherStatus != "failed" {
		t.Fatalf("cancelled gather was treated as confirmed: %+v", e.records[0])
	}
}
