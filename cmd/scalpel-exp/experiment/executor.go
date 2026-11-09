package experiment

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/hendrikcech/netscalpel/pkg"
)

type Executor struct {
	IP        string
	RpcClient *rpc.Client
	// TimestampConfig optionally overrides each scheduled sender's local config.
	TimestampConfig *pkg.TimestampConfig
	G               *errgroup.Group
	Clients         []pkg.Client
	// DryRun is the existing schedule golden-test mode: it records clients
	// without running them, opening sockets, or sleeping.
	DryRun bool
	// DeferStart separates planning from execution so the complete initial
	// manifest can be written before Start launches any measurements.
	DeferStart bool
	Started    bool
	Manifest   *Manifest
	mu         sync.Mutex
	records    []*Measurement
	runErrors  []error
	ctx        context.Context
}

func NewExecutor(ctx context.Context, ip string, rpcClient *rpc.Client) *Executor {
	return &Executor{
		IP:        ip,
		RpcClient: rpcClient,
		G:         new(errgroup.Group),
		Clients:   make([]pkg.Client, 0),
		ctx:       ctx,
	}
}

func (e *Executor) RunClient(client pkg.Client) {
	if sender, ok := client.(*pkg.SenderClient); ok && e.TimestampConfig != nil {
		sender.TimestampConfig = *e.TimestampConfig
	}
	e.Clients = append(e.Clients, client)
	m := describe(client)
	e.records = append(e.records, m)
	if e.DryRun {
		return
	}
	if !e.DeferStart {
		e.launch(client, m)
	}
}

func (e *Executor) Start() {
	if e.DryRun || e.Started {
		return
	}
	e.Started = true
	for i, c := range e.Clients {
		e.launch(c, e.records[i])
	}
}

func (e *Executor) launch(c pkg.Client, m *Measurement) {
	e.Started = true
	e.G.Go(func() error {
		e.mu.Lock()
		now := time.Now().UTC()
		m.ExecutionStart = &now
		m.Status = "running"
		e.mu.Unlock()
		err := c.Run(e.ctx, e.RpcClient)
		e.mu.Lock()
		defer e.mu.Unlock()
		end := time.Now().UTC()
		m.ExecutionEnd = &end
		m.Status = "ran"
		if err != nil {
			e.runErrors = append(e.runErrors, err)
			m.Status = "failed"
			if e.ctx.Err() != nil {
				m.Status = "cancelled"
			}
			m.Errors = append(m.Errors, errorRecord(m.Endpoint, "run", err))
		}
		return err
	})
}

// Wait preserves all measurement failures, including those concurrent with
// cancellation; errgroup.Wait alone returns only the first error.
func (e *Executor) Wait() error {
	firstErr := e.G.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.runErrors) == 0 {
		return firstErr
	}
	return errors.Join(e.runErrors...)
}

// Rebase translates a not-yet-started schedule so its earliest event starts at
// start. All offsets and durations, including capture lead-in, stay intact.
func (e *Executor) Rebase(start time.Time) error {
	if e.Started {
		return fmt.Errorf("cannot rebase a started schedule")
	}
	if start.IsZero() || len(e.Clients) == 0 {
		return fmt.Errorf("rebasing requires a nonempty schedule and a nonzero start")
	}
	var earliest time.Time
	for _, client := range e.Clients {
		t := client.StartTime()
		if t.IsZero() {
			return fmt.Errorf("cannot rebase unscheduled client %T", client)
		}
		if earliest.IsZero() || t.Before(earliest) {
			earliest = t
		}
	}
	shift := start.Sub(earliest)
	for i, client := range e.Clients {
		client.SetStartTime(client.StartTime().Add(shift))
		if i >= len(e.records) {
			continue
		}
		record := e.records[i]
		if !record.ScheduledStart.IsZero() {
			record.ScheduledStart = record.ScheduledStart.Add(shift)
		}
		if !record.ScheduledEnd.IsZero() {
			record.ScheduledEnd = record.ScheduledEnd.Add(shift)
		}
	}
	return nil
}

// AbortServerTests asks the server to cancel every test this executor
// scheduled, so server-side goroutines end promptly after a user abort
// instead of running to their natural end. It is
// called once the round context is cancelled, so the RPCs run on a short
// fresh context.
func (e *Executor) AbortServerTests() error {
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	g := new(errgroup.Group)
	for _, client := range e.Clients {
		g.Go(func() error {
			return client.Abort(ctx, e.RpcClient)
		})
	}
	return g.Wait()
}

func (e *Executor) GatherResults() error {
	errs := make([]error, len(e.Clients))
	var wg sync.WaitGroup
	for i, client := range e.Clients {
		idx, c := i, client
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.Gather(e.ctx, e.RpcClient)
			errs[idx] = err
			e.mu.Lock()
			defer e.mu.Unlock()
			m := e.records[idx]
			if err != nil {
				var completed *pkg.RemoteExecutionError
				var persistence *pkg.ResultPersistenceError
				if errors.As(err, &completed) || errors.As(err, &persistence) {
					// The server returned a terminal result. The procedure failed,
					// or a local result write failed after consuming it. Remote work
					// is confirmed stopped in either case.
					m.GatherStatus = "complete"
				} else {
					m.GatherStatus = "failed"
				}
				if m.Status != "cancelled" {
					m.Status = "failed"
				}
				m.Errors = append(m.Errors, errorRecord(m.Endpoint, "gather", err))
			} else {
				m.GatherStatus = "complete"
				if m.Status == "ran" {
					m.Status = "complete"
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// ResultsConfirmed reports whether every scheduled client delivered a terminal
// result. A failed terminal result and a local result-writing error after
// consuming it are still confirmation; a transport error is not.
func (e *Executor) ResultsConfirmed() bool {
	for _, record := range e.records {
		if record.GatherStatus != "complete" {
			return false
		}
	}
	return true
}

// Tcpdump schedules paired local and remote packet captures from start for
// the given duration.
func (e *Executor) Tcpdump(resultPath string, start time.Time, duration time.Duration) {
	for _, local := range []bool{true, false} {
		var name string
		if local {
			name = "local"
		} else {
			name = "remote"
		}
		e.RunClient(&pkg.CommandClient{
			Params: pkg.TcpdumpParams{
				Name_:    fmt.Sprintf("tcpdump_%v", name),
				Timeout_: duration,
				// Filter:   "udp",
			},
			Local:    local,
			StartAt:  start,
			LocalDir: resultPath,
		})
	}
}
