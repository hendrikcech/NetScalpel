package experiment

import (
	"context"
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
	G         *errgroup.Group
	Clients   []pkg.Client
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
			m.Status = "failed"
			if e.ctx.Err() != nil {
				m.Status = "cancelled"
			}
			m.Errors = append(m.Errors, errorRecord(m.Endpoint, "run", err))
		}
		return err
	})
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
	g := new(errgroup.Group)
	for i, client := range e.Clients {
		g.Go(func() error {
			err := client.Gather(e.ctx, e.RpcClient)
			e.mu.Lock()
			defer e.mu.Unlock()
			m := e.records[i]
			if err != nil {
				m.GatherStatus = "failed"
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
			return err
		})
	}
	return g.Wait()
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
