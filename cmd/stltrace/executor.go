package main

import (
	"bufio"
	"context"
	"fmt"
	"net/rpc"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"

	"gitlab.lrz.de/cm/starlink/netmeas/pkg"
)

type Executor struct {
	IP        string
	RpcClient *rpc.Client
	G         *errgroup.Group
	Clients   []pkg.Client
	ctx       context.Context
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
	e.G.Go(func() error { return client.Run(e.ctx, e.RpcClient) })
}

func (e *Executor) GatherResults() error {
	g := new(errgroup.Group)
	for _, client := range e.Clients {
		g.Go(func() error {
			return client.Gather(e.ctx, e.RpcClient)
		})
	}
	return g.Wait()
}

func (e *Executor) WriteInfo(path string) error {
	infoPath := filepath.Join(path, "info.txt")
	f, err := os.Create(infoPath)
	if err != nil {
		return fmt.Errorf("Failed creating %v: %v", infoPath, err.Error())
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	for _, client := range e.Clients {
		w.WriteString(client.Summary())
		w.WriteString("\n")
	}
	return nil
}

func (e *Executor) tcpdump(resultPath string, start time.Time, duration time.Duration) {
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
