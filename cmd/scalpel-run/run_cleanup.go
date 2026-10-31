package main

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"time"

	"github.com/hendrikcech/netscalpel/pkg"
)

const remoteCleanupTimeout = 2 * time.Second

func runSenderAndGather(ctx context.Context, rpcClient *rpc.Client, client *pkg.SenderClient) error {
	if err := client.Run(ctx, rpcClient); err != nil {
		runErr := fmt.Errorf("Run error: %w", err)
		cleanupErr := cleanupRemoteAfterRunFailure(rpcClient, client, remoteCleanupTimeout)
		return errors.Join(runErr, cleanupErr)
	}
	if err := client.Gather(ctx, rpcClient); err != nil {
		return fmt.Errorf("Gather error: %w", err)
	}
	return nil
}

// cleanupRemoteAfterRunFailure makes one bounded abort and one bounded gather
// attempt only after RequestServer confirmed that remote work was registered.
// SenderClient.Gather retains the peer timestamp report but withholds CSV output
// when Run failed locally.
func cleanupRemoteAfterRunFailure(rpcClient *rpc.Client, client *pkg.SenderClient, timeout time.Duration) error {
	if !client.RemoteStarted() {
		return nil
	}
	if timeout <= 0 {
		timeout = remoteCleanupTimeout
	}

	var cleanupErrs []error
	abortCtx, cancelAbort := context.WithTimeout(context.Background(), timeout)
	if err := client.Abort(abortCtx, rpcClient); err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("abort after run failure: %w", err))
	}
	cancelAbort()

	gatherCtx, cancelGather := context.WithTimeout(context.Background(), timeout)
	if err := client.Gather(gatherCtx, rpcClient); err != nil {
		cleanupErrs = append(cleanupErrs, fmt.Errorf("gather after run failure: %w", err))
	}
	cancelGather()
	return errors.Join(cleanupErrs...)
}
