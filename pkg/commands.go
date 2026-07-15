package pkg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// MonitorCommand runs cmd until timeout elapses, ctx is cancelled, or the
// command exits on its own. In the first two cases it stops the command by
// escalating SIGINT -> SIGTERM -> SIGKILL, moving to the next signal only if
// the command has not exited within 2s. Signals go to the whole process group
// so that children (e.g. tcpdump started via sudo) are reached as well; if
// group signalling is not permitted (setuid sudo child), the process itself
// is signalled and relays as usual. The command is always reaped via Wait.
func MonitorCommand(ctx context.Context, cmd *exec.Cmd, timeout time.Duration) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("Failed to open StdinPipe: %w", err)
	}

	// Own process group so signals reach children too
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Failed to start cmd: %w", err)
	}

	stdin.Close()

	waitC := make(chan error, 1)
	go func() { waitC <- cmd.Wait() }()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	case err := <-waitC:
		// Command exited before its timeout; surface a failure (e.g. tcpdump
		// exiting immediately because sudo is not available).
		if err != nil {
			return fmt.Errorf("Command exited prematurely: %w", err)
		}
		return nil
	}

	for _, signal := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL} {
		if err := syscall.Kill(-cmd.Process.Pid, signal); err != nil {
			// EPERM: group contains a process we may not signal (sudo child);
			// signal the direct child instead, which relays SIGINT/SIGTERM.
			if err := cmd.Process.Signal(signal); err != nil && !errors.Is(err, os.ErrProcessDone) {
				slog.DebugContext(ctx, "Failed to signal command", "signal", signal, "error", err)
			}
		}
		select {
		case <-waitC:
			// Exit status of a signalled command is expected to be non-zero
			slog.DebugContext(ctx, "Command terminated", "signal", signal)
			return nil
		case <-time.After(2 * time.Second):
			slog.WarnContext(ctx, "Command did not exit after signal, escalating", "signal", signal)
		}
	}

	return fmt.Errorf("Command still running after SIGKILL")
}

type RunCommandMode int

const (
	Tcpdump RunCommandMode = iota
)

type Command interface {
	Params() CommandParams
	Exec(resultDir string) (*exec.Cmd, error)
}

type CommandParams interface {
	Name() string
	Timeout() time.Duration
}

type TcpdumpParams struct {
	Name_    string
	Timeout_ time.Duration
	Filter   string
}

var _ CommandParams = (*TcpdumpParams)(nil)

func (p TcpdumpParams) Name() string {
	return p.Name_
}

func (p TcpdumpParams) Timeout() time.Duration {
	return p.Timeout_
}

type TcpdumpCommand struct {
	Params_ TcpdumpParams
}

var _ Command = (*TcpdumpCommand)(nil)

func (c *TcpdumpCommand) Params() CommandParams {
	return c.Params_
}

// TODO: only listen on specific interface
// TODO: use sudo?
func (c *TcpdumpCommand) Exec(resultDir string) (*exec.Cmd, error) {
	resultPath := filepath.Join(resultDir, c.Params().Name()+".pcap")
	cmd := exec.Command("sudo", "tcpdump", "-i", "any", "-s", "150", "-w", resultPath, c.Params_.Filter)
	return cmd, nil
}

type TccParams struct {
	Name_    string
	Timeout_ time.Duration
	Ips      []string
}

var _ CommandParams = (*TccParams)(nil)

func (p TccParams) Name() string {
	return p.Name_
}

func (p TccParams) Timeout() time.Duration {
	return p.Timeout_
}

type TccCommand struct {
	Params_ TccParams
}

var _ Command = (*TccCommand)(nil)

func (c *TccCommand) Params() CommandParams {
	return c.Params_
}

func (c *TccCommand) Exec(resultDir string) (*exec.Cmd, error) {
	args := []string{"sudo", "tcc-trace", "--logpath", resultDir}
	for _, ip := range c.Params_.Ips {
		args = append(args, "--ip", ip)
	}
	cmd := exec.Command(args[0], args[1:]...)
	return cmd, nil
}
