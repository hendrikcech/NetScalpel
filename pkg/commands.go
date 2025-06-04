package pkg

import (
	"fmt"
	"log"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

func RunCommand(cmd *exec.Cmd, timeout time.Duration) error {
	// cmd.Stdin = os.DevNull
	// cmd.Stdout = os.DevNull // os.Stdout
	// cmd.Stderr = os.DevNull // os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("Failed to open StdinPipe: %v", err.Error())
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Failed to start cmd: %v", err.Error())
	}

	stdin.Close()

	// log.Printf("Sleep for command %v", timeout)
	time.Sleep(timeout)

	for _, signal := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL} {
		log.Printf("Sending signal %+v", signal)
		if err := cmd.Process.Signal(signal); err != nil {
			return fmt.Errorf("Failed to signal %v: %v", signal, err.Error())
		}
		time.Sleep(2 * time.Second)
	}

	// log.Printf("Terminating tcpdump: waiting")
	// if err := cmd.Wait(); err != nil {
	// 	return fmt.Errorf("Failed to wait for termination: %v", err.Error())
	// }

	// log.Printf("Returning from RunCommand")
	return nil
}

type RunCommandMode int

const (
	Tcpdump RunCommandMode = iota
)

type Command interface {
	Params() CommandParams
	Cmd(resultDir string) (*exec.Cmd, error)
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
func (c *TcpdumpCommand) Cmd(resultDir string) (*exec.Cmd, error) {
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

func (c *TccCommand) Cmd(resultDir string) (*exec.Cmd, error) {
	args := []string{"sudo", "tcc-trace", "--logpath", resultDir}
	for _, ip := range c.Params_.Ips {
		args = append(args, "--ip", ip)
	}
	cmd := exec.Command(args[0], args[1:]...)
	return cmd, nil
}
