package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

type compressionExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// GetCompressionCommand returns a wrapper over the kopia policy set
func GetCompressionCommand(path, compression string) (*Command, error) {
	if path == "" {
		return nil, fmt.Errorf("path name cannot be empty")
	}
	return &Command{
		Name:        "policy",
		Path:        path,
		Compression: compression,
	}, nil
}

// NewCompressionExecutor returns an instance of Executor that can be used for
// running a kopia policy set command
func NewCompressionExecutor(cmd *Command) Executor {
	return &compressionExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (c *compressionExecutor) Run(debugMode string) error {
	c.execCmd = c.cmd.CompressionCmd(debugMode)
	c.execCmd.Stdout = c.outBuf
	c.execCmd.Stderr = c.errBuf

	if err := c.execCmd.Start(); err != nil {
		c.lastError = err
		return err
	}
	c.isRunning = true
	go func() {
		err := c.execCmd.Wait()
		if err != nil {
			c.lastError = fmt.Errorf("failed to run the kopia compression setting command: %v"+
				" stdout: %v stderr: %v", err, c.outBuf.String(), c.errBuf.String())
			logrus.Errorf("%v", c.lastError)
			return
		}
		c.isRunning = false
	}()

	return nil
}

func (c *compressionExecutor) Status() (*cmdexec.Status, error) {
	if c.lastError != nil {
		fmt.Fprintln(os.Stderr, c.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: c.lastError,
			Done:           true,
		}, nil
	}

	if c.isRunning {
		return &cmdexec.Status{
			Done:           false,
			LastKnownError: nil,
		}, nil
	}

	return &cmdexec.Status{
		Done: true,
	}, nil

}
