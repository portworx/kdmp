package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

type deleteExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// GetDeleteCommand returns a wrapper over the kopia snapshot delete command
func GetDeleteCommand(snapshotID string) (*Command, error) {
	return &Command{
		Name:       "delete",
		SnapshotID: snapshotID,
	}, nil
}

// NewDeleteExecutor returns an instance of Executor that can be used for
// running a kopia snapshot delete command
func NewDeleteExecutor(cmd *Command) Executor {
	return &deleteExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (d *deleteExecutor) Run() error {
	d.execCmd = d.cmd.DeleteCmd()
	d.execCmd.Stdout = d.outBuf
	d.execCmd.Stderr = d.errBuf

	if err := d.execCmd.Start(); err != nil {
		d.lastError = err
		return err
	}
	d.isRunning = true
	go func() {
		err := d.execCmd.Wait()
		if err != nil {
			d.lastError = fmt.Errorf("failed to run the snapshot delete command: %v", err)
			logrus.Infof("stdout: %v", d.execCmd.Stdout)
			logrus.Infof("Stderr: %v", d.execCmd.Stderr)
		}
		d.isRunning = false
	}()
	return nil
}

func (d *deleteExecutor) Status() (*cmdexec.Status, error) {
	if d.lastError != nil {
		fmt.Fprintln(os.Stderr, d.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: d.lastError,
			Done:           true,
		}, nil
	}
	if d.isRunning {
		return &cmdexec.Status{
			Done:           false,
			LastKnownError: nil,
		}, nil
	}
	return &cmdexec.Status{
		Done:           true,
		LastKnownError: nil,
	}, nil
}
