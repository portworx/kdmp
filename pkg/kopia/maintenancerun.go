package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

type maintenanceRunExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// GetMaintenanceRunCommand returns a wrapper over the kopia repo maintenance run command
func GetMaintenanceRunCommand() (*Command, error) {
	return &Command{
		Name: "maintenance",
	}, nil
}

// NewMaintenanceRunExecutor returns an instance of Executor that can be used for
// running a kopia repo maintenance command
func NewMaintenanceRunExecutor(cmd *Command) Executor {
	return &maintenanceRunExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (mr *maintenanceRunExecutor) Run(debugMode string) error {
	mr.execCmd = mr.cmd.MaintenanceRunCmd(debugMode)
	mr.execCmd.Stdout = mr.outBuf
	mr.execCmd.Stderr = mr.errBuf

	if err := mr.execCmd.Start(); err != nil {
		mr.lastError = err
		return err
	}
	mr.isRunning = true
	go func() {
		err := mr.execCmd.Wait()
		if err != nil {
			mr.lastError = fmt.Errorf("failed to run the repo maintenance command: %v"+
				" stdout: %v stderr: %v", mr.execCmd.Stderr, mr.outBuf.String(), mr.errBuf.String())
			logrus.Errorf("%v", mr.lastError)
		}
		mr.isRunning = false
	}()
	return nil
}

func (mr *maintenanceRunExecutor) Status() (*cmdexec.Status, error) {
	if mr.lastError != nil {
		fmt.Fprintln(os.Stderr, mr.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: mr.lastError,
			Done:           true,
		}, nil
	}
	if mr.isRunning {
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
