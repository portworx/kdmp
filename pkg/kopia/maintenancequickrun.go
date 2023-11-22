package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

type quickMaintenanceRunExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// GetQuickMaintenanceRunCommand returns a wrapper over the kopia repo maintenance run command
func GetQuickMaintenanceRunCommand() (*Command, error) {
	return &Command{
		Name: "maintenance",
	}, nil
}

// NewQuickMaintenanceRunExecutor returns an instance of Executor that can be used for
// running a kopia repo maintenance command
func NewQuickMaintenanceRunExecutor(cmd *Command) Executor {
	return &quickMaintenanceRunExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (qmr *quickMaintenanceRunExecutor) Run(debugMode string) error {
	qmr.execCmd = qmr.cmd.QuickMaintenanceRunCmd(debugMode)
	qmr.execCmd.Stdout = qmr.outBuf
	qmr.execCmd.Stderr = qmr.errBuf

	if err := qmr.execCmd.Start(); err != nil {
		qmr.lastError = err
		return err
	}
	qmr.isRunning = true
	go func() {
		err := qmr.execCmd.Wait()
		if err != nil {
			qmr.lastError = fmt.Errorf("failed to run the repo quick maintenance command: %v"+
				" stdout: %v stderr: %v", qmr.execCmd.Stderr, qmr.outBuf.String(), qmr.errBuf.String())
			logrus.Errorf("%v", qmr.lastError)
		}
		qmr.isRunning = false
	}()
	return nil
}

func (qmr *quickMaintenanceRunExecutor) Status() (*cmdexec.Status, error) {
	if qmr.lastError != nil {
		fmt.Fprintln(os.Stderr, qmr.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: qmr.lastError,
			Done:           true,
		}, nil
	}
	if qmr.isRunning {
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
