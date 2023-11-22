package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/user"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

const (
	rootUser = "root@"
)

type maintenanceSetExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// GetMaintenanceSetCommand returns a wrapper over the kopia repo maintenance set command
func GetMaintenanceSetCommand() (*Command, error) {
	hostname, err := os.Hostname()
	if err != nil {
		errMsg := fmt.Sprintf("failed in getting hostname: %v", err)
		logrus.Infof("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	curUser := rootUser
	// Use the current user for maintenance as container may not run with root user always
	user, err := user.Current()
	if err != nil {
		logrus.Errorf("failed to get current user: %v", err)
	} else {
		curUser = fmt.Sprintf("%s@", user.Username)
	}
	owner := curUser + hostname
	logrus.Debugf("GetMaintenanceSetCommand: owner %v", owner)
	return &Command{
		Name:             "maintenance",
		MaintenanceOwner: owner,
	}, nil
}

// NewMaintenanceSetExecutor returns an instance of Executor that can be used for
// running a kopia repo maintenance command
func NewMaintenanceSetExecutor(cmd *Command) Executor {
	return &maintenanceSetExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (ms *maintenanceSetExecutor) Run(debugMode string) error {
	ms.execCmd = ms.cmd.MaintenanceSetCmd(debugMode)
	ms.execCmd.Stdout = ms.outBuf
	ms.execCmd.Stderr = ms.errBuf

	if err := ms.execCmd.Start(); err != nil {
		ms.lastError = err
		return err
	}
	ms.isRunning = true
	go func() {
		err := ms.execCmd.Wait()
		if err != nil {
			ms.lastError = fmt.Errorf("failed to run the repo maintenance set command: %v"+
				" stdout: %v stderr: %v", ms.execCmd.Stderr, ms.outBuf.String(), ms.errBuf.String())
			logrus.Errorf("%v", ms.lastError)
		}
		ms.isRunning = false
	}()
	return nil
}

func (ms *maintenanceSetExecutor) Status() (*cmdexec.Status, error) {
	if ms.lastError != nil {
		fmt.Fprintln(os.Stderr, ms.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: ms.lastError,
			Done:           true,
		}, nil
	}
	if ms.isRunning {
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
