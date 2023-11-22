package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

type globalPolicyExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
}

// SetGlobalPolicyCommand returns a wrapper over the kopia set policy command
func SetGlobalPolicyCommand() (*Command, error) {
	return &Command{
		Name: "set",
	}, nil
}

// NewSetGlobalPolicyExecutor returns an instance of Executor that can be used for
// running a kopia policy command command
func NewSetGlobalPolicyExecutor(cmd *Command) Executor {
	return &globalPolicyExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *globalPolicyExecutor) Run(debugMode string) error {
	b.execCmd = b.cmd.SetPolicyCmd(debugMode)
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}

	go func() {
		err := b.execCmd.Wait()
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the kopia policy command: %v"+
				" stdout: %v stderr: %v", err, b.outBuf.String(), b.errBuf.String())
			logrus.Errorf("%v", b.lastError)
			return
		}
	}()

	return nil
}

func (b *globalPolicyExecutor) Status() (*cmdexec.Status, error) {
	if b.lastError != nil {
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}

	return &cmdexec.Status{
		Done:           true,
		LastKnownError: nil,
	}, nil
}
