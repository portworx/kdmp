package kopia

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

// ListSummaryResponse describes single snapshot list entry.
type ListSummaryResponse struct {
	ID string `json:"id"`
}

type listExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// GetListCommand returns a wrapper over the kopia snapshot list command
func GetListCommand() (*Command, error) {
	return &Command{
		Name: "snapshot",
	}, nil
}

// NewListExecutor returns an instance of Executor that can be used for
// running a kopia snapshot list command
func NewListExecutor(cmd *Command) Executor {
	return &listExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *listExecutor) Run() error {
	b.execCmd = b.cmd.SnapshotListCmd()
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}

	b.isRunning = true

	go func() {
		err := b.execCmd.Wait()

		if err != nil {
			b.lastError = fmt.Errorf("failed to run the kopia snapshot list command: %v"+
				" stdout: %v stderr: %v", err, b.outBuf.String(), b.errBuf.String())
			logrus.Errorf("%v", b.lastError)
			return
		}
		b.isRunning = false
	}()

	return nil
}

func (b *listExecutor) Status() (*cmdexec.Status, error) {
	if b.lastError != nil {
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}
	if b.isRunning {
		return &cmdexec.Status{
			Done:           false,
			LastKnownError: nil,
		}, nil
	}

	var listSummaryResponses []ListSummaryResponse
	err := json.Unmarshal(b.outBuf.Bytes(), &listSummaryResponses)
	if err != nil {
		return &cmdexec.Status{
			Done:           true,
			LastKnownError: err,
		}, nil
	}
	var snapshotIds []string
	for _, listSummaryResponse := range listSummaryResponses {
		snapshotIds = append(snapshotIds, listSummaryResponse.ID)

	}

	return &cmdexec.Status{
		Done:           true,
		SnapshotIDs:    snapshotIds,
		LastKnownError: nil,
	}, nil
}
