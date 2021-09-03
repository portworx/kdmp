package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

// DeleteSummaryResponse describes single snapshot entry.
type DeleteSummaryResponse struct {
	Deleted bool `json:"deleted"`
}

type deleteExecutor struct {
	cmd             *Command
	summaryResponse *DeleteSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
}

// GetDeleteCommand returns a wrapper over the kopia backup command
func GetDeleteCommand(path, repoName, password, provider, sourcePath string) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	return &Command{
		Name:     "create",
		Password: password,
		Path:     path,
		Dir:      sourcePath,
		Args:     []string{"."},
	}, nil
}

// NewDeleteExecutor returns an instance of Executor that can be used for
// running a kopia snapshot create command
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
	go func() {
		err := d.execCmd.Wait()
		if err != nil {
			d.lastError = fmt.Errorf("failed to run the delete backup snapshot command: %v", err)
			logrus.Infof("stdout: %v", d.execCmd.Stdout)
			logrus.Infof("Stderr: %v", d.execCmd.Stderr)
			return
		}

		summaryResponse, err := getDeleteSummary(d.outBuf.Bytes(), d.errBuf.Bytes())
		if err != nil {
			d.lastError = err
			return
		}
		d.summaryResponse = summaryResponse
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
	if d.summaryResponse != nil {
		return &cmdexec.Status{
			Done: true,
		}, nil
	}

	return &cmdexec.Status{
		Done:           false,
		LastKnownError: nil,
	}, nil
}

func getDeleteSummary(outBytes []byte, errBytes []byte) (*DeleteSummaryResponse, error) {
	if bytes.Contains(errBytes, []byte(repoConnectSuccessMsg)) {
		return &DeleteSummaryResponse{
			Deleted: true,
		}, nil
	}

	return &DeleteSummaryResponse{
		Deleted: false,
	}, fmt.Errorf("failed to connect to repository")
}
