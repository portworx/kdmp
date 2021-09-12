package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

// GetRestoreCommand returns a wrapper over the kopia restore command.
func GetRestoreCommand(path, repoName, password, provider, targetPath, snapshotID string) (*Command, error) {
	if targetPath == "" {
		return nil, fmt.Errorf("destination path cannot be empty")
	}
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	if password == "" {
		return nil, fmt.Errorf("password cannot be empty")
	}
	if snapshotID == "" {
		return nil, fmt.Errorf("snapshot id cannot be empty")
	}

	args := []string{snapshotID, "."}

	return &Command{
		Name:     "restore",
		Password: password,
		Dir:      targetPath,
		Provider: provider,
		Args:     args,
	}, nil
}

// RestoreProgressResponse is the json representation of the in-progress
// output of kopia restore
//
// TODO: kopia provides no progress info for a restore process now:
type RestoreProgressResponse struct {
}

// RestoreSummaryResponse is the json representation of the summary output
// of kopia restore
// TODO: kopia does not provide json response of restore
type RestoreSummaryResponse struct {
}

type restoreExecutor struct {
	cmd       *Command
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// NewRestoreExecutor returns an instance of Executor that can be used for
// running a kopia restore command
func NewRestoreExecutor(cmd *Command) Executor {
	return &restoreExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *restoreExecutor) Run() error {

	b.execCmd = b.cmd.RestoreCmd()
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
			b.lastError = fmt.Errorf("failed to run the restore command: %v", err)
			logrus.Infof("stdout: %v", b.execCmd.Stdout)
			logrus.Infof("Stderr: %v", b.execCmd.Stderr)
			return
		}
		b.isRunning = false
	}()
	return nil
}

func (b *restoreExecutor) Status() (*cmdexec.Status, error) {
	if b.lastError != nil {
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}

	if !b.isRunning {
		return &cmdexec.Status{
			ProgressPercentage: 100,
			Done:               true,
		}, nil
	}

	return &cmdexec.Status{}, nil
}
