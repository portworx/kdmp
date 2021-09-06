package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

const (
	// DefaultDestPath is the directory to do kopia restore
	DefaultDestPath = "/data"
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
		Dir:      DefaultDestPath,
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
	cmd             *Command
	responseLock    sync.Mutex
	summaryResponse *RestoreSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
	isRunning       bool
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

	b.responseLock.Lock()
	defer b.responseLock.Unlock()

	if b.isRunning {
		return fmt.Errorf("another restore operation is already running")
	}

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
		b.responseLock.Lock()
		defer b.responseLock.Unlock()
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the restore command: %v", err)
			logrus.Infof("stdout: %v", b.execCmd.Stdout)
			logrus.Infof("Stderr: %v", b.execCmd.Stderr)
			return
		}

		summaryResponse, err := getRestoreSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse
	}()
	return nil
}

func (b *restoreExecutor) Status() (*cmdexec.Status, error) {
	b.responseLock.Lock()
	defer b.responseLock.Unlock()

	if b.lastError != nil {
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}

	if b.summaryResponse != nil {
		return &cmdexec.Status{
			ProgressPercentage: 100,
			Done:               true,
		}, nil
	}

	return &cmdexec.Status{}, nil
}

func getRestoreSummary(outBytes []byte, errBytes []byte) (*RestoreSummaryResponse, error) {
	return &RestoreSummaryResponse{}, nil
}
