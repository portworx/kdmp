package restic

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
)

const (
	// SnapshotIDLatest defines an id that restic recognizes it as the latest snapshot id.
	SnapshotIDLatest = "latest"
)

// GetRestoreCommand returns a wrapper over the restic restore command.
func GetRestoreCommand(
	repoName string,
	snapshotID string,
	secretFilePath string,
	dstPath string,
) (*Command, error) {
	if dstPath == "" {
		return nil, fmt.Errorf("destination path cannot be empty")
	}
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	if secretFilePath == "" {
		return nil, fmt.Errorf("secret file path cannot be empty")
	}
	if snapshotID == "" {
		snapshotID = SnapshotIDLatest
	}

	flags := []string{"--target", "."}

	return &Command{
		Name:           "restore",
		RepositoryName: repoName,
		SecretFilePath: secretFilePath,
		Dir:            dstPath,
		Flags:          append(defaultFlags(), flags...),
		Args:           []string{snapshotID},
	}, nil
}

// RestoreProgressResponse is the json representation of the in-progress
// output of restic restore
//
// TODO: restic provides no progress info for a restore process now:
//
//	https://github.com/restic/restic/issues/1154
type RestoreProgressResponse struct {
}

// RestoreSummaryResponse is the json representation of the summary output
// of restic restore
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
// running a restic restore command
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

	b.execCmd = b.cmd.Cmd()
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}
	b.isRunning = true
	go func() {
		err := b.execCmd.Wait()
		// restore has completed
		b.responseLock.Lock()
		defer b.responseLock.Unlock()
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the restore command: %v", err)
			if len(b.errBuf.Bytes()) > 0 {
				b.lastError = parseStdErr(b.errBuf.Bytes())
			}
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
