package restic

import (
	"bytes"
	"fmt"
	"os/exec"
	"sync"
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

	flags := []string{"--target", dstPath}

	return &Command{
		Name:           "restore",
		RepositoryName: repoName,
		SecretFilePath: secretFilePath,
		Flags:          append(defaultFlags(), flags...),
		Args:           []string{snapshotID},
	}, nil
}

// RestoreProgressResponse is the json representation of the in-progress
// output of restic restore
//
// TODO: restic provides no progress info for a restore process now:
//       https://github.com/restic/restic/issues/1154
type RestoreProgressResponse struct {
	MessageType      string   `json:"message_type"` // "status"
	SecondsElapsed   uint64   `json:"seconds_elapsed,omitempty"`
	SecondsRemaining uint64   `json:"seconds_remaining,omitempty"`
	PercentDone      float64  `json:"percent_done"`
	TotalFiles       uint64   `json:"total_files,omitempty"`
	FilesDone        uint64   `json:"files_done,omitempty"`
	TotalBytes       uint64   `json:"total_bytes,omitempty"`
	BytesDone        uint64   `json:"bytes_done,omitempty"`
	ErrorCount       uint     `json:"error_count,omitempty"`
	CurrentFiles     []string `json:"current_files,omitempty"`
}

// RestoreSummaryResponse is the json representation of the summary output
// of restic restore
type RestoreSummaryResponse struct {
	MessageType         string  `json:"message_type"` // "summary"
	FilesNew            uint    `json:"files_new"`
	FilesChanged        uint    `json:"files_changed"`
	FilesUnmodified     uint    `json:"files_unmodified"`
	DirsNew             uint    `json:"dirs_new"`
	DirsChanged         uint    `json:"dirs_changed"`
	DirsUnmodified      uint    `json:"dirs_unmodified"`
	DataBlobs           int     `json:"data_blobs"`
	TreeBlobs           int     `json:"tree_blobs"`
	DataAdded           uint64  `json:"data_added"`
	TotalFilesProcessed uint    `json:"total_files_processed"`
	TotalBytesProcessed uint64  `json:"total_bytes_processed"`
	TotalDuration       float64 `json:"total_duration"` // in seconds
	SnapshotID          string  `json:"snapshot_id"`
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

func (b *restoreExecutor) Status() (*Status, error) {
	b.responseLock.Lock()
	defer b.responseLock.Unlock()

	if b.lastError != nil {
		return &Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}

	if b.summaryResponse != nil {
		return &Status{
			ProgressPercentage: 100,
			Done:               true,
			LastKnownError:     nil,
		}, nil
	} // else restore is still in progress

	return &Status{
		Done:           false,
		LastKnownError: nil,
	}, nil
}

func getRestoreSummary(outBytes []byte, errBytes []byte) (*RestoreSummaryResponse, error) {
	return &RestoreSummaryResponse{}, nil
}
