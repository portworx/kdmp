package restic

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
)

// GetBackupCommand returns a wrapper over the restic backup
// command
func GetBackupCommand(
	repoName string,
	secretFilePath string,
	srcPath string,
) (*Command, error) {
	if srcPath == "" {
		return nil, fmt.Errorf("source path cannot be empty")
	}
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	if secretFilePath == "" {
		return nil, fmt.Errorf("secret file path cannot be empty")
	}

	return &Command{
		Name:           "backup",
		RepositoryName: repoName,
		SecretFilePath: secretFilePath,
		Dir:            srcPath,
		Flags:          defaultFlags(),
		Args:           []string{"."},
	}, nil
}

// BackupProgressResponse is the json representation of the in-progress
// output of restic backup
type BackupProgressResponse struct {
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

// BackupSummaryResponse is the json representation of the summary output
// of restic backup
type BackupSummaryResponse struct {
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

type backupExecutor struct {
	cmd             *Command
	responseLock    sync.Mutex
	summaryResponse *BackupSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
	isRunning       bool
}

// NewBackupExecutor returns an instance of Executor that can be used for
// running a restic backup command
func NewBackupExecutor(cmd *Command) Executor {
	return &backupExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *backupExecutor) Run() error {
	b.responseLock.Lock()
	defer b.responseLock.Unlock()

	if b.isRunning {
		return fmt.Errorf("another backup operation is already running")
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
		// backup has completed
		b.responseLock.Lock()
		defer b.responseLock.Unlock()
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the backup command: %v", err)
			if err = parseStdErr(b.errBuf.Bytes()); err != nil {
				b.lastError = err
			}
			return
		}

		summaryResponse, err := getSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse
	}()
	return nil
}

func (b *backupExecutor) Status() (*cmdexec.Status, error) {
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
			ProgressPercentage:  100,
			TotalBytesProcessed: b.summaryResponse.TotalBytesProcessed,
			TotalBytes:          b.summaryResponse.TotalBytesProcessed,
			SnapshotID:          b.summaryResponse.SnapshotID,
			Done:                true,
			LastKnownError:      nil,
		}, nil
	} // else backup is still in progress
	progressResponse, err := getProgress(b.outBuf.Bytes(), b.errBuf.Bytes())
	if err != nil {
		return &cmdexec.Status{
			Done:           false,
			LastKnownError: err,
		}, nil
	}

	return &cmdexec.Status{
		ProgressPercentage:  progressResponse.PercentDone * 100,
		TotalBytes:          progressResponse.TotalBytes,
		TotalBytesProcessed: progressResponse.BytesDone,
		Done:                false,
		LastKnownError:      nil,
	}, nil
}

func getSummary(outBytes []byte, errBytes []byte) (*BackupSummaryResponse, error) {
	outLines := bytes.Split(outBytes, []byte("\n"))

	if len(outLines) <= 2 {
		return nil, &cmdexec.Error{
			Reason:    "backup summary not available",
			CmdOutput: string(outBytes),
			CmdErr:    string(errBytes),
		}
	}

	for i := len(outLines) - 1; i >= 0; i-- {
		summaryResponse := &BackupSummaryResponse{}
		if len(outLines[i]) < 1 {
			// ignore \n
			continue
		}
		if err := json.Unmarshal(outLines[i], summaryResponse); err != nil {
			return nil, &cmdexec.Error{
				Reason:    fmt.Sprintf("failed to parse backup summary: %v", err),
				CmdOutput: string(outLines[i]),
				CmdErr:    string(errBytes),
			}
		}
		if summaryResponse.MessageType != "summary" {
			continue
		}
		return summaryResponse, nil
	}
	return nil, &cmdexec.Error{
		Reason: "could not find backup summary",
	}
}

func getProgress(outBytes []byte, errBytes []byte) (*BackupProgressResponse, error) {
	outLines := bytes.Split(outBytes, []byte("\n"))
	if len(outLines) <= 2 {
		return nil, fmt.Errorf("backup progress not available yet")
	}
	outLine := outLines[len(outLines)-2] // last line is \n
	progressResponse := &BackupProgressResponse{}
	if err := json.Unmarshal(outLine, progressResponse); err != nil || len(progressResponse.MessageType) == 0 {
		return nil, &cmdexec.Error{
			Reason:    fmt.Sprintf("failed to parse progress of backup: %v", err),
			CmdOutput: string(outLine),
			CmdErr:    string(errBytes),
		}
	}
	return progressResponse, nil
}
