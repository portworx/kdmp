package kopia

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

// BackupSummaryResponse describes single snapshot entry.
type BackupSummaryResponse struct {
	ID               string           `json:"id"`
	Source           SourceInfo       `json:"source"`
	Description      string           `json:"description"`
	StartTime        time.Time        `json:"startTime"`
	EndTime          time.Time        `json:"endTime"`
	IncompleteReason string           `json:"incomplete,omitempty"`
	Summary          DirectorySummary `json:"summ"`
	RootEntry        string           `json:"rootID"`
	RetentionReasons []string         `json:"retention"`
}

// SourceInfo represents the information about snapshot source.
type SourceInfo struct {
	Host     string `json:"host"`
	UserName string `json:"userName"`
	Path     string `json:"path"`
}

// DirectorySummary represents summary information about a directory.
type DirectorySummary struct {
	TotalFileSize     int64     `json:"size"`
	TotalFileCount    int64     `json:"files"`
	TotalSymlinkCount int64     `json:"symlinks"`
	TotalDirCount     int64     `json:"dirs"`
	MaxModTime        time.Time `json:"maxTime"`
	IncompleteReason  string    `json:"incomplete,omitempty"`

	// number of failed files
	FatalErrorCount   int `json:"numFailed"`
	IgnoredErrorCount int `json:"numIgnoredErrors,omitempty"`

	FailedEntries []*EntryWithError `json:"errors,omitempty"`
}

// EntryWithError describes error encountered when processing an entry.
type EntryWithError struct {
	EntryPath string `json:"path"`
	Error     string `json:"error"`
}

type backupExecutor struct {
	cmd *Command
	//cmd               *ExecCommand
	summaryResponse *BackupSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
}

// GetBackupCommand returns a wrapper over the kopia backup command
func GetBackupCommand(path, repoName, password, provider, sourcePath string) (*Command, error) {
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

// NewBackupExecutor returns an instance of Executor that can be used for
// running a kopia snapshot create command
func NewBackupExecutor(cmd *Command) Executor {
	return &backupExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *backupExecutor) Run() error {
	b.execCmd = b.cmd.BackupCmd()
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}
	go func() {
		err := b.execCmd.Wait()
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the backup command: %v", err)
			logrus.Infof("stdout: %v", b.execCmd.Stdout)
			logrus.Infof("Stderr: %v", b.execCmd.Stderr)
			return
		}

		summaryResponse, err := getBackupSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse
	}()
	return nil
}

func (b *backupExecutor) Status() (*cmdexec.Status, error) {
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
			// TODO: We don't need totalbytes processed as size is same?
			TotalBytesProcessed: uint64(b.summaryResponse.Summary.TotalFileSize),
			TotalBytes:          uint64(b.summaryResponse.Summary.TotalFileSize),
			SnapshotID:          b.summaryResponse.ID,
			Done:                true,
			LastKnownError:      nil,
		}, nil
	} // else backup is still in progress

	return &cmdexec.Status{
		Done:           false,
		LastKnownError: nil,
	}, nil
}

func getBackupSummary(outBytes []byte, errBytes []byte) (*BackupSummaryResponse, error) {
	outLines := bytes.Split(outBytes, []byte("\n"))
	if len(outLines) == 0 {
		return nil, &cmdexec.Error{
			Reason:    "backup summary not available",
			CmdOutput: string(outBytes),
			CmdErr:    string(errBytes),
		}
	}

	outResponse := outLines[0]
	summaryResponse := &BackupSummaryResponse{
		Summary: DirectorySummary{},
	}

	if err := json.Unmarshal(outResponse, summaryResponse); err != nil {
		return nil, &cmdexec.Error{
			Reason:    fmt.Sprintf("failed to parse backup summary: %v", err),
			CmdOutput: string(outResponse),
			CmdErr:    string(errBytes),
		}
	}
	// If the ID is not present fail the backup
	if summaryResponse.ID == "" {
		return nil, &cmdexec.Error{
			Reason:    "failed to backup as snapshot ID is not present",
			CmdOutput: string(outResponse),
			CmdErr:    string(errBytes),
		}
	}
	// If numFailed is non-zero, fail the backup
	if summaryResponse.Summary.FatalErrorCount != 0 {
		return nil, &cmdexec.Error{
			Reason:    fmt.Sprintf("failed to backup as FatalErrorCount is %v", summaryResponse.Summary.FatalErrorCount),
			CmdOutput: string(outResponse),
			CmdErr:    string(errBytes),
		}
	}
	return summaryResponse, nil
}
