package kopia

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

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
	cmd             *Command
	responseLock    sync.Mutex
	summaryResponse *BackupSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
	isRunning       bool
}

// GetInitCommand returns a wrapper over the restic init command
func GetBackupCommand(path, repoName, password, provider, sourcePath string) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	logrus.Infof("line 48 password: %v", password)
	return &Command{
		Name:     "create",
		Password: password,
		Path:     path,
		Dir:      sourcePath,
		Args:     []string{"."},
	}, nil
}

// NewBackupExecutor returns an instance of Executor that can be used for
// running a restic init command
func NewBackupExecutor(cmd *Command) Executor {
	return &backupExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *backupExecutor) Run() error {
	logrus.Infof("line 50 Run()")
	//b.responseLock.Lock()
	//defer b.responseLock.Unlock()

	/*if b.isRunning {
		return fmt.Errorf("another init operation is already running")
	}*/

	b.execCmd = b.cmd.BackupCmd()
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}
	b.isRunning = true
	logrus.Infof("** line 67 Run() cmd: %+v", b.cmd)
	logrus.Infof("** line 68 Run() execcmd: %+v", b.execCmd)
	logrus.Infof("line 69 Run() env : %v, args: %v", b.execCmd.Env, b.execCmd.Args)
	logrus.Infof("line 73 time: %v", time.Now())
	go func() {
		err := b.execCmd.Wait()
		// init has completed
		b.responseLock.Lock()
		defer b.responseLock.Unlock()
		logrus.Infof("line 75 stdout: %v", b.execCmd.Stdout)
		logrus.Infof(" line 76 Stderr: %v", b.execCmd.Stderr)
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the backup command: %v", err)
			logrus.Infof("line 83 stdout: %v", b.execCmd.Stdout)
			logrus.Infof(" line 84 Stderr: %v", b.execCmd.Stderr)
			logrus.Infof("line 85 err: %v", err)
			return
		}

		summaryResponse, err := getBackupSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse
		logrus.Infof("line 140 time: %v", time.Now())
		//logrus.Infof("line 95")
	}()
	logrus.Infof("line 143 stdout: %v", b.execCmd.Stdout)
	logrus.Infof("line 144 Stderr: %v", b.execCmd.Stderr)
	return nil
}

func (b *backupExecutor) Status() (*Status, error) {
	//b.responseLock.Lock()
	//defer b.responseLock.Unlock()

	if b.lastError != nil {
		logrus.Infof("line 109 status")
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}
	logrus.Infof("line 116 status")
	if b.summaryResponse != nil {
		return &Status{
			ProgressPercentage: 100,
			// TODO: We don't need totalbytes processed as size is same?
			TotalBytesProcessed: uint64(b.summaryResponse.Summary.TotalFileSize),
			TotalBytes:          uint64(b.summaryResponse.Summary.TotalFileSize),
			SnapshotID:          b.summaryResponse.ID,
			Done:                true,
			LastKnownError:      nil,
		}, nil
	} // else backup is still in progress
	logrus.Infof("line 123 status")

	return &Status{
		Done:           false,
		LastKnownError: nil,
	}, nil
}

func getBackupSummary(outBytes []byte, errBytes []byte) (*BackupSummaryResponse, error) {
	outLines := bytes.Split(outBytes, []byte("\n"))
	if len(outLines) == 0 {
		return nil, &Error{
			Reason:    "backup summary not available",
			CmdOutput: string(outBytes),
			CmdErr:    string(errBytes),
		}
	}

	outResponse := outLines[0]
	summaryResponse := &BackupSummaryResponse{
		Summary: DirectorySummary{},
	}
	logrus.Infof("line 185 outLines: %v", string(outResponse))

	if err := json.Unmarshal(outResponse, summaryResponse); err != nil {
		return nil, &Error{
			Reason:    fmt.Sprintf("failed to parse backup summary: %v", err),
			CmdOutput: string(outResponse),
			CmdErr:    string(errBytes),
		}
	}
	// If the ID is not present fail the backup
	logrus.Infof("line 203 summaryResponse: %+v", summaryResponse)
	if summaryResponse.ID == "" {
		return nil, &Error{
			Reason:    "failed to backup as snapshot ID is not present",
			CmdOutput: string(outResponse),
			CmdErr:    string(errBytes),
		}
	}
	// If numFailed is non-zero, fail the backup
	if summaryResponse.Summary.FatalErrorCount != 0 {
		return nil, &Error{
			Reason:    fmt.Sprintf("failed to backup as FatalErrorCount is %v", summaryResponse.Summary.FatalErrorCount),
			CmdOutput: string(outResponse),
			CmdErr:    string(errBytes),
		}
	}
	logrus.Infof("line 220 summaryResponse.Summary: %+v", summaryResponse.Summary)
	return summaryResponse, nil
}
