package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

const (
	restoreMsg = "Restored"
)

const (
	// KB 1000 bytes
	KB = 1000
	// MB 1000 KB
	MB = KB * 1000
	// GB 1000 MB
	GB = MB * 1000
	// TB 1000 GB
	TB = GB * 1000
	// PB 1000 TB
	PB = TB * 1000
)

const (
	kb = "KB"
	mb = "MB"
	gb = "GB"
	tb = "TB"
	pb = "PB"
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

func (b *restoreExecutor) Run(debugMode string) error {

	b.execCmd = b.cmd.RestoreCmd(debugMode)
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
			b.lastError = fmt.Errorf("failed to run the restore command: %v"+
				" stdout: %v stderr: %v", err, b.outBuf.String(), b.errBuf.String())
			logrus.Errorf("%v", b.lastError)
			return
		}
		b.isRunning = false
	}()
	return nil
}

func (b *restoreExecutor) Status() (*cmdexec.Status, error) {
	errBytes := b.errBuf.Bytes()

	if b.lastError != nil {
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}
	if !b.isRunning {
		errLines := bytes.Split(errBytes, []byte("\n"))
		var size float64
		for _, lines := range errLines {
			if bytes.Contains(lines, []byte(restoreMsg)) {
				// Sample message of output
				// [Restored 136 files, 3 directories and 0 symbolic links (120.9 MB).]
				modStrings := strings.Split(string(lines), " ")
				mstr := modStrings[len(modStrings)-2]
				trimstr := strings.Trim(mstr, "(")
				trimstr = strings.Trim(trimstr, ".")
				var err error
				size, err = strconv.ParseFloat(trimstr, 32)
				if err != nil {
					// On failure, don't want to fail restore thats the current
					// behavior for other providers also
					logrus.Errorf("%v", err)
				}

				// Convert size to bytes
				if bytes.Contains(lines, []byte(kb)) {
					size = size * KB
				} else if bytes.Contains(lines, []byte(mb)) {
					size = size * MB
				} else if bytes.Contains(lines, []byte(gb)) {
					size = size * GB
				} else if bytes.Contains(lines, []byte(tb)) {
					size = size * TB
				} else if bytes.Contains(lines, []byte(pb)) {
					size = size * PB
				}
			}
		}
		logrus.Infof("restore size: %v", size)
		status := &cmdexec.Status{
			TotalBytes:          uint64(size),
			TotalBytesProcessed: uint64(size),
			LastKnownError:      nil,
			Done:                true,
		}
		return status, nil

	}
	return &cmdexec.Status{
		LastKnownError: nil,
		Done:           false,
	}, nil
}
