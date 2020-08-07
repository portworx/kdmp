package restic

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"
)

const (
	alreadyInitializedErrMsg = "repository master key and config already initialized"
)

var (
	// ErrAlreadyInitialized is returned when the restic repository is already initialized.
	ErrAlreadyInitialized = fmt.Errorf(alreadyInitializedErrMsg)
)

// GetInitCommand returns a wrapper over the restic init command
func GetInitCommand(repoName string, secretFilePath string) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	if secretFilePath == "" {
		return nil, fmt.Errorf("secret file path cannot be empty")
	}

	return &Command{
		Name:           "init",
		RepositoryName: repoName,
		SecretFilePath: secretFilePath,
	}, nil
}

// InitSummaryResponse is the json representation of the summary output
// of restic init
type InitSummaryResponse struct {
	Created bool `json:"created"`
}

type initExecutor struct {
	cmd             *Command
	responseLock    sync.Mutex
	summaryResponse *InitSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
	isRunning       bool
}

// NewInitExecutor returns an instance of Executor that can be used for
// running a restic init command
func NewInitExecutor(cmd *Command) Executor {
	return &initExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *initExecutor) Run() error {
	b.responseLock.Lock()
	defer b.responseLock.Unlock()

	if b.isRunning {
		return fmt.Errorf("another init operation is already running")
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
		// init has completed
		b.responseLock.Lock()
		defer b.responseLock.Unlock()
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the init command: %v", err)
			if err = parseStdErr(b.errBuf.Bytes()); err != nil {
				b.lastError = err
			}
			return
		}

		summaryResponse, err := getInitSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse
	}()
	return nil
}

func (b *initExecutor) Status() (*Status, error) {
	b.responseLock.Lock()
	defer b.responseLock.Unlock()

	if b.lastError != nil {
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}

	if b.summaryResponse != nil {
		return &Status{
			Done: true,
		}, nil
	}

	return &Status{
		Done:           false,
		LastKnownError: nil,
	}, nil
}

func getInitSummary(outBytes []byte, errBytes []byte) (*InitSummaryResponse, error) {
	return &InitSummaryResponse{
		Created: true,
	}, nil
}

func parseStdErr(stdErr []byte) error {
	outLines := bytes.Split(stdErr, []byte("\n"))
	for i := len(outLines) - 1; i >= 0; i-- {
		if bytes.Contains(outLines[i], []byte(alreadyInitializedErrMsg)) {
			return ErrAlreadyInitialized
		}
		if bytes.Contains(outLines[i], []byte("Fatal:")) {
			return fmt.Errorf("error:%s", bytes.TrimLeft(outLines[i], "Fatal:"))
		}
		if len(outLines[i]) > 0 {
			return fmt.Errorf("%s", outLines[i])
		}
	}
	return nil
}
