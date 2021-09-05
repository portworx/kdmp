package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
)

const (
	alreadyRepoExistErrMsg = "found existing data in storage location"
	initializedSuccessMsg  = "Kopia will perform quick maintenance"
)

var (
	// ErrAlreadyRepoExist is returned when the kopia repository is already present.
	ErrAlreadyRepoExist = fmt.Errorf(alreadyRepoExistErrMsg)
)

// CreateSummaryResponse is the json representation of the summary output
// of kopia repo create command
type CreateSummaryResponse struct {
	Created bool `json:"created"`
}

type createExecutor struct {
	cmd             *Command
	summaryResponse *CreateSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
}

// GetCreateCommand returns a wrapper over the kopia repo create command
func GetCreateCommand(path, repoName, password, provider string) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	return &Command{
		Name:           "create",
		Provider:       provider,
		RepositoryName: repoName,
		Password:       password,
		Path:           path,
	}, nil
}

// NewCreateExecutor returns an instance of Executor that can be used for
// running a repo create command
func NewCreateExecutor(cmd *Command) Executor {
	return &createExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *createExecutor) Run() error {
	b.execCmd = b.cmd.InitCmd()
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}

	go func() {
		err := b.execCmd.Wait()
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

func (b *createExecutor) Status() (*cmdexec.Status, error) {
	if b.lastError != nil {
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &cmdexec.Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}

	if b.summaryResponse != nil {
		return &cmdexec.Status{
			Done: true,
		}, nil
	}

	return &cmdexec.Status{
		Done:           false,
		LastKnownError: b.lastError,
	}, nil
}

func getInitSummary(outBytes []byte, errBytes []byte) (*CreateSummaryResponse, error) {
	// For now checking if maintenance msg is printed to know if repository creation
	// is successful.
	if bytes.Contains(errBytes, []byte(initializedSuccessMsg)) {
		return &CreateSummaryResponse{
			Created: true,
		}, nil
	}

	return &CreateSummaryResponse{
		Created: false,
	}, fmt.Errorf("failed to create repository")
}

func parseStdErr(stdErr []byte) error {
	outLines := bytes.Split(stdErr, []byte("\n"))
	for _, out := range outLines {
		if bytes.Contains(out, []byte(alreadyRepoExistErrMsg)) {
			return ErrAlreadyRepoExist
		}

	}
	return fmt.Errorf("error while creating repository: %v", outLines)
}
