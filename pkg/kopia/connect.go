package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
)

type connectExecutor struct {
	cmd             *Command
	summaryResponse *ConnectSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
}

// ConnectSummaryResponse is the json representation of the summary output
// of kopia repository connect
type ConnectSummaryResponse struct {
	Created bool `json:"created"`
}

const (
	repoConnectSuccessMsg = "Connected to repository"
)

// GetConnectCommand returns a wrapper over the kopia connect command
func GetConnectCommand(path, repoName, password, provider string, disableSsl bool) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	return &Command{
		Name:           "connect",
		Provider:       provider,
		RepositoryName: repoName,
		Password:       password,
		Path:           path,
		DisableSsl:     disableSsl,
	}, nil
}

// NewConnectExecutor returns an instance of Executor that can be used for
// running a kopia repository connect command
func NewConnectExecutor(cmd *Command) Executor {
	return &connectExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *connectExecutor) Run() error {
	b.execCmd = b.cmd.ConnectCmd()
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}

	go func() {
		err := b.execCmd.Wait()

		if err != nil {
			b.lastError = fmt.Errorf("failed to run the kopia connect command: %v"+
				" stdout: %v stderr: %v", err, b.outBuf.String(), b.errBuf.String())
			logrus.Errorf("%v", b.lastError)
			return
		}

		summaryResponse, err := getConnectSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse
	}()

	return nil
}

func (b *connectExecutor) Status() (*cmdexec.Status, error) {
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
		LastKnownError: nil,
	}, nil
}
func getConnectSummary(outBytes []byte, errBytes []byte) (*ConnectSummaryResponse, error) {
	if bytes.Contains(errBytes, []byte(repoConnectSuccessMsg)) {
		return &ConnectSummaryResponse{
			Created: true,
		}, nil
	}

	return &ConnectSummaryResponse{
		Created: false,
	}, fmt.Errorf("failed to connect to repository")
}
