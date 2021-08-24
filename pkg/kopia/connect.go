package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/sirupsen/logrus"
)

type connectExecutor struct {
	cmd             *Command
	responseLock    sync.Mutex
	summaryResponse *ConnectSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
	isRunning       bool
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
func GetConnectCommand(path, repoName, password, provider string) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	logrus.Infof("line 32 password: %v", password)
	return &Command{
		Name:           "connect",
		Provider:       provider,
		RepositoryName: repoName,
		Password:       password,
		Path:           path,
	}, nil
}

// NewConnectExecutor returns an instance of Executor that can be used for
// running a restic repository connect command
func NewConnectExecutor(cmd *Command) Executor {
	return &connectExecutor{
		cmd:    cmd,
		outBuf: new(bytes.Buffer),
		errBuf: new(bytes.Buffer),
	}
}

func (b *connectExecutor) Run() error {
	logrus.Infof("line 71 Run()")
	//b.responseLock.Lock()
	//defer b.responseLock.Unlock()

	/*if b.isRunning {
		return fmt.Errorf("another init operation is already running")
	}*/

	b.execCmd = b.cmd.ConnectCmd()
	b.execCmd.Stdout = b.outBuf
	b.execCmd.Stderr = b.errBuf

	if err := b.execCmd.Start(); err != nil {
		b.lastError = err
		return err
	}
	b.isRunning = true
	logrus.Infof("** line 87 Run() cmd: %+v", b.cmd)
	logrus.Infof("** line 90 Run() execcmd: %+v", b.execCmd)
	logrus.Infof("line 91 Run() env : %v, args: %v", b.execCmd.Env, b.execCmd.Args)
	go func() {
		err := b.execCmd.Wait()
		// init has completed
		//b.responseLock.Lock()
		//defer b.responseLock.Unlock()
		logrus.Infof("line 96 stdout: %v", b.execCmd.Stdout)
		logrus.Infof(" line 97 Stderr: %v", b.execCmd.Stderr)
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the init command: %v", err)
			return
		}

		summaryResponse, err := getConnectSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			logrus.Infof("line 112")
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse
		logrus.Infof("line 114 summaryResponse: %+v", b.summaryResponse)
		logrus.Infof("error: %v", b.lastError)
	}()

	logrus.Infof("line 121")
	return nil
}

// TODO: Implement status fully
func (b *connectExecutor) Status() (*Status, error) {
	//b.responseLock.Lock()
	//defer b.responseLock.Unlock()

	if b.lastError != nil {
		logrus.Infof("line 129 status")
		fmt.Fprintln(os.Stderr, b.errBuf.String())
		return &Status{
			LastKnownError: b.lastError,
			Done:           true,
		}, nil
	}
	logrus.Infof("line 136 status")
	if b.summaryResponse != nil {
		logrus.Infof("line 138 status")
		return &Status{
			Done: true,
		}, nil
	}
	logrus.Infof("line 142 status")

	return &Status{
		Done:           false,
		LastKnownError: nil,
	}, nil
}
func getConnectSummary(outBytes []byte, errBytes []byte) (*ConnectSummaryResponse, error) {
	if bytes.Contains(errBytes, []byte(repoConnectSuccessMsg)) {
		logrus.Infof("line 154 Successfully created repository")
		return &ConnectSummaryResponse{
			Created: true,
		}, nil
	}

	return &ConnectSummaryResponse{
		Created: false,
	}, fmt.Errorf("failed to connect to repository")
}
