package kopia

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/sirupsen/logrus"
)

/*
const (
	progressCheckInterval = 5 * time.Second
)*/

const (
	alreadyInitializedErrMsg = "found existing data in storage location"
	initializedSuccesMsg     = "Kopia will perform quick maintenance"
)

var (
	// ErrAlreadyInitialized is returned when the restic repository is already initialized.
	ErrAlreadyInitialized = fmt.Errorf(alreadyInitializedErrMsg)
)

// InitSummaryResponse is the json representation of the summary output
// of restic init
type InitSummaryResponse struct {
	Created bool `json:"created"`
}

type initExecutor struct {
	cmd          *Command
	responseLock sync.Mutex
	//summaryResponse *InitSummaryResponse
	execCmd   *exec.Cmd
	outBuf    *bytes.Buffer
	errBuf    *bytes.Buffer
	lastError error
	isRunning bool
}

// GetInitCommand returns a wrapper over the restic init command
func GetInitCommand(path, repoName, password, provider string) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	logrus.Infof("line 48 password: %v", password)
	return &Command{
		Name:           "create",
		Provider:       provider,
		RepositoryName: repoName,
		Password:       password,
		Path:           path,
	}, nil
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
	logrus.Infof("line 71 Run()")
	//b.responseLock.Lock()
	//defer b.responseLock.Unlock()

	/*if b.isRunning {
		return fmt.Errorf("another init operation is already running")
	}*/

	b.execCmd = b.cmd.InitCmd()
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
			// TODO: Add error handling parsing details
			if err = parseStdErr(b.errBuf.Bytes()); err != nil {
				b.lastError = err
			}
			return
		}

		summaryResponse, err := getInitSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
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
func (b *initExecutor) Status() (*Status, error) {
	//b.responseLock.Lock()
	//defer b.responseLock.Unlock()

	if b.lastError != nil {
		logrus.Infof("line 129 status err: %v", b.lastError)
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

func getInitSummary(outBytes []byte, errBytes []byte) (*InitSummaryResponse, error) {
	logrus.Infof("line 151 out: %+v", string(outBytes))
	logrus.Infof("line 152 err: %+v", string(errBytes))
	// For now checking if maintenance msg is printed to know if repository creation
	// is successful.
	if bytes.Contains(errBytes, []byte(initializedSuccesMsg)) {
		logrus.Infof("line 154 Successfully created repository")
		return &InitSummaryResponse{
			Created: true,
		}, nil
	}

	logrus.Infof("line 160 getInitSummary")
	return &InitSummaryResponse{
		Created: false,
	}, fmt.Errorf("failed to create repository")
}

func parseStdErr(stdErr []byte) error {
	outLines := bytes.Split(stdErr, []byte("\n"))
	//for i := len(outLines) - 1; i >= 0; i-- {
	for _, out := range outLines {
		logrus.Infof("line 182: out: %v", out)
		if bytes.Contains(out, []byte(alreadyInitializedErrMsg)) {
			logrus.Infof("line 183 parseStdErr")
			return ErrAlreadyInitialized
		}

		/*if len(outLines[i]) > 0 {
			return fmt.Errorf("%s", outLines[i])
		}*/
	}
	return fmt.Errorf("error while creating repository: %v", outLines)
}
