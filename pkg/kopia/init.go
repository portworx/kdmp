package kopia

import (
	"bytes"
	"fmt"
	"os/exec"
	"sync"
	"time"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/sirupsen/logrus"
)

const (
	progressCheckInterval = 5 * time.Second
)

const (
	alreadyInitializedErrMsg = "repository master key and config already initialized"
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
	cmd             *Command
	responseLock    sync.Mutex
	summaryResponse *InitSummaryResponse
	execCmd         *exec.Cmd
	outBuf          *bytes.Buffer
	errBuf          *bytes.Buffer
	lastError       error
	isRunning       bool
}

// GetInitCommand returns a wrapper over the restic init command
func GetInitCommand(repoName string, secretFilePath string) (*Command, error) {
	if repoName == "" {
		return nil, fmt.Errorf("repository name cannot be empty")
	}
	if secretFilePath == "" {
		return nil, fmt.Errorf("secret file path cannot be empty")
	}
	// Fixme: don't need to store this
	return &Command{
		Name:           "create",
		RepositoryName: repoName,
		//SecretFilePath: secretFilePath,
		Password: drivers.SecretValue,
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
	/*kout, err := exec.Command("ls").Output()
	//err := tcmd.Run()
    logrus.Infof("line 85 ls out: %v", string(kout))		
    if err != nil {
        logrus.Errorf("line 87")
    }*/

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
		b.responseLock.Lock()
		defer b.responseLock.Unlock()
		logrus.Infof("line 103 stdout: %v", b.execCmd.Stdout)
		logrus.Infof(" line 104 Stderr: %v", b.execCmd.Stderr)
		if err != nil {
			b.lastError = fmt.Errorf("failed to run the init command: %v", err)
			// TODO: Add error handling parsing details
			/*if err = parseStdErr(b.errBuf.Bytes()); err != nil {
				b.lastError = err
			}*/
			logrus.Infof("line 111 stdout: %v", b.execCmd.Stdout)
		 	logrus.Infof(" line 112 Stderr: %v", b.execCmd.Stderr)
			logrus.Infof("line 113 err: %v", err)
			return
		}

		/*summaryResponse, err := getInitSummary(b.outBuf.Bytes(), b.errBuf.Bytes())
		if err != nil {
			b.lastError = err
			return
		}
		b.summaryResponse = summaryResponse*/
		logrus.Infof("line 112")
	}()
	logrus.Infof("line 112 stdout: %v", b.execCmd.Stdout)
	logrus.Infof(" line 113 Stderr: %v", b.execCmd.Stderr)
	return nil
}
// TODO: Implement status fully
func (b *initExecutor) Status() (*Status, error) {
	return &Status{
		Done:           false,
		LastKnownError: nil,
	}, nil
}