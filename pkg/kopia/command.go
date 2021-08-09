package kopia

import (
	"os"
	"os/exec"

	"github.com/sirupsen/logrus"
)

const (
	baseCmd = "kopia"
	//baseCmd = "ls"
)

// Command defines the essential fields required to
// execute any restic commands
type Command struct {
	// Name is the name of the restic sub command.
	Name string
	// RepositoryName is the name of the restic repository.
	RepositoryName string
	// SecretFilePath is the file path which has the password
	// for the restic repository.
	SecretFilePath string
	// Dir specifies the working directory of the command.
	Dir string
	// Args is a list of arguments to the restic command.
	Args []string
	// Flags is a list of flags provided to the restic command.
	// The order of the elements in the slice is important
	Flags []string
	// Envs is a list of environment variables to the restic command.
	// Each entry is of the form "key=value".
	Env []string
	// Password is the env for storing password
	Password string
}


// Status is the current status of the command being executed
type Status struct {
	// ProgressPercentage is the progress of the command in percentage
	ProgressPercentage float64
	// TotalBytesProcessed is the no. of bytes processed
	TotalBytesProcessed uint64
	// TotalBytes is the total no. of bytes to be backed up
	TotalBytes uint64
	// SnapshotID is the snapshot ID of the backup being handled
	SnapshotID string
	// Done indicates if the operation has completed
	Done bool
	// LastKnownError is the last known error of the command
	LastKnownError error
}

// Executor interface defines APIs for implementing a command wrapper
// for long running export/restore commands in an asyncronous fashion with the ability
// to query for the status.
type Executor interface {
	// Run a long running command. Returns a unique CommandID that can be
	// used for fetching the status of the command
	Run() error

	// Status returns the status of
	Status() (*Status, error)
}

// AddArg adds an argument to the command
func (c *Command) AddArg(arg string) *Command {
	c.Args = append(c.Args, arg)
	return c
}

// AddFlag adds a flag to the command
func (c *Command) AddFlag(flag string) *Command {
	c.Flags = append(c.Flags, flag)
	return c
}

// AddEnv adds environment variables to the command
func (c *Command) AddEnv(envs []string) *Command {
	c.Env = append(c.Env, envs...)
	return c
}

// Cmd returns os/exec.Cmd object for the provided Command
func (c *Command) InitCmd() *exec.Cmd {
	logrus.Infof("line 85 InitCmd()")

	// Get all the flags
	argsSlice := []string{
		"repository",
		c.Name, // create command
		// TODO: Move this to flags from the caller for different cloud provider
		"s3",
		c.RepositoryName,
		"--password",
		c.Password,
		"--access-key=CT6R80D3ST0VW9NY6HYP",
		"--secret-access-key=a0V6dPqu8C26KbAsa9qsIrfhsbvyGjjPPmZN2qD4",
		"--bucket=kopiapk",
		//"--endpoint=minio.portworx.dev",
	}
	argsSlice = append(argsSlice, c.Flags...)
	// Get the cmd args
	argsSlice = append(argsSlice, c.Args...)
	cmd := exec.Command(baseCmd, argsSlice...)
	if len(c.Env) > 0 {
		cmd.Env = append(os.Environ(), c.Env...)
	}
	cmd.Dir = c.Dir
	logrus.Infof("line 103 InitCmd cmd: %+v", cmd)
	return cmd
}