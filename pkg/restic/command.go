package restic

import (
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
)

const (
	baseCmd = "restic"
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
func (c *Command) Cmd() *exec.Cmd {
	// Get all the flags
	argsSlice := []string{
		c.Name,
		"--repo",
		c.RepositoryName,
		"--password-file",
		c.SecretFilePath,
	}
	argsSlice = append(argsSlice, c.Flags...)
	// Get the cmd args
	argsSlice = append(argsSlice, c.Args...)
	cmd := exec.Command(baseCmd, argsSlice...)
	if len(c.Env) > 0 {
		cmd.Env = append(os.Environ(), c.Env...)
	}
	cmd.Dir = c.Dir
	return cmd
}

// ID is a unique ID that identifies a running command
type ID string

// Executor interface defines APIs for implementing a command wrapper
// for long running export/restore commands in an asynchronous fashion with the ability
// to query for the status.
type Executor interface {
	// Run a long running command. Returns a unique CommandID that can be
	// used for fetching the status of the command
	Run() error

	// Status returns the status of
	Status() (*cmdexec.Status, error)
}

func defaultFlags() []string {
	return []string{"--host", "kdmp", "--json"}
}
