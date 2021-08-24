package kopia

import (
	"os"
	"os/exec"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
)

const (
	baseCmd = "kopia"
)

// Command defines the essential fields required to
// execute any kopia commands
type Command struct {
	// Name is the name of the kopia sub command.
	Name string
	// Path is the bucket name for the repo
	Path string
	// RepositoryName is the name of the repository.
	RepositoryName string
	// Dir specifies the working directory of the command.
	Dir string
	// Args is a list of arguments to the kopia command.
	Args []string
	// Flags is a list of flags provided to the kopia command.
	// The order of the elements in the slice is important
	Flags []string
	// Envs is a list of environment variables to the kopia command.
	// Each entry is of the form "key=value".
	Env []string
	// Password is the env for storing password
	Password string
	// Provider storage provider (aws, Google, Azure)
	Provider string
}

// Executor interface defines APIs for implementing a command wrapper
// for long running export/restore commands in an asyncronous fashion with the ability
// to query for the status.
type Executor interface {
	// Run a long running command. Returns a unique CommandID that can be
	// used for fetching the status of the command
	Run() error

	// Status returns the status of
	Status() (*cmdexec.Status, error)
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
	// Get all the flags
	argsSlice := []string{
		"repository",
		c.Name, // create command
		"s3",
		"--bucket",
		c.Path,
		"--password",
		c.Password,
		"--prefix",
		c.RepositoryName,
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

// BackupCmd returns os/exec.Cmd object for the kopia create Command
func (c *Command) BackupCmd() *exec.Cmd {

	// Get all the flags
	argsSlice := []string{
		"snapshot",
		c.Name, // create command
		"--json",
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

// ConnectCmd returns os/exec.Cmd object for the kopia connect Command
func (c *Command) ConnectCmd() *exec.Cmd {
	// Get all the flags
	argsSlice := []string{
		"repository",
		c.Name, // connect command
		c.Provider,
		"--bucket",
		c.Path,
		"--password",
		c.Password,
		"--prefix",
		c.RepositoryName,	
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
