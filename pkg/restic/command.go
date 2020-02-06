package restic

import (
	"fmt"
	"os/exec"
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
	// Args is a list of arguments to the restic command.
	Args []string
	// Flags is a list of flags provided to the restic command.
	// The order of the elements in the slice is important
	Flags []string
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
	return exec.Command(baseCmd, argsSlice...)
}

// ID is a unique ID that identifies a running command
type ID string

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

// Error is the error returned by the command
type Error struct {
	// CmdOutput is the stdout received from the command
	CmdOutput string
	// CmdErr is the stderr received from the command
	CmdErr string
	// Reason is the actual reason describing the error
	Reason string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%v: Cmd Output [%v] Cmd Error [%v]", e.Reason, e.CmdOutput, e.CmdErr)
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

func defaultFlags() []string {
	return []string{"--host", "kdmp", "--json"}
}
