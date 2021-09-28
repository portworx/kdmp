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
	// Env is a list of environment variables to the kopia command.
	// Each entry is of the form "key=value".
	Env []string
	// Password is the env for storing password
	Password string
	// Provider storage provider (aws, Google, Azure)
	Provider string
	// SnapshotID snapshot ID
	SnapshotID string
	// MaintenanceOwner owner of maintenance command
	MaintenanceOwner string
	// DisableSsl option to disable ssl for s3
	DisableSsl bool
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

// CreateCmd returns os/exec.Cmd object for the kopia repo create Command
func (c *Command) CreateCmd() *exec.Cmd {
	// Get all the flags
	var argsSlice []string
	switch c.Provider {
	case "azure":
		argsSlice = []string{
			"repository",
			c.Name, // create command
			c.Provider,
			"--password",
			c.Password,
			"--prefix",
			c.RepositoryName,
		}
	case "s3", "google":
		argsSlice = []string{
			"repository",
			c.Name, // create command
			c.Provider,
			"--bucket",
			c.Path,
			"--password",
			c.Password,
			"--prefix",
			c.RepositoryName,
		}
	}
	var ssl []string
	if c.DisableSsl {
		ssl = []string{
			"--disable-tls",
		}
	}
	argsSlice = append(argsSlice, ssl...)
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
	var argsSlice []string
	switch c.Provider {
	case "azure":
		argsSlice = []string{
			"repository",
			c.Name, // connect command
			c.Provider,
			"--password",
			c.Password,
			"--prefix",
			c.RepositoryName,
		}
	case "s3", "google":
		argsSlice = []string{
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
	}
	var ssl []string
	if c.DisableSsl {
		ssl = []string{
			"--disable-tls",
		}
	}
	argsSlice = append(argsSlice, c.Flags...)
	argsSlice = append(argsSlice, ssl...)
	// Get the cmd args
	argsSlice = append(argsSlice, c.Args...)
	cmd := exec.Command(baseCmd, argsSlice...)
	if len(c.Env) > 0 {
		cmd.Env = append(os.Environ(), c.Env...)
	}
	cmd.Dir = c.Dir
	return cmd
}

// RestoreCmd returns os/exec.Cmd object for the kopia restore Command
func (c *Command) RestoreCmd() *exec.Cmd {
	// Get all the flags
	argsSlice := []string{
		"snapshot",
		c.Name, // restore command
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

// SetPolicyCmd returns os/exec.Cmd object for the kopia policy Command
func (c *Command) SetPolicyCmd() *exec.Cmd {
	// Get all the flags
	argsSlice := []string{
		"policy",
		c.Name, // set command
		"--global",
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

// DeleteCmd returns os/exec.Cmd object for the kopia snapshot delete Command
func (c *Command) DeleteCmd() *exec.Cmd {
	// Get all the flags
	argsSlice := []string{
		"snapshot",
		c.Name, // delete command
		c.SnapshotID,
		"--delete",
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

// MaintenanceRunCmd returns os/exec.Cmd object for the kopia maintenance run Command
func (c *Command) MaintenanceRunCmd() *exec.Cmd {
	// Get all the flags
	argsSlice := []string{
		c.Name, // maintenance command
		"run",
		"--full",
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

// MaintenanceSetCmd returns os/exec.Cmd object for the kopia maintenance set Command
func (c *Command) MaintenanceSetCmd() *exec.Cmd {
	// Get all the flags

	argsSlice := []string{
		c.Name, // maintenance command
		"set",
		"--owner",
		c.MaintenanceOwner,
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
