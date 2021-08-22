package kopia

import (
	"flag"

<<<<<<< HEAD
=======
	"github.com/sirupsen/logrus"
>>>>>>> PB-1808: Adding kopia tool as executor
	"github.com/spf13/cobra"

	"k8s.io/kubectl/pkg/cmd/util"
)

var (
	//namespace          string
	secretFilePath string
	credentials    string
	kopiaRepo      string
)

// NewCommand returns a kopia command wrapper
func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "kopia_executor",
		Short: "a command executor for long running kopia commands",
	}

	// TODO: More flags to be added in later changes
	cmds.PersistentFlags().StringVar(&kopiaRepo, "repository", "", "Name of the kopia repository")
	cmds.PersistentFlags().StringVarP(&secretFilePath, "secret-file-path", "s", "", "Path of the secret file used for locking/unlocking kopia repositories")
	cmds.PersistentFlags().StringVarP(&credentials, "credentials", "c", "", "Secret holding repository credentials")

	// TODO: Add commands here for all kopiaexecutor operations like
	// backup and restore
	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}
