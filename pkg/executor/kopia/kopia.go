package kopia

import (
	"flag"

	"github.com/spf13/cobra"

	"k8s.io/kubectl/pkg/cmd/util"
)

var (
	namespace        string
	volumeBackupName string
	kopiaRepo        string
	credentials      string
)

// NewCommand returns a kopia command wrapper
func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "kopia_executor",
		Short: "a command executor for long running kopia commands",
	}

	// TODO: More flags to be added in later changes
	cmds.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", "Namespace for this command")
	cmds.PersistentFlags().StringVar(&kopiaRepo, "repository", "", "Name of the kopia repository. If provided it will overwrite the BackupLocation one")
	cmds.PersistentFlags().StringVarP(&credentials, "credentials", "c", "", "Secret holding repository credentials")

	cmds.AddCommand(
		newBackupCommand(),
	)
	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}
