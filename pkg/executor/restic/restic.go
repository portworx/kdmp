package restic

import (
	"flag"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

var (
	namespace      string
	dataExportName string
	secretFilePath string
)

// NewCommand returns a restic command wrapper
func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "restic_executor",
		Short: "a command executor for long running restic commands",
	}

	cmds.PersistentFlags().StringVarP(&namespace, "namespace", "n", "", "Namespace for this command")
	cmds.PersistentFlags().StringVarP(&dataExportName, "dataexport", "d", "", "Name of the DataExport object")
	cmds.PersistentFlags().StringVarP(&secretFilePath, "secret-file-path", "s", "", "Path of the secret file used for locking/unlocking restic reposiories")

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
