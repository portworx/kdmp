package nfs

import (
	"flag"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

var (
	uploadBkpResources  string
	restoreBkpResources string
)

// NewCommand returns a kopia command wrapper
func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "nfs_executor",
		Short: "a command executor for nfs target as support ",
	}

	// TODO: More flags to be added in later changes
	cmds.PersistentFlags().StringVar(&uploadBkpResources, "upload-backup-resource", "", "Option to upload backup resources")
	cmds.PersistentFlags().StringVar(&restoreBkpResources, "download-apply-resources", "", "Option to download the resources and apply the specs")

	cmds.AddCommand(
		newUploadBkpResourceCommand(),
		newRestoreResourcesCommand(),
	)

	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}
