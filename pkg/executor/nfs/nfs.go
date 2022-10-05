package nfs

import (
	"flag"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

var (
	//uploadBkpResources  string
	//restoreBkpResources string
	appRestoreCRName     string
	appBackupCRName      string
	restoreNamespace     string
	applicationrestoreCR string
	bkpNamespace         string
	rbCrName             string
	rbCrNamespace        string
)

// NewCommand returns a kopia command wrapper
func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "nfs_executor",
		Short: "a command executor for nfs target as support ",
	}

	// TODO: More flags to be added in later changes
	//cmds.PersistentFlags().StringVar(&uploadBkpResources, "upload-backup-resource", "", "Option to upload backup resources")
	//cmds.PersistentFlags().StringVar(&restoreBkpResources, "download-apply-resources", "", "Option to download the resources and apply the specs")
	//cmds.PersistentFlags().StringVar(&restoreBkpResources, "restore-volume", "", "Option to restore volume content")
	cmds.PersistentFlags().StringVarP(&restoreNamespace, "restore-namespace", "", "", "Namespace for restore CR")
	cmds.PersistentFlags().StringVarP(&appRestoreCRName, "app-cr-name", "", "", "application restore CR name")
	cmds.PersistentFlags().StringVarP(&rbCrName, "rb-cr-name", "", "", "Name for resourcebackup CR to update job status")
	cmds.PersistentFlags().StringVarP(&rbCrNamespace, "rb-cr-namespace", "", "", "Namespace for resourcebackup CR to update job status")

	cmds.AddCommand(
		newUploadBkpResourceCommand(),
		newRestoreResourcesCommand(),
		newDeleteResourcesCommand(),
		newRestoreVolumeCommand(),
	)

	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}
