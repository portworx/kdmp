package kopia

import (
	"flag"

	"github.com/spf13/cobra"

	"k8s.io/kubectl/pkg/cmd/util"
)

var (
	volumeBackupName        string
	kopiaRepo               string
	credentials             string
	backupLocationName      string
	backupLocationNamespace string
	logLevelDebug           string
)

// NewCommand returns a kopia command wrapper
func NewCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use:   "kopia_executor",
		Short: "a command executor for long running kopia commands",
	}

	// TODO: More flags to be added in later changes
	cmds.PersistentFlags().StringVar(&kopiaRepo, "repository", "", "Name of the kopia repository. If provided it will overwrite the BackupLocation one")
	cmds.PersistentFlags().StringVarP(&credentials, "credentials", "c", "", "Secret holding repository credentials")
	cmds.PersistentFlags().StringVar(&backupLocationName, "backup-location", "", "Name of the BackupLocation object, used for authentication")
	cmds.PersistentFlags().StringVar(&backupLocationNamespace, "backup-location-namespace", "", "Namespace of BackupLocation object, used for authentication")
	cmds.PersistentFlags().StringVar(&volumeBackupName, "volume-backup-name", "", "Provided VolumeBackup CRD will be updated with the latest backup progress details")
	cmds.PersistentFlags().StringVar(&logLevelDebug, "log-level", "", "If debug mode in kopia is to be used")

	cmds.AddCommand(
		newBackupCommand(),
		newRestoreCommand(),
		newDeleteCommand(),
		newMaintenanceCommand(),
	)
	cmds.PersistentFlags().AddGoFlagSet(flag.CommandLine)
	err := flag.CommandLine.Parse([]string{})
	if err != nil {
		util.CheckErr(err)
		return nil
	}

	return cmds
}
