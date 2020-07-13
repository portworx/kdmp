package restic

import (
	"fmt"
	"time"

	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/restic"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

func newRestoreCommand() *cobra.Command {
	var (
		targetPath string
		snapshotID string
	)
	restoreCommand := &cobra.Command{
		Use:   "restore",
		Short: "Start a restic restore",
		Run: func(c *cobra.Command, args []string) {
			if len(backupLocationFile) == 0 && len(backupLocationName) == 0 {
				util.CheckErr(fmt.Errorf("backup-location or backup-location-file has to be provided for restic backups"))
				return
			}
			if len(targetPath) == 0 {
				util.CheckErr(fmt.Errorf("target-path argument is required for restic backups"))
				return
			}
			runRestore(snapshotID, targetPath)
		},
	}
	restoreCommand.Flags().StringVar(&targetPath, "target-path", "", "Destination path for restic restore backup")
	restoreCommand.Flags().StringVar(&snapshotID, "id", "", "Snapshot id of the backup")
	return restoreCommand
}

func runRestore(snapshotID, targetPath string) {
	repositoryName, env, err := executor.ParseBackupLocation(backupLocationName, namespace, backupLocationFile)
	if err != nil {
		util.CheckErr(err)
		return
	}

	backupCmd, err := restic.GetRestoreCommand(repositoryName, snapshotID, secretFilePath, targetPath)
	if err != nil {
		util.CheckErr(err)
		return
	}
	backupCmd.AddEnv(env)
	backupExecutor := restic.NewRestoreExecutor(backupCmd)
	if err := backupExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run backup command: %v", err)
		util.CheckErr(err)
		return
	}
	for {
		time.Sleep(progressCheckInterval)
		status, err := backupExecutor.Status()
		if err != nil {
			util.CheckErr(status.LastKnownError)
			return
		}
		if status.LastKnownError != nil {
			util.CheckErr(status.LastKnownError)
			return
		}
		if status.Done {
			return
		}
	}
}
