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
			handleErr(runRestore(snapshotID, targetPath))
		},
	}
	restoreCommand.Flags().StringVar(&targetPath, "target-path", "", "Destination path for restic restore backup")
	restoreCommand.Flags().StringVar(&snapshotID, "id", "", "Snapshot id of the backup")
	return restoreCommand
}

func runRestore(snapshotID, targetPath string) error {
	repo, err := executor.ParseBackupLocation(resticRepo, backupLocationName, namespace, backupLocationFile)
	if err != nil {
		return err
	}

	backupCmd, err := restic.GetRestoreCommand(repo.Path, snapshotID, secretFilePath, targetPath)
	if err != nil {
		return err
	}
	backupCmd.AddEnv(repo.AuthEnv)
	backupExecutor := restic.NewRestoreExecutor(backupCmd)
	if err := backupExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run backup command: %v", err)
		return err
	}
	for {
		time.Sleep(progressCheckInterval)
		status, err := backupExecutor.Status()
		if err != nil {
			return err
		}
		if status.LastKnownError != nil {
			return status.LastKnownError
		}
		if status.Done {
			break
		}
	}

	return nil
}
