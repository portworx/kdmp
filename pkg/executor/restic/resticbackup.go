package restic

import (
	"fmt"
	"time"

	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/restic"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	progressCheckInterval = 5 * time.Second
)

func newBackupCommand() *cobra.Command {
	var (
		sourcePath string
	)
	backupCommand := &cobra.Command{
		Use:   "backup",
		Short: "Start a restic backup",
		Run: func(c *cobra.Command, args []string) {
			if len(backupLocationFile) == 0 && len(backupLocationName) == 0 {
				util.CheckErr(fmt.Errorf("backup-location or backup-location-file has to be provided for restic backups"))
				return
			}
			if len(sourcePath) == 0 {
				util.CheckErr(fmt.Errorf("source-path argument is required for restic backups"))
				return
			}
			runBackup(sourcePath)
		},
	}
	backupCommand.Flags().StringVar(&sourcePath, "source-path", "", "Source for restic backup")
	return backupCommand
}

func runBackup(sourcePath string) {
	repositoryName, envs, err := executor.ParseBackupLocation(backupLocationName, namespace, backupLocationFile)
	if err != nil {
		//updateDataExportStatusOnError(err)
		util.CheckErr(err)
		return
	}

	initCmd, err := restic.GetInitCommand(repositoryName, secretFilePath)
	if err != nil {
		//updateDataExportStatusOnError(err)
		util.CheckErr(err)
		return
	}
	initCmd.AddEnv(envs)
	initExecutor := restic.NewInitExecutor(initCmd)
	if err := initExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run backup command: %v", err)
		//updateDataExportStatusOnError(err)
		util.CheckErr(err)
		return
	}
	for {
		time.Sleep(progressCheckInterval)
		status, err := initExecutor.Status()
		if err != nil {
			util.CheckErr(status.LastKnownError)
			return
		}
		//updateDataExportStatus(status)
		if status.LastKnownError != nil && status.LastKnownError != restic.ErrAlreadyInitialized {
			util.CheckErr(status.LastKnownError)
			return
		}
		if status.Done {
			break
		}
	}

	backupCmd, err := restic.GetBackupCommand(repositoryName, secretFilePath, sourcePath)
	if err != nil {
		//updateDataExportStatusOnError(err)
		util.CheckErr(err)
		return
	}
	backupCmd.AddEnv(envs)
	backupExecutor := restic.NewBackupExecutor(backupCmd)
	if err := backupExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run backup command: %v", err)
		//updateDataExportStatusOnError(err)
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
		//updateDataExportStatus(status)
		if status.LastKnownError != nil {
			util.CheckErr(status.LastKnownError)
			return
		}
		if status.Done {
			return
		}
	}
}

// Update the ExportProgress object
/*func updateDataExportStatus(status *restic.Status) {
	de, err := utils.Instance().GetDataExport(dataExportName, namespace)
	if err != nil {
		logrus.Errorf("failed to update status for DataExport %v object with error: %v", dataExportName, err)
		return
	}
	if status.Done {
		if status.LastKnownError != nil {
			de.Status.Status = kdmpv1alpha1.DataExportStatusFailed
			de.Status.Reason = status.LastKnownError.Error()
		} else {
			de.Status.Status = kdmpv1alpha1.DataExportStatusSuccessful
			de.Status.ProgressPercentage = int(status.ProgressPercentage)
		}
	} else {
		de.Status.Status = kdmpv1alpha1.DataExportStatusInProgress
		de.Status.ProgressPercentage = int(status.ProgressPercentage)
	}
	if _, err = utils.Instance().UpdateDataExportStatus(de); err != nil {
		logrus.Errorf("failed to update status for DataExport %v object with error: %v", dataExportName, err)
	}
	return
}

func updateDataExportStatusOnError(exportErr error) {
	de, err := utils.Instance().GetDataExport(dataExportName, namespace)
	if err != nil {
		logrus.Errorf("failed to update status for DataExport %v object with error: %v", dataExportName, err)
		return
	}
	de.Status.Status = kdmpv1alpha1.DataExportStatusFailed
	de.Status.Reason = exportErr.Error()
	if _, err = utils.Instance().UpdateDataExportStatus(de); err != nil {
		logrus.Errorf("failed to update status for DataExport %v object with error: %v", dataExportName, err)
	}
	return
}*/
