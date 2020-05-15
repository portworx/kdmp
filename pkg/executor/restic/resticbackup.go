package restic

import (
	"fmt"
	"time"

	kdmpv1alpha1 "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/restic"
	"github.com/portworx/kdmp/pkg/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	progressCheckInterval = 1 * time.Minute
)

func newBackupCommand() *cobra.Command {
	var (
		backupLocationName string
		sourcePath         string
	)
	backupCommand := &cobra.Command{
		Use:   "backup",
		Short: "Start a restic backup",
		Run: func(c *cobra.Command, args []string) {
			if len(backupLocationName) == 0 {
				util.CheckErr(fmt.Errorf("backup-location argument is required for restic backups"))
				return
			}
			if len(sourcePath) == 0 {
				util.CheckErr(fmt.Errorf("source-path argument is required for restic backups"))
				return
			}
			runBackup(backupLocationName, sourcePath)
		},
	}
	backupCommand.Flags().StringVar(&backupLocationName, "backup-location", "", "Name of the BackupLocation object")
	backupCommand.Flags().StringVar(&sourcePath, "source-path", "", "Source for restic backup")
	return backupCommand
}

func runBackup(
	backupLocationName string,
	sourcePath string,
) {
	if len(dataExportName) == 0 {
		util.CheckErr(fmt.Errorf("dataexport argument is required for restic backups"))
		return
	}

	repositoryName, err := executor.ParseBackupLocation(backupLocationName, namespace)
	if err != nil {
		updateDataExportStatusOnError(err)
		util.CheckErr(err)
		return
	}

	backupCmd, err := restic.GetBackupCommand(repositoryName, secretFilePath, sourcePath)
	if err != nil {
		updateDataExportStatusOnError(err)
		util.CheckErr(err)
		return
	}
	backupExecutor := restic.NewBackupExecutor(backupCmd)
	if err := backupExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run backup command: %v", err)
		updateDataExportStatusOnError(err)
		util.CheckErr(err)
		return
	}
	for {
		time.Sleep(progressCheckInterval)
		status, _ := backupExecutor.Status()
		updateDataExportStatus(status)
		if status.Done {
			return
		}
	}
}

func updateDataExportStatus(status *restic.Status) {
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
}
