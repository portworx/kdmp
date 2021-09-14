package kopia

import (
	"fmt"

	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

var (
	restoreNamespace string
)

func newRestoreCommand() *cobra.Command {
	var (
		targetPath string
		snapshotID string
	)
	restoreCommand := &cobra.Command{
		Use:   "restore",
		Short: "Start a kopia restore",
		Run: func(c *cobra.Command, args []string) {
			if len(backupLocationName) == 0 {
				util.CheckErr(fmt.Errorf("backup-location has to be provided for kopia restores"))
				return
			}
			if len(targetPath) == 0 {
				util.CheckErr(fmt.Errorf("target-path argument is required for kopia restores"))
				return
			}
			executor.HandleErr(runRestore(snapshotID, targetPath))
		},
	}
	restoreCommand.Flags().StringVarP(&restoreNamespace, "restore-namespace", "", "", "Namespace for restore command")
	restoreCommand.Flags().StringVar(&targetPath, "target-path", "", "Destination path for kopia restore")
	restoreCommand.Flags().StringVar(&snapshotID, "snapshot-id", "", "Snapshot id of the restore")
	return restoreCommand
}

// VolumeBackup CR are present in the same namespace as the restore PVC
// be restored namespace. Stork would take care of creating it
func runRestore(snapshotID, targetPath string) error {
	logrus.Infof("Restore started from snapshotID: %s", snapshotID)
	// Parse using the mounted secrets
	fn := "runRestore"
	repo, err := executor.ParseCloudCred()

	if err != nil {
		errMsg := fmt.Sprintf("parse backuplocation failed: %s", err)
		logrus.Errorf("%s: %v", fn, errMsg)
		if statusErr := executor.WriteVolumeBackupStatus(
			&executor.Status{LastKnownError: err},
			volumeBackupName,
			restoreNamespace,
		); statusErr != nil {
			errMsg = fmt.Sprintf("Updating volume backup cr failed with error %s", statusErr)
			return fmt.Errorf("%s: %v", fn, errMsg)
		}
		return fmt.Errorf(errMsg)
	}

	repo.Name = kopiaRepo

	if err = runKopiaRepositoryConnect(repo); err != nil {
		errMsg := fmt.Sprintf("repository %s connect failed: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	if err = runKopiaRestore(repo, targetPath, snapshotID); err != nil {
		errMsg := fmt.Sprintf("restore failed: %v", err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func runKopiaRestore(repository *executor.Repository, targetPath, snapshotID string) error {
	logrus.Infof("kopia restore started from snapshot %s", snapshotID)
	restoreCmd, err := kopia.GetRestoreCommand(
		repository.Path,
		repository.Name,
		repository.Password,
		string(repository.Type),
		targetPath,
		snapshotID,
	)
	if err != nil {
		return err
	}

	initExecutor := kopia.NewRestoreExecutor(restoreCmd)
	if err := initExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run restore command: %v", err)
		return err
	}

	t := func() (interface{}, bool, error) {
		status, err := initExecutor.Status()
		if err != nil {
			return "", false, err
		}
		if err = executor.WriteVolumeBackupStatus(
			status,
			volumeBackupName,
			restoreNamespace,
		); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
			logrus.Errorf("%v", errMsg)
			return "", false, fmt.Errorf(errMsg)
		}
		if status.LastKnownError != nil {
			return "", false, status.LastKnownError
		}
		if status.Done {
			return "", false, nil
		}

		return "", true, fmt.Errorf("restore status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		return err
	}

	logrus.Infof("kopia restore successful from snapshot %s", snapshotID)

	return nil
}
