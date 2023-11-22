package kopia

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func newDeleteCommand() *cobra.Command {
	var (
		snapshotID                  string
		credSecretName              string
		credSecretNamespace         string
		volumeBackupDeleteName      string
		volumeBackupDeleteNamespace string
		logLevelDebug               string
	)
	deleteCommand := &cobra.Command{
		Use:   "delete",
		Short: "delete a backup snapshot",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(runDelete(snapshotID, volumeBackupDeleteName, volumeBackupDeleteNamespace))
		},
	}
	deleteCommand.Flags().StringVar(&snapshotID, "snapshot-id", "", "snapshot ID for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&credSecretName, "cred-secret-name", "", " cred secret name for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&credSecretNamespace, "cred-secret-namespace", "", "cred secret namespace for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&volumeBackupDeleteName, "volume-backup-delete-name", "", "volumeBackupdelete CR name for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&volumeBackupDeleteNamespace, "volume-backup-delete-namespace", "", "volumeBackupdelete CR namespace for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&logLevelDebug, "log-level", "", "If debug mode in kopia is to be used")
	return deleteCommand
}

func isNfsKopiaRepositoryFileExists(repo *executor.Repository) bool {
	repoBaseDir := repo.Path + genericBackupDir + "/"
	kopiaRepoFile := filepath.Join(repoBaseDir, repo.Name, kopiaNFSRepositoryFile)
	_, err := os.Stat(kopiaRepoFile)
	if err != nil && os.IsNotExist(err) {
		logrus.Debugf("kopia repository file %v does not exist", kopiaRepoFile)
		return false
	}
	return true
}

func runDelete(snapshotID, volumeBackupDeleteName, volumeBackupDeleteNamespace string) error {
	// Parse using the mounted secrets
	fn := "runDelete:"
	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		errMsg := fmt.Sprintf("failed in parsing backuplocation: %s", rErr)
		logrus.Errorf("%s %v", fn, errMsg)
		if err := executor.WriteVolumeBackupDeleteStatus(kdmpapi.VolumeBackupDeleteStatusFailed, errMsg, volumeBackupDeleteName, volumeBackupDeleteNamespace); err != nil {
			errMsg := fmt.Sprintf("failed in updating VolumeBackupDelete CR [%s:%s]: %v", volumeBackupDeleteName, volumeBackupDeleteNamespace, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(errMsg)
	}

	repo.Name = frameBackupPath()

	if repo.Type == storkv1.BackupLocationNFS {
		if !isNfsKopiaRepositoryFileExists(repo) {
			logrus.Info("kopia repository file is not found in the NFS backuplocation and nothing to process for deletion")
			return nil
		}
	}

	if err := runKopiaRepositoryConnect(repo); err != nil {
		errMsg := fmt.Sprintf("repository [%v] connect failed: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		if err := executor.WriteVolumeBackupDeleteStatus(kdmpapi.VolumeBackupDeleteStatusFailed, errMsg, volumeBackupDeleteName, volumeBackupDeleteNamespace); err != nil {
			errMsg := fmt.Sprintf("failed in updating VolumeBackupDelete CR [%s:%s]: %v", volumeBackupDeleteName, volumeBackupDeleteNamespace, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(errMsg)
	}

	snapshotList, err := runKopiaSnapshotList(repo)
	if err != nil {
		errMsg := fmt.Sprintf("snapshot list failed: %v", err)
		logrus.Errorf("%s: %v", fn, errMsg)
		if err := executor.WriteVolumeBackupDeleteStatus(kdmpapi.VolumeBackupDeleteStatusFailed, errMsg, volumeBackupDeleteName, volumeBackupDeleteNamespace); err != nil {
			errMsg := fmt.Sprintf("failed in updating VolumeBackupDelete CR [%s:%s]: %v", volumeBackupDeleteName, volumeBackupDeleteNamespace, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(errMsg)
	}

	var snapshotIdFound bool
	for _, data := range snapshotList {
		if data == snapshotID {
			snapshotIdFound = true
			break
		}
	}

	if !snapshotIdFound {
		logrus.Warnf("the snapshot ID [%v] does not exist in the backup location and thus cannot be deleted", snapshotID)
		return nil
	}

	if err := runKopiaDelete(repo, snapshotID); err != nil {
		errMsg := fmt.Sprintf("snapshot [%v] delete failed: %v", snapshotID, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		if err := executor.WriteVolumeBackupDeleteStatus(kdmpapi.VolumeBackupDeleteStatusFailed, errMsg, volumeBackupDeleteName, volumeBackupDeleteNamespace); err != nil {
			errMsg := fmt.Sprintf("failed in updating VolumeBackupDelete CR [%s:%s]: %v", volumeBackupDeleteName, volumeBackupDeleteNamespace, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(errMsg)
	}

	return nil
}

func runKopiaDelete(repository *executor.Repository, snapshotID string) error {
	fn := "runKopiaDelete:"
	logrus.Infof("kopia delete started")
	deleteCmd, err := kopia.GetDeleteCommand(
		snapshotID,
	)
	if err != nil {
		errMsg := fmt.Sprintf("getting delete backup snapshot command for snapshot ID [%v] failed: %v", snapshotID, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	// Check and add debug level logs for kopia delete command
	deleteCmd = isKopiaDebugModeEnabled(deleteCmd, logLevelDebug)

	initExecutor := kopia.NewDeleteExecutor(deleteCmd)
	if err := initExecutor.Run(); err != nil {
		errMsg := fmt.Sprintf("running delete backup snapshot command for snapshotID [%v] failed: %v", snapshotID, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	for {
		time.Sleep(progressCheckInterval)
		status, err := initExecutor.Status()
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
	logrus.Infof("successfully delete snapshot with ID : [%v]", snapshotID)
	return nil
}

func runKopiaSnapshotList(repository *executor.Repository) ([]string, error) {
	var err error
	var listCmd *kopia.Command
	logrus.Infof("Executing kopia snapshot list command")
	listCmd, err = kopia.GetListCommand()
	if err != nil {
		return nil, err
	}

	// Check and add bebug level logs for kopia snapshot list command
	listCmd = isKopiaDebugModeEnabled(listCmd, logLevelDebug)

	listExecutor := kopia.NewListExecutor(listCmd)
	if err := listExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run snapshot list command: %v", err)
		return nil, err
	}

	for {
		time.Sleep(progressCheckInterval)
		status, err := listExecutor.Status()
		if err != nil {
			return nil, err
		}
		if status.LastKnownError != nil {
			return nil, status.LastKnownError
		}

		if status.Done {
			logrus.Infof("kopia snapshot list command executed successfully")
			return status.SnapshotIDs, nil
		}
	}
}
