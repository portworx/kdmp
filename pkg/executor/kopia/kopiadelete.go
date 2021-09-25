package kopia

import (
	"fmt"
	"time"

	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func newDeleteCommand() *cobra.Command {
	var (
		snapshotID            string
		credSecretName        string
		credSecretNamespace   string
		volumeDeleteName      string
		volumeDeleteNamespace string
	)
	deleteCommand := &cobra.Command{
		Use:   "delete",
		Short: "delete a backup snapshot",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(runDelete(snapshotID))
		},
	}
	deleteCommand.Flags().StringVar(&snapshotID, "snapshot-id", "", "snapshot ID for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&credSecretName, "cred-secret-name", "", " cred secret name for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&credSecretNamespace, "cred-secret-namespace", "", "cred secret namespace for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&volumeDeleteName, "volume-delete-name", "", "volumedelete CR name for kopia backup snapshot that need to be deleted")
	deleteCommand.Flags().StringVar(&volumeDeleteNamespace, "volume-delete-namespace", "", "volumedelete CR namespace for kopia backup snapshot that need to be deleted")
	return deleteCommand
}

func runDelete(snapshotID string) error {
	// Parse using the mounted secrets
	fn := "runDelete:"
	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		errMsg := fmt.Sprintf("failed in parseing backuplocation: %s", rErr)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	repo.Name = frameBackupPath()

	if err := runKopiaRepositoryConnect(repo); err != nil {
		errMsg := fmt.Sprintf("repository [%v] connect failed: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	if err := runKopiaDelete(repo, snapshotID); err != nil {
		errMsg := fmt.Sprintf("snapshot [%v] delete failed: %v", snapshotID, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

func runKopiaDelete(repository *executor.Repository, snapshotID string) error {
	fn := "runKopiaDelete:"
	deleteCmd, err := kopia.GetDeleteCommand(
		snapshotID,
	)
	if err != nil {
		errMsg := fmt.Sprintf("getting delete backup snapshot command for snapshot ID [%v] failed: %v", snapshotID, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
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
	return nil
}
