package kopia

import (
	"fmt"

	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func newDeleteCommand() *cobra.Command {
	var (
		snapshotID          string
		credSecretName      string
		credSecretNamespace string
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

	// kopia doesn't have a way to know if repository is already initialized.
	// Repository create needs to run only first time.
	// Check if kopia.repository exists
	exists, err := isRepositoryExists(repo)
	if err != nil {
		errMsg := fmt.Sprintf("repository exists check for repo [%s] failed: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	if !exists {
		if err = runKopiaCreateRepo(repo); err != nil {
			errMsg := fmt.Sprintf("repository  [%v] creation failed: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	if err = runKopiaRepositoryConnect(repo); err != nil {
		errMsg := fmt.Sprintf("repository [%v] connect failed: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	if err = runKopiaDelete(repo, snapshotID); err != nil {
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
	// This is needed to handle case where after kopia repo create was successful and
	// the pod got terminated. Now user triggers another backup, so we need to pass
	// credentials for "snapshot create".
	initExecutor := kopia.NewDeleteExecutor(deleteCmd)
	if err := initExecutor.Run(); err != nil {
		errMsg := fmt.Sprintf("running delete backup snapshot command for snapshotID [%v] failed: %v", snapshotID, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	t := func() (interface{}, bool, error) {
		status, err := initExecutor.Status()
		if err != nil {
			return "", false, err
		}
		if status.LastKnownError != nil {
			return "", false, status.LastKnownError
		}

		if status.Done {
			return "", false, nil
		}
		return "", true, fmt.Errorf("backup snapshot delete command status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		return err
	}

	return nil
}
