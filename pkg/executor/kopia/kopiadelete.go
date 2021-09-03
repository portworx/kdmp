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
		snapshotID string
	)
	backupCommand := &cobra.Command{
		Use:   "delete",
		Short: "delate a kopia backup snapshot",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(runDelete(snapshotID))
		},
	}
	backupCommand.Flags().StringVar(&snapshotID, "snapshot-id", "", "snapshot ID for kopia backup snapshot that need to be deleted")
	return backupCommand
}

func runDelete(snapshotID string) error {
	// Parse using the mounted secrets
	fn := "runBackup"
	repo, rErr := executor.ParseCloudCred()
	repo.Name = frameBackupPath()

	if rErr != nil {
		return fmt.Errorf("parse backuplocation: %s", rErr)
	}

	// kopia doesn't have a way to know if repository is already initialized.
	// Repository create needs to run only first time.
	// Check if kopia.repository exists
	exists, err := isRepositoryExists(repo)
	if err != nil {
		errMsg := fmt.Sprintf("repository exists check for repo %s failed: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf("%s: %v", errMsg, err)
	}

	if !exists {
		if err = runKopiaCreateRepo(repo); err != nil {
			errMsg := "repo creation failed"
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf("%s: %v", errMsg, err)
		}
	}

	if err = runKopiaRepositoryConnect(repo); err != nil {
		errMsg := fmt.Sprintf("repository connect failed: %v", err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	if err = runKopiaDelete(repo, snapshotID); err != nil {
		errMsg := fmt.Sprintf("snapshot delete failed: %v", err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

func runKopiaDelete(repository *executor.Repository, snapshotID string) error {
	logrus.Infof("kopia backup started...")
	deleteCmd, err := kopia.GetDeleteCommand(
		repository.Path,
		repository.Name,
		repository.Password,
		string(repository.Type),
		snapshotID,
	)
	if err != nil {
		return err
	}
	// This is needed to handle case where after kopia repo create was successful and
	// the pod got terminated. Now user triggers another backup, so we need to pass
	// credentials for "snapshot create".
	initExecutor := kopia.NewDeleteExecutor(deleteCmd)
	if err := initExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run delete command: %v", err)
		return err
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

	logrus.Infof("kopia delete successful...")

	return nil
}
