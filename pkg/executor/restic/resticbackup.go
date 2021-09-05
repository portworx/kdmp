package restic

import (
	"fmt"
	"time"

	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/restic"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	progressCheckInterval = 5 * time.Second
)

func newBackupCommand() *cobra.Command {
	var (
		sourcePath     string
		sourcePathGlob string
	)
	backupCommand := &cobra.Command{
		Use:   "backup",
		Short: "Start a restic backup",
		Run: func(c *cobra.Command, args []string) {
			if len(backupLocationFile) == 0 && len(backupLocationName) == 0 {
				util.CheckErr(fmt.Errorf("backup-location or backup-location-file has to be provided for restic backups"))
				return
			}
			srcPath, err := executor.GetSourcePath(sourcePath, sourcePathGlob)
			if err != nil {
				util.CheckErr(err)
				return
			}

			executor.HandleErr(runBackup(srcPath))
		},
	}
	backupCommand.Flags().StringVar(&sourcePath, "source-path", "", "Source for restic backup")
	backupCommand.Flags().StringVar(&sourcePathGlob, "source-path-glob", "", "The regexp should match only one path that will be used for backup")
	backupCommand.Flags().StringVar(&volumeBackupName, "volume-backup-name", "", "Provided VolumeBackup CRD will be updated with the latest backup progress details")
	return backupCommand
}

func runBackup(sourcePath string) error {
	repo, err := executor.ParseBackupLocation(resticRepo, backupLocationName, namespace, backupLocationFile)
	if err != nil {
		if statusErr := executor.WriteVolumeBackupStatus(
			&executor.Status{LastKnownError: err},
			volumeBackupName,
			namespace,
		); statusErr != nil {
			return statusErr
		}
		return fmt.Errorf("parse backuplocation: %s", err)
	}

	if volumeBackupName != "" {
		if err = executor.CreateVolumeBackup(
			volumeBackupName,
			namespace,
			repo.Name,
			backupLocationName,
		); err != nil {
			return err
		}
	}

	if err = runResticInit(repo.Path, repo.AuthEnv); err != nil {
		return fmt.Errorf("run restic init: %s", err)
	}
	if err = runResticBackup(sourcePath, repo.Path, repo.AuthEnv); err != nil {
		return fmt.Errorf("run restic backup: %s", err)
	}

	fmt.Println("Backup has been successfully created")
	return nil
}

func runResticInit(repositoryName string, env []string) error {
	initCmd, err := restic.GetInitCommand(repositoryName, secretFilePath)
	if err != nil {
		return err
	}
	initCmd.AddEnv(env)
	initExecutor := restic.NewInitExecutor(initCmd)
	if err := initExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run backup command: %v", err)
		return err
	}
	for {
		time.Sleep(progressCheckInterval)
		status, err := initExecutor.Status()
		if err != nil {
			return err
		}
		if status.LastKnownError != nil {
			if status.LastKnownError != restic.ErrAlreadyInitialized {
				return status.LastKnownError
			}
			status.LastKnownError = nil
		}
		if err = executor.WriteVolumeBackupStatus(
			status,
			volumeBackupName,
			namespace,
		); err != nil {
			logrus.Errorf("failed to write a VolumeBackup status: %v", err)
			continue
		}
		if status.Done {
			break
		}
	}

	return nil
}

func runResticBackup(sourcePath, repositoryName string, env []string) error {
	backupCmd, err := restic.GetBackupCommand(repositoryName, secretFilePath, sourcePath)
	if err != nil {
		return err
	}
	backupCmd.AddEnv(env)
	backupExecutor := restic.NewBackupExecutor(backupCmd)
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
		if err = executor.WriteVolumeBackupStatus(
			status,
			volumeBackupName,
			namespace,
		); err != nil {
			logrus.Errorf("failed to write a VolumeBackup status: %v", err)
			continue
		}
		if status.Done {
			break
		}
	}

	return nil
}
