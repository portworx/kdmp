package restic

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/restic"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
			srcPath, err := getSourcPath(sourcePath, sourcePathGlob)
			if err != nil {
				util.CheckErr(err)
				return
			}

			handleErr(runBackup(srcPath))
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
		if statusErr := writeVolumeBackupStatus(&restic.Status{LastKnownError: err}); statusErr != nil {
			return statusErr
		}
		return fmt.Errorf("parse backuplocation: %s", err)
	}

	if volumeBackupName != "" {
		if err = createVolumeBackup(volumeBackupName, namespace, repo.Name); err != nil {
			return err
		}
	}

	if err = runResticInit(repo.Path, repo.AuthEnv); err != nil {
		return fmt.Errorf("run restic init: %s", err)
	}
	logrus.Infof("line 71 runBackup sourcePath: %v", sourcePath)
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
		if err = writeVolumeBackupStatus(status); err != nil {
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
		if err = writeVolumeBackupStatus(status); err != nil {
			logrus.Errorf("failed to write a VolumeBackup status: %v", err)
			continue
		}
		if status.Done {
			break
		}
	}

	return nil
}

// writeVolumeBackupStatus writes a restic status to the VolumeBackup crd.
func writeVolumeBackupStatus(status *restic.Status) error {
	if volumeBackupName == "" {
		return nil
	}

	vb, err := kdmpops.Instance().GetVolumeBackup(context.Background(), volumeBackupName, namespace)
	if err != nil {
		return fmt.Errorf("get %s/%s VolumeBackup: %v", volumeBackupName, namespace, err)
	}

	vb.Status.ProgressPercentage = status.ProgressPercentage
	vb.Status.TotalBytes = status.TotalBytes
	vb.Status.TotalBytesProcessed = status.TotalBytesProcessed
	vb.Status.SnapshotID = status.SnapshotID
	if status.LastKnownError != nil {
		vb.Status.LastKnownError = status.LastKnownError.Error()
	} else {
		vb.Status.LastKnownError = ""
	}

	if _, err = kdmpops.Instance().UpdateVolumeBackup(context.Background(), vb); err != nil {
		return fmt.Errorf("update %s/%s VolumeBackup: %v", volumeBackupName, namespace, err)
	}
	return nil
}

func createVolumeBackup(name, namespace, repository string) error {
	new := &kdmpapi.VolumeBackup{
		ObjectMeta: v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kdmpapi.VolumeBackupSpec{
			Repository: repository,
			BackupLocation: kdmpapi.DataExportObjectReference{
				Name:      backupLocationName,
				Namespace: namespace,
			},
		},
	}

	vb, err := kdmpops.Instance().GetVolumeBackup(context.Background(), name, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = kdmpops.Instance().CreateVolumeBackup(context.Background(), new)
		}
		return err
	}

	if !reflect.DeepEqual(vb.Spec, new.Spec) {
		return fmt.Errorf("volumebackup %s/%s with different spec already exists", namespace, name)
	}

	if vb.Status.SnapshotID != "" {
		return fmt.Errorf("volumebackup %s/%s with snapshot id already exists", namespace, name)
	}

	return nil
}

func getSourcPath(path, glob string) (string, error) {
	if len(path) == 0 && len(glob) == 0 {
		return "", fmt.Errorf("source-path argument is required for restic backups")
	}

	if len(path) > 0 {
		return path, nil
	}

	matches, err := filepath.Glob(glob)
	if err != nil {
		return "", fmt.Errorf("parse source-path-glob: %s", err)
	}

	if len(matches) != 1 {
		return "", fmt.Errorf("parse source-path-glob: invalid amount of matches: %v", matches)
	}

	return matches[0], nil
}

func handleErr(err error) {
	if err != nil {
		util.CheckErr(err)
	}
}
