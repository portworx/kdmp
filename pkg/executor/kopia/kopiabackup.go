package kopia

import (
	"fmt"
	"path/filepath"
	"reflect"
	"time"

	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	// TODO: Add calls later just for vendoring purpose
	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gocloud.dev/blob"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	progressCheckInterval = 5 * time.Second
	genericBackupDir      = "generic-backup"
)

func newBackupCommand() *cobra.Command {
	var (
		sourcePath     string
		sourcePathGlob string
	)
	backupCommand := &cobra.Command{
		Use:   "backup",
		Short: "Start a kopia backup",
		Run: func(c *cobra.Command, args []string) {
			/*if len(backupLocationFile) == 0 && len(backupLocationName) == 0 {
				util.CheckErr(fmt.Errorf("backup-location or backup-location-file has to be provided for kopia backups"))
				return
			}*/
			srcPath, err := getSourcePath(sourcePath, sourcePathGlob)
			if err != nil {
				util.CheckErr(err)
				return
			}

			handleErr(runBackup(srcPath))
		},
	}
	backupCommand.Flags().StringVar(&sourcePath, "source-path", "", "Source for kopia backup")
	backupCommand.Flags().StringVar(&sourcePathGlob, "source-path-glob", "", "The regexp should match only one path that will be used for backup")
	backupCommand.Flags().StringVar(&volumeBackupName, "volume-backup-name", "", "Provided VolumeBackup CRD will be updated with the latest backup progress details")
	return backupCommand
}

func runBackup(sourcePath string) error {
	//repo, err := executor.ParseBackupLocation(kopiaRepo, backupLocationName, namespace, backupLocationFile)
	// Parse using the mounted secrets
	repo, err := executor.ParseCloudCred()
	repo.Name = frameBackupPath()

	logrus.Infof("line 54 runBackup - repo %+v", repo)
	if err != nil {
		if statusErr := writeVolumeBackupStatus(&kopia.Status{LastKnownError: err}); statusErr != nil {
			return statusErr
		}
		return fmt.Errorf("parse backuplocation: %s", err)
	}
	if volumeBackupName != "" {
		if err = createVolumeBackup(volumeBackupName, namespace, repo.Name); err != nil {
			return err
		}
	}
	// TODO: kopia doesn't have a way to know if repository is already initialzed.
	// Repository create needs to run only first time.
	// One option is to check if the repo path exists, if not do a repository create
	logrus.Infof("line 76 time: %v", time.Now())
	exists, err := isRepositoryExists(repo)
	if err != nil {
		return fmt.Errorf("run kopia repo check failed: %v", err)
	}

	if !exists {
		if err = runKopiaInit(repo); err != nil {
			return fmt.Errorf("run kopia init: %v", err)
		}
	}

	logrus.Infof("line 80 sourcePath: %v", sourcePath)

	time.Sleep(20 * time.Second)
	if err = runKopiaRepositoryConnect(repo); err != nil {
		return fmt.Errorf("run kopia repository connect failed: %v", err)
	}

	if err = runKopiaBackup(repo, sourcePath); err != nil {
		return fmt.Errorf("run kopia backup failed: %v", err)
	}

	//fmt.Println("Backup has been successfully created")
	logrus.Infof("line 89 time: %v", time.Now())
	return nil
}

func buildStorkBackupLocation(repository *executor.Repository) *storkapi.BackupLocation {
	//var backupType storkapi.BackupLocationType
	backupLocation := &storkapi.BackupLocation{
		ObjectMeta: meta.ObjectMeta{},
		Location:   storkapi.BackupLocationItem{},
	}
	switch repository.Type {
	case storkapi.BackupLocationS3:
		backupLocation.Location.S3Config = &storkapi.S3Config{
			AccessKeyID:      repository.S3Config.AccessKeyID,
			SecretAccessKey:  repository.S3Config.SecretAccessKey,
			Endpoint:         repository.S3Config.Endpoint,
			Region:           repository.S3Config.Endpoint,
		}
	}
	backupLocation.Location.Path = repository.Path
	backupLocation.ObjectMeta.Name = repository.Name
	backupLocation.Location.Type = storkapi.BackupLocationS3

	return backupLocation
}

func populateS3AccessDetails(initCmd *kopia.Command, repository *executor.Repository) *kopia.Command {
	// kopia is not honouring env variabels set in the pod so passing them as flags
	initCmd.AddArg("--endpoint")
	initCmd.AddArg(repository.S3Config.Endpoint)
	initCmd.AddArg("--access-key")
	initCmd.AddArg(repository.S3Config.AccessKeyID)
	initCmd.AddArg("--secret-access-key")
	initCmd.AddArg(repository.S3Config.SecretAccessKey)

	return initCmd
}

func runKopiaInit(repository *executor.Repository) error {
	initCmd, err := kopia.GetInitCommand(repository.Path, repository.Name, repository.Password, string(repository.Type))
	logrus.Infof("line 84 runKopiaInit cmd: %+v", initCmd)
	if err != nil {
		return err
	}
	// TODO: Add for other storagr providers
	initCmd = populateS3AccessDetails(initCmd, repository)
	initExecutor := kopia.NewInitExecutor(initCmd)
	if err := initExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run kopia init command: %v", err)
		return err
	}
	//TODO: Temp commented out
	for {
		time.Sleep(progressCheckInterval)
		status, err := initExecutor.Status()
		if err != nil {
			return err
		}
		if status.LastKnownError != nil {
			logrus.Infof("line 130 runKopiaInit")
			if status.LastKnownError != kopia.ErrAlreadyInitialized {
				logrus.Infof("line 132 runKopiaInit")
				return status.LastKnownError
			}
			status.LastKnownError = nil
		}
		logrus.Infof("line 137 runKopiaInit")
		// TODO: Enable this
		if err = writeVolumeBackupStatus(status); err != nil {
			logrus.Errorf("failed to write a VolumeBackup status: %v", err)
			continue
		}
		if status.Done {
			break
		}
	}
	logrus.Infof("line 138 kopia repository create successfully done")
	return nil
}

func runKopiaBackup(repository *executor.Repository, sourcePath string) error {
	backupCmd, err := kopia.GetBackupCommand(
		repository.Path,
		repository.Name,
		repository.Password,
		string(repository.Type),
		sourcePath,
	)
	logrus.Infof("line 104 runKopiaBackup cmd: %+v", backupCmd)
	if err != nil {
		return err
	}
	// This is needed to handle case where aftert kopia repo create it was successful
	// the pod got terminated. Now user trigerrs another backup, so we need to pass
	// credentials for "snapshot create".
	//backupCmd = populateS3AccessDetails(backupCmd, repository)
	initExecutor := kopia.NewBackupExecutor(backupCmd)
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
			if status.LastKnownError != kopia.ErrAlreadyInitialized {
				return status.LastKnownError
			}
			status.LastKnownError = nil
		}
		// TODO: Enable this
		logrus.Infof("line 179 status: %+v", status)
		if err = writeVolumeBackupStatus(status); err != nil {
			logrus.Errorf("failed to write a VolumeBackup status: %v", err)
			continue
		}
		if status.Done {
			break
		}
	}
	logrus.Infof("line 186 kopia backup successfully done")
	return nil
}

func runKopiaRepositoryConnect(repository *executor.Repository) error {
	connectCmd, err := kopia.GetConnectCommand(repository.Path, repository.Name, repository.Password, string(repository.Type))
	logrus.Infof("line 84 runKopiaInit cmd: %+v", connectCmd)
	if err != nil {
		return err
	}
	// TODO: Add for other storagr providers
	connectCmd = populateS3AccessDetails(connectCmd, repository)
	//initCmd.AddEnv(env)
	initExecutor := kopia.NewConnectExecutor(connectCmd)
	if err := initExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run repository connect  command: %v", err)
		return err
	}
	//TODO: Temp commented out
	for {
		time.Sleep(progressCheckInterval)
		status, err := initExecutor.Status()
		if err != nil {
			return err
		}
		if status.LastKnownError != nil {
			if status.LastKnownError != kopia.ErrAlreadyInitialized {
				return status.LastKnownError
			}
			status.LastKnownError = nil
		}
		// TODO: Enable this, not needed
		/*if err = writeVolumeBackupStatus(status); err != nil {
			logrus.Errorf("failed to write a VolumeBackup status: %v", err)
			continue
		}*/
		if status.Done {
			break
		}
	}
	logrus.Infof("line 227 kopia connect successfully done")
	return nil
}

// TODO: Can this be made common?
// writeVolumeBackupStatus writes a restic status to the VolumeBackup crd.
func writeVolumeBackupStatus(status *kopia.Status) error {
	if volumeBackupName == "" {
		return nil
	}

	vb, err := kdmpops.Instance().GetVolumeBackup(volumeBackupName, namespace)
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

	if _, err = kdmpops.Instance().UpdateVolumeBackup(vb); err != nil {
		return fmt.Errorf("update %s/%s VolumeBackup: %v", volumeBackupName, namespace, err)
	}
	return nil
}

// TODO: Can this be made common?
func createVolumeBackup(name, namespace, repository string) error {
	new := &kdmpapi.VolumeBackup{
		ObjectMeta: v1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kdmpapi.VolumeBackupSpec{
			Repository: repository,
			BackupLocation: kdmpapi.DataExportObjectReference{
				Name:      credentials,
				Namespace: namespace,
			},
		},
	}

	vb, err := kdmpops.Instance().GetVolumeBackup(name, namespace)
	if err != nil {
		if errors.IsNotFound(err) {
			_, err = kdmpops.Instance().CreateVolumeBackup(new)
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

func getSourcePath(path, glob string) (string, error) {
	if len(path) == 0 && len(glob) == 0 {
		return "", fmt.Errorf("source-path argument is required for kopia backups")
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

// Under backuplocaiton path, following path would be creaetd
// <bucket>/generic-backup/<pvcns - pvc>
func frameBackupPath() string {
	return genericBackupDir + "/" + kopiaRepo + "/"
}

func buildStorkBackupLocation(repository *executor.Repository) (*storkv1.BackupLocation, error) {
	var backupType storkv1.BackupLocationType
	backupLocation := &storkv1.BackupLocation{
		ObjectMeta: metav1.ObjectMeta{},
		Location:   storkv1.BackupLocationItem{},
	}

	switch repository.Type {
	case storkv1.BackupLocationS3:
		backupType = storkv1.BackupLocationS3
		backupLocation.Location.S3Config = &storkv1.S3Config{
			AccessKeyID:     repository.S3Config.AccessKeyID,
			SecretAccessKey: repository.S3Config.SecretAccessKey,
			Endpoint:        repository.S3Config.Endpoint,
			Region:          repository.S3Config.Region,
		}
	}

	backupLocation.Location.Path = repository.Path
	backupLocation.ObjectMeta.Name = repository.Name
	backupLocation.Location.Type = backupType
	return backupLocation, nil
}

func isRepositoryExists(repository *executor.Repository) (bool, error) {
	bl, err := buildStorkBackupLocation(repository)
	if err != nil {
		logrus.Errorf("%v", err)
		return false, err
	}
	bucket, err := objectstore.GetBucket(bl)
	if err != nil {
		logrus.Errorf("line 373 err: %v", err)
	}
	bucket = blob.PrefixedBucket(bucket, repository.Name)
	exists, err := bucket.Exists(context.TODO(), "kopia.repository")
	if err != nil {
		logrus.Errorf("line 371 %v", err)
		return false, err
	}
	if exists {
		logrus.Infof("kopia.repository exists")
	} else {
		logrus.Infof("kopia.repository doesn't exists")
	}
	return exists, nil
}
