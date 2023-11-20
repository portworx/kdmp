package kopia

import (
	"context"
	"fmt"
	"os"
	"time"

	storkv1 "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gocloud.dev/blob"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	progressCheckInterval = 5 * time.Second
	genericBackupDir      = "generic-backup"
	kopiaRepositoryFile   = "kopia.repository"
	annualSnapshots       = "2147483647"
	monthlySnapshots      = "2147483647"
	weeklySnapshots       = "2147483647"
	dailySnapshots        = "2147483647"
	hourlySnapshots       = "2147483647"
	latestSnapshots       = "2147483647"
)

var (
	bkpNamespace    string
	compression     string
	excludeFileList string
)

var (
	kopiaProviderType = map[storkv1.BackupLocationType]string{
		storkv1.BackupLocationS3:     "s3",
		storkv1.BackupLocationGoogle: "gcs",
		storkv1.BackupLocationAzure:  "azure",
		storkv1.BackupLocationNFS:    "filesystem",
	}
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
			srcPath, err := executor.GetSourcePath(sourcePath, sourcePathGlob)
			if err != nil {
				util.CheckErr(err)
				return
			}

			executor.HandleErr(runBackup(srcPath))
		},
	}
	backupCommand.Flags().StringVarP(&bkpNamespace, "backup-namespace", "n", "", "Namespace for backup command")
	backupCommand.Flags().StringVar(&sourcePath, "source-path", "", "Source for kopia backup")
	backupCommand.Flags().StringVar(&sourcePathGlob, "source-path-glob", "", "The regexp should match only one path that will be used for backup")
	backupCommand.Flags().StringVar(&compression, "compression", "", "Compression type to be used")
	backupCommand.Flags().StringVar(&excludeFileList, "exclude-file-list", "", " list of dir names that need to be exclude in the kopia snapshot")

	return backupCommand
}

func runBackup(sourcePath string) error {
	// Parse using the mounted secrets
	fn := "runBackup"
	repo, rErr := executor.ParseCloudCred()
	var repoName string
	if repo == nil {
		// A case wherein repo was nil, we want VB CR with respective failed msg
		// hence having a empty repo name
		repoName = ""
	} else {
		repoName = frameBackupPath()
		repo.Name = repoName
	}
	if volumeBackupName != "" {
		if err := executor.CreateVolumeBackup(
			volumeBackupName,
			bkpNamespace,
			repoName,
			backupLocationName,
			backupLocationNamespace,
		); err != nil {
			logrus.Errorf("%s: %v", fn, err)
			return err
		}
	}
	if rErr != nil {
		if statusErr := executor.WriteVolumeBackupStatus(
			&executor.Status{LastKnownError: rErr},
			volumeBackupName,
			bkpNamespace,
		); statusErr != nil {
			return statusErr
		}
		return fmt.Errorf("parse backuplocation: %s", rErr)
	}
	// kopia doesn't have a way to know if repository is already initialized.
	// Repository create needs to run only first time.
	// Check if kopia.repository exists
	// TODO : We need to check for kopia.repository file. For now we are
	// skipping the check and assuming the repo doesn't exist specifically for NFS.
	// Read the BL type
	fPath := drivers.KopiaCredSecretMount + "/" + "type"
	blType, err := os.ReadFile(fPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", fPath, err)
		logrus.Errorf("%v", errMsg)
		return fmt.Errorf(errMsg)
	}
	var exists = false
	if storkv1.BackupLocationType(blType) != storkv1.BackupLocationNFS {
		exists, err = isRepositoryExists(repo)
		if err != nil {
			errMsg := fmt.Sprintf("repository exists check for repo %s failed: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf("%s: %v", errMsg, err)
		}
	}
	if !exists {
		if err = runKopiaCreateRepo(repo); err != nil {
			errMsg := fmt.Sprintf("repository %s creation failed", repo.Name)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf("%s: %v", errMsg, err)
		}

		if err = setGlobalPolicy(); err != nil {
			errMsg := fmt.Sprintf("setting global policy for repository %s failed: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	if err = runKopiaRepositoryConnect(repo); err != nil {
		status := &executor.Status{
			LastKnownError: err,
		}
		if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, bkpNamespace); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		errMsg := fmt.Sprintf("connecting to repository %s failed: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	// if compression is not set in config map, it means no need of enabling compression
	if compression != "" {
		if err = runKopiaCompression(repo, sourcePath); err != nil {
			errMsg := fmt.Sprintf("compression failed for path %s: %v", sourcePath, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	// if excludeFileList is not set in config map, it means no need to exclude any dir in the snapshot.
	if excludeFileList != "" {
		if err = runKopiaExcludeFileList(repo, sourcePath); err != nil {
			errMsg := fmt.Sprintf("setting exclude file list failed for path %s: %v", sourcePath, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
	}

	if err = runKopiaBackup(repo, sourcePath); err != nil {
		errMsg := fmt.Sprintf("backup failed for repository %s: %v", repo.Name, err)
		logrus.Errorf("%s: %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	return nil
}

func populateS3AccessDetails(initCmd *kopia.Command, repository *executor.Repository) *kopia.Command {
	// kopia is not honouring env variabels set in the pod so passing them as flags
	initCmd.AddArg("--endpoint")
	initCmd.AddArg(repository.S3Config.Endpoint)
	initCmd.AddArg("--access-key")
	initCmd.AddArg(repository.S3Config.AccessKeyID)
	initCmd.AddArg("--secret-access-key")
	initCmd.AddArg(repository.S3Config.SecretAccessKey)
	// At present the backuplocation CR was set with "AES256" value for SSE-S3.
	// So need to do this conversion.
	switch repository.S3Config.SseType {
	case "AES256":
		initCmd.AddArg("--sseType")
		initCmd.AddArg("SSE-S3")
	}

	return initCmd
}

func populateGCEAccessDetails(initCmd *kopia.Command, repository *executor.Repository) *kopia.Command {
	initCmd.AddArg("--credentials-file")
	initCmd.AddArg(executor.AccountKeyPath)

	return initCmd
}

func populateAzureccessDetails(initCmd *kopia.Command, repository *executor.Repository) *kopia.Command {
	initCmd.AddArg("--container")
	initCmd.AddArg(repository.Path)
	initCmd.AddArg("--storage-account")
	initCmd.AddArg(repository.AzureConfig.StorageAccountName)
	initCmd.AddArg("--storage-key")
	initCmd.AddArg(repository.AzureConfig.StorageAccountKey)

	return initCmd
}

func runKopiaCreateRepo(repository *executor.Repository) error {
	var err error
	var repoCreateCmd *kopia.Command

	logrus.Infof("Repository creation started")
	switch repository.Type {
	case storkv1.BackupLocationS3:
		repoCreateCmd, err = kopia.GetCreateCommand(
			repository.Path,
			repository.Name,
			repository.Password,
			kopiaProviderType[repository.Type],
			repository.S3Config.Region,
			repository.S3Config.DisableSSL,
		)
	default:
		repoCreateCmd, err = kopia.GetCreateCommand(
			repository.Path,
			repository.Name,
			repository.Password,
			kopiaProviderType[repository.Type],
			"",
			false,
		)
	}
	if err != nil {
		return err
	}
	// NFS doesn't need any special treatment for repo create command
	// hence no case exist for it.
	switch repository.Type {
	case storkv1.BackupLocationS3:
		repoCreateCmd = populateS3AccessDetails(repoCreateCmd, repository)
	case storkv1.BackupLocationGoogle:
		repoCreateCmd = populateGCEAccessDetails(repoCreateCmd, repository)
	case storkv1.BackupLocationAzure:
		repoCreateCmd = populateAzureccessDetails(repoCreateCmd, repository)
	}

	initExecutor := kopia.NewCreateExecutor(repoCreateCmd)
	if err := initExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run repository create command: %v", err)
		return err
	}

	t := func() (interface{}, bool, error) {
		status, err := initExecutor.Status()
		if err != nil {
			return "", true, err
		}
		if status.LastKnownError != nil {
			if status.LastKnownError != kopia.ErrAlreadyRepoExist {
				if err = executor.WriteVolumeBackupStatus(
					status,
					volumeBackupName,
					bkpNamespace,
				); err != nil {
					errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
					logrus.Errorf("%v", errMsg)
					return "", true, fmt.Errorf(errMsg)
				}
				return "", true, status.LastKnownError
			}
			status.LastKnownError = nil
		}

		if err = executor.WriteVolumeBackupStatus(
			status,
			volumeBackupName,
			bkpNamespace,
		); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
			logrus.Errorf("%v", errMsg)
			return "", true, fmt.Errorf(errMsg)
		}
		if status.Done {
			return "", false, nil
		}

		return "", true, fmt.Errorf("repo create status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		logrus.Errorf("repository %s creation failed: %v", repository.Name, err)
		return err
	}
	logrus.Infof("Repository creation successful")

	return nil
}

func runKopiaBackup(repository *executor.Repository, sourcePath string) error {
	logrus.Infof("Backup started")
	backupCmd, err := kopia.GetBackupCommand(
		repository.Path,
		repository.Name,
		repository.Password,
		string(repository.Type),
		sourcePath,
	)
	if err != nil {
		return err
	}
	// This is needed to handle case where after kopia repo create was successful and
	// the pod got terminated. Now user triggers another backup, so we need to pass
	// credentials for "snapshot create".
	backupExecutor := kopia.NewBackupExecutor(backupCmd)
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
		if err = executor.WriteVolumeBackupStatus(
			status,
			volumeBackupName,
			bkpNamespace,
		); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
			logrus.Errorf("%v", errMsg)
			continue
		}
		if status.LastKnownError != nil {
			return status.LastKnownError
		}
		if status.Done {
			break
		}

	}
	logrus.Infof("Backup successful")
	return nil
}

func runKopiaRepositoryConnect(repository *executor.Repository) error {
	var err error
	var connectCmd *kopia.Command
	logrus.Infof("Repository connect started")
	switch repository.Type {
	case storkv1.BackupLocationS3:
		connectCmd, err = kopia.GetConnectCommand(
			repository.Path,
			repository.Name,
			repository.Password,
			kopiaProviderType[repository.Type],
			repository.S3Config.Region,
			repository.S3Config.DisableSSL,
		)
	default:
		connectCmd, err = kopia.GetConnectCommand(
			repository.Path,
			repository.Name,
			repository.Password,
			kopiaProviderType[repository.Type],
			"",
			false,
		)
	}
	if err != nil {
		return err
	}

	switch repository.Type {
	case storkv1.BackupLocationS3:
		connectCmd = populateS3AccessDetails(connectCmd, repository)
	case storkv1.BackupLocationGoogle:
		connectCmd = populateGCEAccessDetails(connectCmd, repository)
	case storkv1.BackupLocationAzure:
		connectCmd = populateAzureccessDetails(connectCmd, repository)
	}
	connectExecutor := kopia.NewConnectExecutor(connectCmd)
	if err := connectExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run repository connect  command: %v", err)
		return err
	}

	t := func() (interface{}, bool, error) {
		status, err := connectExecutor.Status()
		if err != nil {
			return "", true, err
		}
		if status.LastKnownError != nil {
			return "", true, status.LastKnownError
		}
		if status.Done {
			return "", false, nil
		}

		return "", true, fmt.Errorf("repository connect status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		logrus.Errorf("failed connecting to repository %s: %v", repository.Name, err)
		return err
	}
	logrus.Infof("kopia repo connect successful ..")

	return nil
}

func setGlobalPolicy() error {
	logrus.Infof("Setting global policy")
	policyCmd, err := kopia.SetGlobalPolicyCommand()
	if err != nil {
		return err
	}
	// As we don't want kopia maintenance to kick in and trigger global policy on default values
	// for the repository, setting them to very high values
	policyCmd = addPolicySetting(policyCmd)
	policyExecutor := kopia.NewSetGlobalPolicyExecutor(policyCmd)
	if err := policyExecutor.Run(); err != nil {
		errMsg := fmt.Sprintf("failed to run setting global policy command: %v", err)
		logrus.Errorf("%v", errMsg)
		return fmt.Errorf(errMsg)
	}

	t := func() (interface{}, bool, error) {
		status, err := policyExecutor.Status()
		if err != nil {
			return "", false, err
		}
		if status.LastKnownError != nil {
			if err = executor.WriteVolumeBackupStatus(
				status,
				volumeBackupName,
				bkpNamespace,
			); err != nil {
				errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
				logrus.Errorf("%v", errMsg)
				return "", true, fmt.Errorf(errMsg)
			}
			return "", true, status.LastKnownError
		}

		if status.Done {
			return "", false, nil
		}

		return "", true, fmt.Errorf("global policy command status not available")
	}

	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		logrus.Errorf("failed setting global policy for repository")
		return err
	}
	logrus.Infof("Global policy set successfully")

	return nil
}

func runKopiaExcludeFileList(repository *executor.Repository, sourcePath string) error {
	logrus.Infof("setting exclude file list for the snapshot")
	excludeFileListCmd, err := kopia.GetExcludeFileListCommand(
		sourcePath,
		excludeFileList,
	)
	if err != nil {
		return err
	}
	excludeFileListExecutor := kopia.NewExcludeFileListExecutor(excludeFileListCmd)
	if err := excludeFileListExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run exclude file list command: %v", err)
		return err
	}
	t := func() (interface{}, bool, error) {
		status, err := excludeFileListExecutor.Status()
		if err != nil {
			return "", true, err
		}
		if status.LastKnownError != nil {
			if err = executor.WriteVolumeBackupStatus(
				status,
				volumeBackupName,
				bkpNamespace,
			); err != nil {
				errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
				logrus.Errorf("%v", errMsg)
				return "", true, fmt.Errorf(errMsg)
			}
			return "", true, status.LastKnownError
		}
		if status.Done {
			return "", false, nil
		}

		return "", true, fmt.Errorf("setting exclude file list for snapshot command status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		logrus.Errorf("failed setting snapshot exclude file list for path %v: %v", sourcePath, err)
		return err
	}

	logrus.Infof("setting exclude file list is  successfully")
	return nil
}

func runKopiaCompression(repository *executor.Repository, sourcePath string) error {
	logrus.Infof("Compression started")
	compressionCmd, err := kopia.GetCompressionCommand(
		sourcePath,
		compression,
	)
	if err != nil {
		return err
	}
	compressionExecutor := kopia.NewCompressionExecutor(compressionCmd)
	if err := compressionExecutor.Run(); err != nil {
		err = fmt.Errorf("failed to run compression command: %v", err)
		return err
	}
	t := func() (interface{}, bool, error) {
		status, err := compressionExecutor.Status()
		if err != nil {
			return "", true, err
		}
		if status.LastKnownError != nil {
			if err = executor.WriteVolumeBackupStatus(
				status,
				volumeBackupName,
				bkpNamespace,
			); err != nil {
				errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
				logrus.Errorf("%v", errMsg)
				return "", true, fmt.Errorf(errMsg)
			}
			return "", true, status.LastKnownError
		}
		if status.Done {
			return "", false, nil
		}

		return "", true, fmt.Errorf("enabling compression command status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		logrus.Errorf("failed setting compression for path %v: %v", sourcePath, err)
		return err
	}

	logrus.Infof("compression set successfully")
	return nil
}

// Under backuplocation path, following path would be created
// <bucket>/generic-backup/<ns - pvc>
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
			DisableSSL:      repository.S3Config.DisableSSL,
		}
	case storkv1.BackupLocationGoogle:
		backupType = storkv1.BackupLocationGoogle
		backupLocation.Location.GoogleConfig = &storkv1.GoogleConfig{
			ProjectID:  repository.GoogleConfig.ProjectID,
			AccountKey: repository.GoogleConfig.AccountKey,
		}
	case storkv1.BackupLocationAzure:
		backupType = storkv1.BackupLocationAzure
		backupLocation.Location.AzureConfig = &storkv1.AzureConfig{
			StorageAccountName: repository.AzureConfig.StorageAccountName,
			StorageAccountKey:  repository.AzureConfig.StorageAccountKey,
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
	exists := false
	t := func() (interface{}, bool, error) {
		bucket, err := objectstore.GetBucket(bl)
		if err != nil {
			status := &executor.Status{
				LastKnownError: err,
			}
			if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, bkpNamespace); err != nil {
				errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
				logrus.Errorf("%v", errMsg)
				return "", true, fmt.Errorf(errMsg)
			}
			return "", true, fmt.Errorf("repo check status not available")
		}

		bucket = blob.PrefixedBucket(bucket, repository.Name)
		exists, err = bucket.Exists(context.TODO(), kopiaRepositoryFile)
		if err != nil {
			status := &executor.Status{
				LastKnownError: err,
			}
			if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, bkpNamespace); err != nil {
				errMsg := fmt.Sprintf("failed to write a VolumeBackup status: %v", err)
				logrus.Errorf("%v", errMsg)
				return "", true, fmt.Errorf(errMsg)
			}
			return "", true, fmt.Errorf("repo check status not available")
		}
		if exists {
			logrus.Infof("%s exists", kopiaRepositoryFile)
		} else {
			logrus.Infof("%s doesn't exists", kopiaRepositoryFile)
		}

		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		logrus.Errorf("repository %s exists check failed: %v", repository.Name, err)
		return exists, err
	}

	return exists, nil
}

func addPolicySetting(policyCmd *kopia.Command) *kopia.Command {
	policyCmd.AddArg("--keep-latest")
	policyCmd.AddArg(latestSnapshots)
	policyCmd.AddArg("--keep-hourly")
	policyCmd.AddArg(hourlySnapshots)
	policyCmd.AddArg("--keep-daily")
	policyCmd.AddArg(dailySnapshots)
	policyCmd.AddArg("--keep-weekly")
	policyCmd.AddArg(weeklySnapshots)
	policyCmd.AddArg("--keep-monthly")
	policyCmd.AddArg(monthlySnapshots)
	policyCmd.AddArg("--keep-annual")
	policyCmd.AddArg(annualSnapshots)

	return policyCmd
}
