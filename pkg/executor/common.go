package executor

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"

	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
)

const (
	amazonS3Endpoint      = "s3.amazonaws.com"
	googleAccountFilePath = "/root/.gce_credentials"
)

// ParseBackupLocation parses the provided backup location and returns the repository name
func ParseBackupLocation(backupLocationName, namespace string) (string, error) {
	backupLocation, err := storkops.Instance().GetBackupLocation(backupLocationName, namespace)
	if err != nil {
		return "", err
	}

	switch backupLocation.Location.Type {
	case stork_api.BackupLocationS3:
		return parseS3(backupLocation.Location)
	case stork_api.BackupLocationAzure:
		return parseAzure(backupLocation.Location)
	case stork_api.BackupLocationGoogle:
		return parseGce(backupLocation.Location)
	}
	return "", fmt.Errorf("unsupported backup location: %v", backupLocation.Location.Type)
}

func parseS3(backupLocation stork_api.BackupLocationItem) (string, error) {
	if backupLocation.S3Config == nil {
		return "", fmt.Errorf("failed to parse s3 config from BackupLocation")
	}

	os.Setenv("AWS_ACCESS_KEY_ID", backupLocation.S3Config.AccessKeyID)
	os.Setenv("AWS_SECRET_ACCESS_KEY", backupLocation.S3Config.SecretAccessKey)
	if backupLocation.S3Config.Region != "" {
		os.Setenv("AWS_REGION", backupLocation.S3Config.Region)
	}

	bucketName := path.Join(backupLocation.S3Config.Endpoint, backupLocation.Path)
	return "s3:" + bucketName, nil
}

func parseAzure(backupLocation stork_api.BackupLocationItem) (string, error) {
	if backupLocation.AzureConfig == nil {
		return "", fmt.Errorf("failed to parse azure config from BackupLocation")
	}
	os.Setenv("AZURE_ACCOUNT_NAME", backupLocation.AzureConfig.StorageAccountName)
	os.Setenv("AZURE_ACCOUNT_KEY", backupLocation.AzureConfig.StorageAccountKey)
	return "azure:" + backupLocation.Path + "/", nil
}

func parseGce(backupLocation stork_api.BackupLocationItem) (string, error) {
	if backupLocation.GoogleConfig == nil {
		return "", fmt.Errorf("failed to parse google config from BackupLocation")
	}

	if err := ioutil.WriteFile(
		googleAccountFilePath,
		[]byte(backupLocation.GoogleConfig.AccountKey),
		0644,
	); err != nil {
		return "", fmt.Errorf("failed to parse google account key: %v", err)
	}

	os.Setenv("GOOGLE_PROJECT_ID", backupLocation.GoogleConfig.ProjectID)
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", googleAccountFilePath)

	return "gs:" + backupLocation.Path + "/", nil
}
