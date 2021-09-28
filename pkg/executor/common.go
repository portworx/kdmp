package executor

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/kubectl/pkg/cmd/util"
)

const (
	amazonS3Endpoint      = "s3.amazonaws.com"
	googleAccountFilePath = "/root/.gce_credentials"
	accessKeypath         = "/etc/cred-secret/accessKey"
	secretAccessKeyPath   = "/etc/cred-secret/secretAccessKey"
	bucketPath            = "/etc/cred-secret/path"
	endpointPath          = "/etc/cred-secret/endpoint"
	passwordPath          = "/etc/cred-secret/password"
	regionPath            = "/etc/cred-secret/region"
	disableSslPath        = "/etc/cred-secret/disablessl"
	// AccountKeyPath gce account key path
	AccountKeyPath         = "/etc/cred-secret/accountkey"
	projectIDKeypath       = "/etc/cred-secret/projectid"
	storageAccountNamePath = "/etc/cred-secret/storageaccountname"
	storageAccountKeyPath  = "/etc/cred-secret/storageaccountkey"

	// DefaultTimeout Max time a command will be retired before failing
	DefaultTimeout = 2 * time.Minute
)

// BackupTool backup tool
type BackupTool int

const (
	// ResticType for restic tool
	ResticType BackupTool = 0
	// KopiaType for kopia tool
	KopiaType BackupTool = 1
)

// S3Config specifies the config required to connect to an S3-compliant
// objectstore
type S3Config struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	// Region will be defaulted to us-east-1 if not provided
	Region     string
	DisableSSL bool
}

// AzureConfig specifies the config required to connect to Azure Blob Storage
type AzureConfig struct {
	StorageAccountName string
	StorageAccountKey  string
}

// GoogleConfig specifies the config required to connect to Google Cloud Storage
type GoogleConfig struct {
	ProjectID  string
	AccountKey string
}

// Repository contains information used to connect the repository.
type Repository struct {
	// Name is a repository name without an url address.
	Name string
	// Path is bucket name.
	Path string
	// AuthEnv is a set of environment variables used for authentication.
	AuthEnv []string
	// S3Config s3config details
	S3Config *S3Config
	// AzureConfig azure config details
	AzureConfig *AzureConfig
	// GoogleConfig goole config details
	GoogleConfig *GoogleConfig
	// Password repository password
	Password string
	// Type objectstore type
	Type storkapi.BackupLocationType
}

// Status is the current status of the command being executed
type Status struct {
	// ProgressPercentage is the progress of the command in percentage
	ProgressPercentage float64
	// TotalBytesProcessed is the no. of bytes processed
	TotalBytesProcessed uint64
	// TotalBytes is the total no. of bytes to be backed up
	TotalBytes uint64
	// SnapshotID is the snapshot ID of the backup being handled
	SnapshotID string
	// Done indicates if the operation has completed
	Done bool
	// LastKnownError is the last known error of the command
	LastKnownError error
}

// Error is the error returned by the command
type Error struct {
	// CmdOutput is the stdout received from the command
	CmdOutput string
	// CmdErr is the stderr received from the command
	CmdErr string
	// Reason is the actual reason describing the error
	Reason string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%v: Cmd Output [%v] Cmd Error [%v]", e.Reason, e.CmdOutput, e.CmdErr)
}

// ParseBackupLocation parses the provided backup location and returns the repository name
func ParseBackupLocation(repoName, name, namespace, filePath string) (*Repository, error) {
	backupLocation, err := readBackupLocation(name, namespace, filePath)
	if err != nil {
		return nil, err
	}

	switch backupLocation.Location.Type {
	case storkapi.BackupLocationS3:
		return parseS3(repoName, backupLocation.Location)
	case storkapi.BackupLocationAzure:
		return parseAzure(repoName, backupLocation.Location)
	case storkapi.BackupLocationGoogle:
		return parseGce(repoName, backupLocation.Location)
	}
	return nil, fmt.Errorf("unsupported backup location: %v", backupLocation.Location.Type)
}

func readBackupLocation(name, namespace, filePath string) (*storkapi.BackupLocation, error) {
	if name != "" {
		if namespace == "" {
			namespace = "default"
		}
		return storkops.Instance().GetBackupLocation(name, namespace)
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	out := &storkapi.BackupLocation{}
	if err = yaml.NewYAMLOrJSONDecoder(f, 1024).Decode(out); err != nil {
		return nil, err
	}

	return out, nil
}

func parseS3(repoName string, backupLocation storkapi.BackupLocationItem) (*Repository, error) {
	if backupLocation.S3Config == nil {
		return nil, fmt.Errorf("failed to parse s3 config from BackupLocation")
	}

	envs := make([]string, 0)
	envs = append(envs, fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", backupLocation.S3Config.AccessKeyID))
	envs = append(envs, fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", backupLocation.S3Config.SecretAccessKey))
	if backupLocation.S3Config.Region != "" {
		envs = append(envs, fmt.Sprintf("AWS_REGION=%s", backupLocation.S3Config.Region))
	}

	if repoName == "" {
		repoName = backupLocation.Path
	}
	return &Repository{
		Name:    repoName,
		Path:    fmt.Sprintf("s3:%s/%s", backupLocation.S3Config.Endpoint, repoName),
		AuthEnv: envs,
	}, nil
}

func parseAzure(repoName string, backupLocation storkapi.BackupLocationItem) (*Repository, error) {
	if backupLocation.AzureConfig == nil {
		return nil, fmt.Errorf("failed to parse azure config from BackupLocation")
	}
	envs := make([]string, 0)
	envs = append(envs, fmt.Sprintf("AZURE_ACCOUNT_NAME=%s", backupLocation.AzureConfig.StorageAccountName))
	envs = append(envs, fmt.Sprintf("AZURE_ACCOUNT_KEY=%s", backupLocation.AzureConfig.StorageAccountKey))

	if repoName == "" {
		repoName = backupLocation.Path
	}
	return &Repository{
		Name:    repoName,
		Path:    "azure:" + repoName + "/",
		AuthEnv: envs,
	}, nil
}

func parseGce(repoName string, backupLocation storkapi.BackupLocationItem) (*Repository, error) {
	if backupLocation.GoogleConfig == nil {
		return nil, fmt.Errorf("failed to parse google config from BackupLocation")
	}

	if err := ioutil.WriteFile(
		googleAccountFilePath,
		[]byte(backupLocation.GoogleConfig.AccountKey),
		0644,
	); err != nil {
		return nil, fmt.Errorf("failed to parse google account key: %v", err)
	}

	envs := make([]string, 0)
	envs = append(envs, fmt.Sprintf("GOOGLE_PROJECT_ID=%s", backupLocation.GoogleConfig.ProjectID))
	envs = append(envs, fmt.Sprintf("GOOGLE_APPLICATION_CREDENTIALS=%s", googleAccountFilePath))

	if repoName == "" {
		repoName = backupLocation.Path
	}
	return &Repository{
		Name:    repoName,
		Path:    "gs:" + repoName + "/",
		AuthEnv: envs,
	}, nil
}

// ParseCloudCred parsing cloud credentials
func ParseCloudCred() (*Repository, error) {
	// Read the BL type
	fPath := drivers.KopiaCredSecretMount + "/" + "type"
	blType, err := ioutil.ReadFile(fPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", fPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	logrus.Infof("type: %v", string(blType))
	repository := &Repository{}
	var rErr error
	switch storkapi.BackupLocationType(blType) {
	case storkapi.BackupLocationS3:
		repository, rErr = parseS3Creds()
	case storkapi.BackupLocationGoogle:
		repository, rErr = parseGoogleCreds()
	case storkapi.BackupLocationAzure:
		repository, rErr = parseAzureCreds()
	}
	if rErr != nil {
		return nil, rErr
	}

	password, err := ioutil.ReadFile(passwordPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", passwordPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	bucket, err := ioutil.ReadFile(bucketPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", bucketPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	repository.Path = string(bucket)
	repository.Password = string(password)

	return repository, rErr
}

func parseS3Creds() (*Repository, error) {
	repository := &Repository{
		S3Config: &S3Config{},
	}
	accessKey, err := ioutil.ReadFile(accessKeypath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", accessKeypath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	secretAccessKey, err := ioutil.ReadFile(secretAccessKeyPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", secretAccessKeyPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	endpoint, err := ioutil.ReadFile(endpointPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", endpointPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	disableSsl, err := ioutil.ReadFile(disableSslPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", disableSslPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	isSsl, err := strconv.ParseBool(string(disableSsl))
	if err != nil {
		errMsg := fmt.Sprintf("failed converting ssl value %v to bool: %s", disableSsl, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	repository.S3Config.AccessKeyID = string(accessKey)
	repository.S3Config.SecretAccessKey = string(secretAccessKey)
	repository.S3Config.Endpoint = string(endpoint)
	repository.S3Config.DisableSSL = isSsl
	repository.Type = storkapi.BackupLocationS3
	region, err := ioutil.ReadFile(regionPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", regionPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	repository.S3Config.Region = string(region)

	return repository, nil
}

func parseGoogleCreds() (*Repository, error) {
	repository := &Repository{
		GoogleConfig: &GoogleConfig{},
	}

	projectID, err := ioutil.ReadFile(projectIDKeypath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", projectIDKeypath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	accountKey, err := ioutil.ReadFile(AccountKeyPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", AccountKeyPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	repository.Type = storkapi.BackupLocationGoogle
	repository.GoogleConfig.AccountKey = string(accountKey)
	repository.GoogleConfig.ProjectID = string(projectID)

	return repository, nil
}

func parseAzureCreds() (*Repository, error) {
	repository := &Repository{
		AzureConfig: &AzureConfig{},
	}

	storageAccountName, err := ioutil.ReadFile(storageAccountNamePath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", storageAccountNamePath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	storageAccountKey, err := ioutil.ReadFile(storageAccountKeyPath)
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file %s : %s", storageAccountKeyPath, err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	repository.Type = storkapi.BackupLocationAzure
	repository.AzureConfig.StorageAccountName = string(storageAccountName)
	repository.AzureConfig.StorageAccountKey = string(storageAccountKey)

	return repository, nil
}

// WriteVolumeBackupStatus writes a backup status to the VolumeBackup crd.
func WriteVolumeBackupStatus(
	status *Status,
	volumeBackupName,
	namespace string,
) error {
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

// CreateVolumeBackup creates volumebackup CRD
func CreateVolumeBackup(name, namespace, repository, blName, blNamespace string) error {
	new := &kdmpapi.VolumeBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: kdmpapi.VolumeBackupSpec{
			Repository: repository,
			BackupLocation: kdmpapi.DataExportObjectReference{
				Name:      blName,
				Namespace: blNamespace,
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

// GetSourcePath data source path
func GetSourcePath(path, glob string) (string, error) {
	if len(path) == 0 && len(glob) == 0 {
		return "", fmt.Errorf("source-path argument is required for backups")
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

// HandleErr handle errors
func HandleErr(err error) {
	if err != nil {
		util.CheckErr(err)
	}
}
