package executor

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/yaml"
)

const (
	amazonS3Endpoint      = "s3.amazonaws.com"
	googleAccountFilePath = "/root/.gce_credentials"
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
	Region string
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

func ParseCloudCred() (*Repository, error) {
	// Read the BL type
	blType, err := ioutil.ReadFile("/tmp/cred-secret/type")
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file /tmp/type : %s", err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	logrus.Infof("type: %v", string(blType))
	switch storkapi.BackupLocationType(blType) {
	case storkapi.BackupLocationS3:
		return parseS3Creds()
	}

	out, err := exec.Command("ls", "/tmp/cred-secret").Output()
	logrus.Infof("out: %v", string(out))
	return nil, nil
	//return nil, fmt.Errorf("unsupported backup location: %v", backupLocation.Location.Type)
}

func parseS3Creds() (*Repository, error) {
	repository := &Repository{
		S3Config: &S3Config{},
	}
	accessKey, err := ioutil.ReadFile("/tmp/cred-secret/accessKey")
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file /tmp/cred-secret/accessKey : %s", err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	err = os.Setenv("AWS_ACCESS_KEY_ID ", string(accessKey))
	logrus.Infof("line 205 err: %v", err)

	secretAccessKey, err := ioutil.ReadFile("/tmp/cred-secret/secretAccessKey")
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file /tmp/cred-secret/secretAccessKey : %s", err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	err = os.Setenv("AWS_SECRET_ACCESS_KEY ", string(secretAccessKey))
	logrus.Infof("line 214 err: %v", err)

	// Set the path
	bucket, err := ioutil.ReadFile("/tmp/cred-secret/path")
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file /tmp/cred-secret/path : %s", err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}

	endpoint, err := ioutil.ReadFile("/tmp/cred-secret/endpoint")
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file /tmp/cred-secret/endpoint : %s", err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	password, err := ioutil.ReadFile("/tmp/cred-secret/password")
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file /tmp/cred-secret/password : %s", err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	repository.Password = string(password)
	repository.S3Config.AccessKeyID = string(accessKey)
	repository.S3Config.SecretAccessKey = string(secretAccessKey)
	repository.S3Config.Endpoint = string(endpoint)
	repository.Type = storkapi.BackupLocationS3
	// TODO: Add logic to backup under generic-backup folder/pvc name
	repository.Path = string(bucket)
	region, err := ioutil.ReadFile("/tmp/cred-secret/region")
	if err != nil {
		errMsg := fmt.Sprintf("failed reading data from file /tmp/cred-secret/region : %s", err)
		logrus.Errorf("%v", errMsg)
		return nil, fmt.Errorf(errMsg)
	}
	repository.S3Config.Region = string(region)
	//logrus.Infof("line 240 repository: %v", repository)
	logrus.Infof(" acces key: %v, secret: %v", os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY"))
	return repository, nil
}
