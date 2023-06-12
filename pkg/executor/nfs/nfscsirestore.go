package nfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	kSnapshotClient "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/executor"
	kdmpopts "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/kdmp/pkg/version"
	kdmpschedops "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/k8s/stork"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
)

const (
	kdmpAnnotationPrefix = "kdmp.portworx.com/"
	backupObjectUIDKey   = kdmpAnnotationPrefix + "backupobject-uid"
	pvcUIDKey            = kdmpAnnotationPrefix + "pvc-uid"
	csiProvider          = "csi"
)

func newCSIVolumeRestoreCommand() *cobra.Command {
	restoreCommand := &cobra.Command{
		Use:   "restore-csi-vol",
		Short: "Restore pvc from csi volumesnapshot resources",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(restoreCSIVolume(deCrName, deCrNamespace))
		},
	}
	restoreCommand.PersistentFlags().StringVarP(&deCrName, "de-name", "", "", "Name for dataexport CR")
	restoreCommand.PersistentFlags().StringVarP(&deCrNamespace, "de-namespace", "", "", "Namespace for dataexport CR")

	return restoreCommand
}

func restoreCSIVolume(
	deCrName string,
	deCrNamespace string,
) error {

	fn := "restoreCSIVolume"
	dataExport, err := kdmpschedops.Instance().GetDataExport(deCrName, deCrNamespace)
	if err != nil {
		msg := fmt.Sprintf("failed to get restore DataExport CR: %v", err)
		logrus.Errorf("%s: %v", fn, msg)
		return err
	}

	vb, err := kdmpopts.Instance().GetVolumeBackup(context.Background(),
		dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error accessing volumebackup %s in namespace %s : %v",
			dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace, err)
		logrus.Errorf("%s: %v", fn, msg)
		return fmt.Errorf(msg)
	}

	volumeBackupName := vb.Name
	// Creating PVC from snapshot

	snapshotDriverName, err := getSnapshotDriverName(dataExport)
	if err != nil {
		msg := fmt.Sprintf("failed to get snapshot driver name: %v", err)
		logrus.Errorf("%s: %v", fn, msg)
		status := &executor.Status{
			LastKnownError: fmt.Errorf(msg),
		}
		if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, deCrNamespace); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status after hitting error [%s]: %v", msg, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(msg)
	}

	snapshotter := snapshotter.NewDefaultSnapshotter()
	snapshotDriver, err := snapshotter.Driver(snapshotDriverName)
	if err != nil {
		msg := fmt.Sprintf("failed to get snapshot driver for %v: %v", snapshotDriverName, err)
		logrus.Errorf("%s: %v", fn, msg)
		status := &executor.Status{
			LastKnownError: fmt.Errorf(msg),
		}
		if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, deCrNamespace); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status after hitting error [%s]: %v", msg, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(msg)
	}

	bl, err := stork.Instance().GetBackupLocation(vb.Spec.BackupLocation.Name, vb.Spec.BackupLocation.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error while getting backuplocation %s/%s : %v",
			dataExport.Spec.Source.Namespace, dataExport.Spec.Source.Name, err)
		logrus.Errorf("%s: %v", fn, msg)
		status := &executor.Status{
			LastKnownError: fmt.Errorf(msg),
		}
		if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, deCrNamespace); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status after hitting error [%s]: %v", msg, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(msg)
	}

	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		errMsg := fmt.Sprintf("%s: error parsing cloud cred: %v", fn, rErr)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	backupUID := getAnnotationValue(dataExport, backupObjectUIDKey)
	pvcUID := getAnnotationValue(dataExport, pvcUIDKey)
	csiGenericBackupDirectory := filepath.Join(repo.Path, volumeSnapShotCRDirectory)
	status, err := snapshotDriver.RestoreFromLocalSnapshot(bl, dataExport.Status.RestorePVC, snapshotDriverName, pvcUID, backupUID, getCSICRUploadDirectory(csiGenericBackupDirectory, pvcUID), dataExport.Namespace)
	if err != nil {
		msg := fmt.Sprintf("Error while restoring from local snapshot with volumebackup %s in namespace %s : %v",
			dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace, err)
		logrus.Errorf("%s: %v", fn, msg)
		status := &executor.Status{
			LastKnownError: fmt.Errorf(msg),
		}
		if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, deCrNamespace); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status after hitting error [%s]: %v", msg, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(msg)
	}

	if !status {
		msg := fmt.Sprintf("Restoring from local snapshot with volumebackup %s in namespace %s could not be done",
			dataExport.Spec.Source.Name, dataExport.Spec.Source.Namespace)
		logrus.Errorf("%s: %v", fn, msg)
		status := &executor.Status{
			LastKnownError: fmt.Errorf(msg),
		}
		if err = executor.WriteVolumeBackupStatus(status, volumeBackupName, deCrNamespace); err != nil {
			errMsg := fmt.Sprintf("failed to write a VolumeBackup status after hitting error [%s]: %v", msg, err)
			logrus.Errorf("%v", errMsg)
			return fmt.Errorf(errMsg)
		}
		return fmt.Errorf(msg)
	}
	return nil
}

func getSnapshotDriverName(dataExport *kdmpapi.DataExport) (string, error) {
	if len(dataExport.Spec.SnapshotStorageClass) == 0 {
		return "", fmt.Errorf("snapshot storage class not provided")
	}
	if dataExport.Spec.SnapshotStorageClass == "default" ||
		dataExport.Spec.SnapshotStorageClass == "Default" {
		return csiProvider, nil
	}
	// Check if snapshot class is a CSI snapshot class
	config, err := rest.InClusterConfig()
	if err != nil {
		return "", err
	}

	cs, err := kSnapshotClient.NewForConfig(config)
	if err != nil {
		return "", err
	}
	v1SnapshotRequired, err := version.RequiresV1VolumeSnapshot()
	if err != nil {
		return "", err
	}
	if v1SnapshotRequired {
		_, err = cs.SnapshotV1().VolumeSnapshotClasses().Get(context.TODO(), dataExport.Spec.SnapshotStorageClass, metav1.GetOptions{})
	} else {
		_, err = cs.SnapshotV1beta1().VolumeSnapshotClasses().Get(context.TODO(), dataExport.Spec.SnapshotStorageClass, metav1.GetOptions{})
	}
	if err == nil {
		return csiProvider, nil
	}
	if err != nil && !k8sErrors.IsNotFound(err) {
		return "", nil
	}
	return "", fmt.Errorf("did not find any supported snapshot driver for snapshot storage class %s", dataExport.Spec.SnapshotStorageClass)
}

// CreateCredentialsSecret parses the provided backup location and creates secret with cloud credentials
func CreateCredentialsSecret(secretName, blName, blNamespace, namespace string, labels map[string]string) error {
	backupLocation, err := readBackupLocation(blName, blNamespace, "")
	if err != nil {
		return err
	}

	// TODO: Add for other cloud providers
	// Creating cloud cred secret
	switch backupLocation.Location.Type {
	case storkapi.BackupLocationS3:
		return createS3Secret(secretName, backupLocation, namespace, labels)
	case storkapi.BackupLocationGoogle:
		return createGoogleSecret(secretName, backupLocation, namespace, labels)
	case storkapi.BackupLocationAzure:
		return createAzureSecret(secretName, backupLocation, namespace, labels)
	case storkapi.BackupLocationNFS:
		return utils.CreateNfsSecret(secretName, backupLocation, namespace, labels)
	}

	return fmt.Errorf("unsupported backup location: %v", backupLocation.Location.Type)
}

func readBackupLocation(name, namespace, filePath string) (*storkapi.BackupLocation, error) {
	if name != "" {
		if namespace == "" {
			namespace = "default"
		}
		return stork.Instance().GetBackupLocation(name, namespace)
	}

	// TODO: This is needed for restic, we can think of removing it later
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

func createS3Secret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["endpoint"] = []byte(backupLocation.Location.S3Config.Endpoint)
	credentialData["accessKey"] = []byte(backupLocation.Location.S3Config.AccessKeyID)
	credentialData["secretAccessKey"] = []byte(backupLocation.Location.S3Config.SecretAccessKey)
	credentialData["region"] = []byte(backupLocation.Location.S3Config.Region)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["disablessl"] = []byte(strconv.FormatBool(backupLocation.Location.S3Config.DisableSSL))
	err := utils.CreateJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createGoogleSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["accountkey"] = []byte(backupLocation.Location.GoogleConfig.AccountKey)
	credentialData["projectid"] = []byte(backupLocation.Location.GoogleConfig.ProjectID)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	err := utils.CreateJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func createAzureSecret(secretName string, backupLocation *storkapi.BackupLocation, namespace string, labels map[string]string) error {
	credentialData := make(map[string][]byte)
	credentialData["type"] = []byte(backupLocation.Location.Type)
	credentialData["password"] = []byte(backupLocation.Location.RepositoryPassword)
	credentialData["path"] = []byte(backupLocation.Location.Path)
	credentialData["storageaccountname"] = []byte(backupLocation.Location.AzureConfig.StorageAccountName)
	credentialData["storageaccountkey"] = []byte(backupLocation.Location.AzureConfig.StorageAccountKey)
	err := utils.CreateJobSecret(secretName, namespace, credentialData, labels)

	return err
}

func getAnnotationValue(de *kdmpapi.DataExport, key string) string {
	var val string
	if _, ok := de.Annotations[key]; ok {
		val = de.Annotations[key]
	}
	return val
}
