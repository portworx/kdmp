package nfs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/go-openapi/inflect"
	kSnapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	kSnapshotv1beta1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1beta1"
	storkvolume "github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/csi"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	"github.com/libopenstorage/stork/pkg/snapshotter"
	"github.com/libopenstorage/stork/pkg/utils"
	"github.com/libopenstorage/stork/pkg/version"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmputils "github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	resKinds map[string]string
)

const (
	metadataObjectName           = "metadata.json"
	namespacesFile               = "namespaces.json"
	crdFile                      = "crds.json"
	resourcesFile                = "resources.json"
	storageClassFile             = "storageclass.json"
	backupResourcesBatchCount    = 15
	volumeSnapShotCRDirectory    = "csi-generic"
	backupObjectUIDKeyInBackupCR = "portworx.io/backup-uid"
)

func newUploadBkpResourceCommand() *cobra.Command {
	bkpUploadCommand := &cobra.Command{
		Use:   "backup",
		Short: "Start a resource backup to nfs target",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(uploadResources(bkpNamespace, appBackupCRName, rbCrName, rbCrNamespace))
		},
	}
	bkpUploadCommand.Flags().StringVarP(&bkpNamespace, "backup-namespace", "", "", "Namespace for backup command")
	bkpUploadCommand.Flags().StringVarP(&appBackupCRName, "app-cr-name", "", "", "Namespace for applicationbackup CR whose resource to be backed up")
	bkpUploadCommand.PersistentFlags().StringVarP(&rbCrName, "rb-cr-name", "", "", "Name for resourcebackup CR to update job status")
	bkpUploadCommand.PersistentFlags().StringVarP(&rbCrNamespace, "rb-cr-namespace", "", "", "Namespace for resourcebackup CR to update job status")

	return bkpUploadCommand
}

func uploadResources(
	bkpNamespace string,
	applicationCRName string,
	rbCrName string,
	rbCrNamespace string,
) error {
	err := uploadBkpResource(bkpNamespace, applicationCRName)
	if err != nil {
		//update resourcebackup CR with status and reason
		st := kdmpapi.ResourceBackupProgressStatus{
			Status:             kdmpapi.ResourceBackupStatusFailed,
			Reason:             err.Error(),
			ProgressPercentage: 0,
		}

		err = executor.UpdateResourceBackupStatus(st, rbCrName, rbCrNamespace)
		if err != nil {
			logrus.Errorf("failed to update resorucebackup[%v/%v] status: %v", rbCrNamespace, rbCrName, err)
		}
		return err
	}
	//update resourcebackup CR with status and reason
	st := kdmpapi.ResourceBackupProgressStatus{
		Status:             kdmpapi.ResourceBackupStatusSuccessful,
		Reason:             kdmputils.ResourceUploadSuccessMsg,
		ProgressPercentage: 100,
	}
	err = executor.UpdateResourceBackupStatus(st, rbCrName, rbCrNamespace)
	if err != nil {
		logrus.Errorf("failed to update resorucebackup[%v/%v] status: %v", rbCrNamespace, rbCrName, err)
		return err
	}

	return nil
}

func uploadBkpResource(
	bkpNamespace string,
	applicationCRName string,
) error {
	funct := "uploadBkpResource"
	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		errMsg := fmt.Sprintf("%s: error parsing cloud cred: %v", funct, rErr)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	backup, err := storkops.Instance().GetApplicationBackup(applicationCRName, bkpNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error fetching applicationbackup %s: %v", funct, applicationCRName, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	logrus.Infof("backup.ObjectMeta.Name: %v, string(backup.ObjectMeta.UID %v", backup.ObjectMeta.Name, string(backup.ObjectMeta.UID))
	bkpDir := filepath.Join(repo.Path, bkpNamespace, backup.ObjectMeta.Name, string(backup.ObjectMeta.UID))
	logrus.Infof("bkpDir: %v", bkpDir)
	if err := os.MkdirAll(bkpDir, 0777); err != nil {
		errMsg := fmt.Sprintf("%s: error creating backup dir: %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	// First create the required directory
	encryptionKey, err := getEncryptionKey(bkpNamespace, backup)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}
	err = uploadResource(bkpNamespace, backup, bkpDir, encryptionKey)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading resources: %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	err = uploadNamespaces(bkpNamespace, backup, bkpDir, encryptionKey)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading namespace resource %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	err = uploadStorageClasses(bkpNamespace, backup, bkpDir, encryptionKey)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading storageclasses %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	err = uploadCSISnapshots(bkpNamespace, backup, bkpDir, encryptionKey)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading CSI snapshot file %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	csiGenericBackupDirectory := filepath.Join(repo.Path, volumeSnapShotCRDirectory)
	logrus.Infof("csi generic backup directory: %v", csiGenericBackupDirectory)
	if err := os.MkdirAll(csiGenericBackupDirectory, 0777); err != nil {
		errMsg := fmt.Sprintf("%s: error creating csi generic backup directory: %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	err = uploadCSISnapshotInfoForPVCs(bkpNamespace, backup, csiGenericBackupDirectory, encryptionKey)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading csi snapshot info for pvcs for generic backup %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	err = uploadCRDResources(resKinds, bkpDir, backup, encryptionKey)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading CRD resource %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	err = uploadMetadatResources(bkpNamespace, backup, bkpDir, encryptionKey)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading metadata resource %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil

}

func uploadResource(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
	encryptionKey string,
) error {
	funct := "uploadResource"
	rc := initResourceCollector()
	resKinds = make(map[string]string)
	objInfo := []stork_api.ObjectInfo{}
	for _, val := range backup.Status.Resources {
		objInfo = append(objInfo, val.ObjectInfo)
	}
	optionalBackupResources := []string{"Job"}
	resourceCollectorOpts := resourcecollector.Options{}

	dummyObjects := stork_api.CreateObjectsMap(objInfo)
	// If there are more number of namespaces, do it in batches
	allObjects := make([]runtime.Unstructured, 0)
	for i := 0; i < len(backup.Spec.Namespaces); i += backupResourcesBatchCount {
		batch := backup.Spec.Namespaces[i:min(i+backupResourcesBatchCount, len(backup.Spec.Namespaces))]
		objects, _, err := rc.GetResources(
			batch,
			backup.Spec.Selectors,
			nil,
			dummyObjects,
			optionalBackupResources,
			true,
			resourceCollectorOpts,
		)
		if err != nil {
			logrus.Errorf("error getting resources: %v", err)
			return err
		}

		allObjects = append(allObjects, objects...)
	}
	// For DBG remove it later
	for _, obj := range allObjects {
		metadata, err := meta.Accessor(obj)
		logrus.Infof("metadata: %+v", metadata)
		gvk := obj.GetObjectKind().GroupVersionKind()
		resKinds[gvk.Kind] = gvk.Version
		if err != nil {
			logrus.Infof("%s: %v", funct, err)
			return err
		}
	}

	// TODO: Need to create directory with UID GUID needed
	// for nfs share
	jsonBytes, err := json.MarshalIndent(allObjects, "", " ")
	if err != nil {
		logrus.Infof("%s: %v", funct, err)
		return err
	}

	err = uploadData(resourcePath, jsonBytes, resourcesFile, encryptionKey)
	if err != nil {
		logrus.Errorf("%s: %v", funct, err)
		return err
	}

	return nil
}

func uploadStorageClasses(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
	encryptionKey string,
) error {
	funct := "uploadStorageClasses"
	storageClassAdded := make(map[string]bool)
	var storageClasses []*storagev1.StorageClass
	for _, volume := range backup.Status.Volumes {
		// Get the pvc spec
		pvc, err := core.Instance().GetPersistentVolumeClaim(volume.PersistentVolumeClaim, volume.Namespace)
		if err != nil {
			return err
		}
		// Get storageclass
		sc, err := core.Instance().GetStorageClassForPVC(pvc)
		if err != nil {
			return fmt.Errorf("failed to get storage class for PVC %s: %v", pvc.Name, err)
		}
		// only add one instance of a storageclass
		if !storageClassAdded[sc.Name] {
			sc.Kind = "StorageClass"
			sc.APIVersion = "storage.k8s.io/v1"
			sc.ResourceVersion = ""
			storageClasses = append(storageClasses, sc)
			storageClassAdded[sc.Name] = true
		}

	}
	scJSONBytes, err := json.Marshal(storageClasses)
	if err != nil {
		return err
	}
	err = uploadData(resourcePath, scJSONBytes, storageClassFile, encryptionKey)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}
	return nil
}

func uploadCSISnapshots(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
	encryptionKey string,
) error {
	funct := "uploadStorageClasses"
	// snapshot.json changes
	snapshotter, err := snapshotter.NewCSIDriver()
	if err != nil {
		return err
	}

	v1SnapshotRequired, err := version.RequiresV1VolumeSnapshot()
	if err != nil {
		return err
	}
	var vsMap, vsContentMap, vsClassMap interface{}

	if v1SnapshotRequired {
		vsMap = make(map[string]*kSnapshotv1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1.VolumeSnapshotContent)
		vsClassMap = make(map[string]*kSnapshotv1.VolumeSnapshotClass)
	} else {
		vsMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
		vsClassMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)
	}

	for _, volume := range backup.Status.Volumes {
		if volume.DriverName != storkvolume.CSIDriverName {
			continue
		}
		// Get PVC we're checking the backup for
		pvc, err := core.Instance().GetPersistentVolumeClaim(volume.PersistentVolumeClaim, volume.Namespace)
		if err != nil {
			return err
		}

		// Not in cleanup state. From here on, we're checking if the PVC snapshot has finished.
		snapshotName := fmt.Sprintf("%s-%s-%s", csi.SnapshotBackupPrefix, utils.GetUIDLastSection(backup.UID), utils.GetUIDLastSection(pvc.UID))
		// getBackupSnapshotName(pvc, backup)

		snapshotInfo, err := snapshotter.SnapshotStatus(
			snapshotName,
			volume.Namespace,
		)
		if err != nil {
			logrus.Infof("sivakumar -- c.snapshotter.SnapshotStatus failed with %v", err)
			return err
		}
		if v1SnapshotRequired {
			snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1.VolumeSnapshot)
			if !ok {
				return fmt.Errorf("failed to map volumesnapshor object")
			}
			vsMap.(map[string]*kSnapshotv1.VolumeSnapshot)[volume.BackupID] = snapshot
		} else {
			snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
			if !ok {
				return fmt.Errorf("failed to map volumesnapshor object")
			}
			vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot)[volume.BackupID] = snapshot
		}
		if v1SnapshotRequired {
			snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1.VolumeSnapshotContent)
			if !ok {
				return fmt.Errorf("failed to map volumesnapshotcontent object")
			}
			vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent)[volume.BackupID] = snapshotContent
		} else {
			snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
			if !ok {
				return fmt.Errorf("failed to map volumesnapshotcontent object")
			}
			vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)[volume.BackupID] = snapshotContent
		}
		if v1SnapshotRequired {
			snapshotClass, ok := snapshotInfo.Class.(*kSnapshotv1.VolumeSnapshotClass)
			if !ok {
				return fmt.Errorf("failed to map volumesnapshotClass object")
			}
			vsClassMap.(map[string]*kSnapshotv1.VolumeSnapshotClass)[snapshotClass.Name] = snapshotClass
		} else {
			snapshotClass, ok := snapshotInfo.Class.(*kSnapshotv1beta1.VolumeSnapshotClass)
			if !ok {
				return fmt.Errorf("failed to map volumesnapshotClass object")
			}
			vsClassMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)[snapshotClass.Name] = snapshotClass
		}
	}

	var csiBackup interface{}
	v1VolumeSnapshotRequired, err := version.RequiresV1VolumeSnapshot()
	if err != nil {
		return fmt.Errorf("failed to get volumesnapshot version supported by cluster")
	}
	if v1VolumeSnapshotRequired {
		csiBackup = csi.CsiBackupObject{
			VolumeSnapshots:          vsMap.(map[string]*kSnapshotv1.VolumeSnapshot),
			VolumeSnapshotContents:   vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent),
			VolumeSnapshotClasses:    vsClassMap.(map[string]*kSnapshotv1.VolumeSnapshotClass),
			V1VolumeSnapshotRequired: true,
		}
	} else {
		csiBackup = csi.CsiBackupObject{
			VolumeSnapshots:          vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot),
			VolumeSnapshotContents:   vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent),
			VolumeSnapshotClasses:    vsClassMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotClass),
			V1VolumeSnapshotRequired: false,
		}
	}

	var csiBackupBytes []byte

	csiBackupBytes, err = json.Marshal(csiBackup)
	if err != nil {
		return err
	}
	err = uploadData(resourcePath, csiBackupBytes, csi.SnapshotObjectName, encryptionKey)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	return nil
}

func uploadCSISnapshotInfoForPVCs(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
	encryptionKey string,
) error {
	funct := "uploadCSISnapshotInfoForPVCs"
	backupUID := getAnnotationValueFromApplicationBackup(backup, backupObjectUIDKeyInBackupCR)
	for _, volInfo := range backup.Status.Volumes {
		if volInfo.DriverName == storkvolume.KDMPDriverName {
			volumeSnapshot := volInfo.VolumeSnapshot
			if len(volumeSnapshot) > 0 {
				_, volumeSnapshot := splitVolumeSnapshotInfoInVolumeInfo(volumeSnapshot)
				timestampEpoch := strconv.FormatInt(backup.GetObjectMeta().GetCreationTimestamp().Unix(), 10)
				err := uploadSnapshotObjectsForPVC(volumeSnapshot, volInfo.Namespace, getCSICRUploadDirectory(resourcePath, volInfo.PersistentVolumeClaimUID), getVSFileName(backupUID, timestampEpoch), encryptionKey)
				if err != nil {
					logrus.Errorf("%s err: %v", funct, err)
					return err
				} else {
					// If uploading of snapshot CRs is successful, cleanup the local vs/vsc CRs
					err = deleteSnapshotObjects(volumeSnapshot, volInfo.Namespace)
					if err != nil {
						logrus.Errorf("%s err: %v", funct, err)
						return err
					}
				}
			}
		}
	}
	return nil
}

func splitVolumeSnapshotInfoInVolumeInfo(volumeSnapshot string) (string, string) {
	subStrings := strings.Split(volumeSnapshot, ",")
	return subStrings[0], subStrings[1]
}

func uploadSnapshotObjectsForPVC(
	snapshotName string,
	namespace string,
	resourcePath string,
	objectName string,
	encryptionKey string,
) error {
	funct := "uploadSnapshotObjectsForPVC"
	snapshotter, err := snapshotter.NewCSIDriver()
	if err != nil {
		return err
	}
	snapshotInfo, err := snapshotter.SnapshotStatus(
		snapshotName,
		namespace,
	)
	if err != nil {
		logrus.Infof("%s getting snapshotstatus failed with %v", funct, err)
		return err
	}

	v1SnapshotRequired, err := version.RequiresV1VolumeSnapshot()
	if err != nil {
		return err
	}
	var vsMap, vsContentMap, vsClassMap interface{}
	var snapID string

	if v1SnapshotRequired {
		vsMap = make(map[string]*kSnapshotv1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1.VolumeSnapshotContent)
		vsClassMap = make(map[string]*kSnapshotv1.VolumeSnapshotClass)
	} else {
		vsMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshot)
		vsContentMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)
		vsClassMap = make(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)
	}

	if v1SnapshotRequired {
		snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1.VolumeSnapshot)
		if !ok {
			return fmt.Errorf("failed to map volumesnapshot object")
		}
		snapID = snapshot.Name
		vsMap.(map[string]*kSnapshotv1.VolumeSnapshot)[snapID] = snapshot
	} else {
		snapshot, ok := snapshotInfo.SnapshotRequest.(*kSnapshotv1beta1.VolumeSnapshot)
		if !ok {
			return fmt.Errorf("failed to map volumesnapshor object")
		}
		snapID = snapshot.Name
		vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot)[snapID] = snapshot
	}
	if v1SnapshotRequired {
		snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1.VolumeSnapshotContent)
		if !ok {
			return fmt.Errorf("failed to map volumesnapshotcontent object")
		}
		vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent)[snapID] = snapshotContent
	} else {
		snapshotContent, ok := snapshotInfo.Content.(*kSnapshotv1beta1.VolumeSnapshotContent)
		if !ok {
			return fmt.Errorf("failed to map volumesnapshotcontent object")
		}
		vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent)[snapID] = snapshotContent
	}
	if v1SnapshotRequired {
		snapshotClass, ok := snapshotInfo.Class.(*kSnapshotv1.VolumeSnapshotClass)
		if !ok {
			return fmt.Errorf("failed to map volumesnapshotClass object")
		}
		vsClassMap.(map[string]*kSnapshotv1.VolumeSnapshotClass)[snapshotClass.Name] = snapshotClass
	} else {
		snapshotClass, ok := snapshotInfo.Class.(*kSnapshotv1beta1.VolumeSnapshotClass)
		if !ok {
			return fmt.Errorf("failed to map volumesnapshotClass object")
		}
		vsClassMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotClass)[snapshotClass.Name] = snapshotClass
	}

	var csiBackup interface{}
	v1VolumeSnapshotRequired, err := version.RequiresV1VolumeSnapshot()
	if err != nil {
		return fmt.Errorf("failed to get volumesnapshot version supported by cluster")
	}
	if v1VolumeSnapshotRequired {
		csiBackup = csi.CsiBackupObject{
			VolumeSnapshots:          vsMap.(map[string]*kSnapshotv1.VolumeSnapshot),
			VolumeSnapshotContents:   vsContentMap.(map[string]*kSnapshotv1.VolumeSnapshotContent),
			VolumeSnapshotClasses:    vsClassMap.(map[string]*kSnapshotv1.VolumeSnapshotClass),
			V1VolumeSnapshotRequired: true,
		}
	} else {
		csiBackup = csi.CsiBackupObject{
			VolumeSnapshots:          vsMap.(map[string]*kSnapshotv1beta1.VolumeSnapshot),
			VolumeSnapshotContents:   vsContentMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotContent),
			VolumeSnapshotClasses:    vsClassMap.(map[string]*kSnapshotv1beta1.VolumeSnapshotClass),
			V1VolumeSnapshotRequired: false,
		}
	}

	var csiBackupBytes []byte

	csiBackupBytes, err = json.Marshal(csiBackup)
	if err != nil {
		return err
	}
	err = uploadData(resourcePath, csiBackupBytes, objectName, encryptionKey)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	return nil
}

func deleteSnapshotObjects(volumeSnapshotName, volumeSnapshotNamespace string) error {
	snapshotter, err := snapshotter.NewCSIDriver()
	if err != nil {
		return err
	}
	err = snapshotter.DeleteSnapshot(volumeSnapshotName, volumeSnapshotNamespace, true)
	msg := fmt.Sprintf("failed in removing local volume snapshot CRs for %s/%s: %v", volumeSnapshotName, volumeSnapshotNamespace, err)
	if err != nil {
		logrus.Errorf(msg)
		return fmt.Errorf(msg)
	}
	return nil
}

func getVSFileName(backupUUID, timestamp string) string {
	return fmt.Sprintf("%s-%s.json", backupUUID, timestamp)
}

func getCSICRUploadDirectory(resourcePath, pvcUID string) string {
	return filepath.Join(resourcePath, pvcUID)
}

func uploadNamespaces(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
	encryptionKey string,
) error {
	funct := "uploadNamespaces"
	var namespaces []*v1.Namespace

	for _, namespace := range backup.Spec.Namespaces {
		ns, err := core.Instance().GetNamespace(namespace)
		if err != nil {
			logrus.Errorf("%s err: %v", funct, err)
			return err
		}
		ns.ResourceVersion = ""
		namespaces = append(namespaces, ns)
	}
	jsonBytes, err := json.MarshalIndent(namespaces, "", " ")
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	err = uploadData(resourcePath, jsonBytes, namespacesFile, encryptionKey)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	return nil
}

func uploadCRDResources(
	resKinds map[string]string,
	resourcePath string,
	backup *stork_api.ApplicationBackup,
	encryptionKey string,
) error {
	funct := "uploadCRDResources"
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		return err
	}
	ruleset := inflect.NewDefaultRuleset()
	ruleset.AddPlural("quota", "quotas")
	ruleset.AddPlural("prometheus", "prometheuses")
	ruleset.AddPlural("mongodbcommunity", "mongodbcommunity")
	v1CrdAPIReqrd, err := version.RequiresV1Registration()
	if err != nil {
		return err
	}
	if v1CrdAPIReqrd {
		var crds []*apiextensionsv1.CustomResourceDefinition
		crdsGroups := make(map[string]bool)
		// First collect the group detail for the CRDs, which has CR
		for _, crd := range crdList.Items {
			for _, v := range crd.Resources {
				if _, ok := resKinds[v.Kind]; !ok {
					continue
				}
				crdsGroups[utils.GetTrimmedGroupName(v.Group)] = true
			}

		}
		// pick up all the CRDs that belongs to the group in the crdsGroups map
		for _, crd := range crdList.Items {
			for _, v := range crd.Resources {
				if _, ok := crdsGroups[utils.GetTrimmedGroupName(v.Group)]; !ok {
					continue
				}
				crdName := ruleset.Pluralize(strings.ToLower(v.Kind)) + "." + v.Group
				res, err := apiextensions.Instance().GetCRD(crdName, metav1.GetOptions{})
				if err != nil {
					if k8s_errors.IsNotFound(err) {
						continue
					}
					logrus.Errorf("Unable to get custom resource definition for %s, err: %v", v.Kind, err)
					return err
				}
				crds = append(crds, res)
			}

		}
		jsonBytes, err := json.MarshalIndent(crds, "", " ")
		if err != nil {
			logrus.Errorf("%s err: %v", funct, err)
			return err
		}

		err = uploadData(resourcePath, jsonBytes, crdFile, encryptionKey)
		if err != nil {
			logrus.Errorf("%s err: %v", funct, err)
			return err
		}
		return nil
	}
	var crds []*apiextensionsv1beta1.CustomResourceDefinition
	crdsGroups := make(map[string]bool)
	// First collect the group detail for the CRDs, which has CR
	for _, crd := range crdList.Items {
		for _, v := range crd.Resources {
			if _, ok := resKinds[v.Kind]; !ok {
				continue
			}
			crdsGroups[utils.GetTrimmedGroupName(v.Group)] = true
		}
	}
	// pick up all the CRDs that belongs to the group in the crdsGroups map
	for _, crd := range crdList.Items {
		for _, v := range crd.Resources {
			if _, ok := crdsGroups[utils.GetTrimmedGroupName(v.Group)]; !ok {
				continue
			}
			crdName := ruleset.Pluralize(strings.ToLower(v.Kind)) + "." + v.Group
			res, err := apiextensions.Instance().GetCRDV1beta1(crdName, metav1.GetOptions{})
			if err != nil {
				if k8s_errors.IsNotFound(err) {
					continue
				}
				logrus.Errorf("Unable to get customresourcedefination for %s, err: %v", v.Kind, err)
				return err
			}
			crds = append(crds, res)
		}

	}
	jsonBytes, err := json.MarshalIndent(crds, "", " ")
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}
	err = uploadData(resourcePath, jsonBytes, crdFile, encryptionKey)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}
	return nil
}

// getObjectPath construct the full base path for a given backup
// The format is "namespace/backupName/backupUID" which will be unique for each backup
func getObjectPath(
	backup *stork_api.ApplicationBackup,
) string {
	return filepath.Join(backup.Namespace, backup.Name, string(backup.UID))
}

func uploadMetadatResources(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
	encryptionKey string,
) error {
	funct := "uploadMetadatResources"
	// In the in-memory copy alone, we will update the backup status to success.
	// Update to the actual CR will be taken in the stork applicaitonbackup CR controller.
	backup.Status.BackupPath = getObjectPath(backup)
	backup.Status.Stage = stork_api.ApplicationBackupStageFinal
	backup.Status.FinishTimestamp = metav1.Now()
	backup.Status.Status = stork_api.ApplicationBackupStatusSuccessful
	backup.Status.Reason = "Volumes and resources were backed up successfully"
	// Only on success compute the total backup size
	for _, vInfo := range backup.Status.Volumes {
		backup.Status.TotalSize += vInfo.TotalSize
	}

	jsonBytes, err := json.MarshalIndent(backup, "", " ")
	if err != nil {
		return err
	}

	err = uploadData(resourcePath, jsonBytes, metadataObjectName, encryptionKey)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}
	return nil
}

func uploadData(
	resourcePath string,
	data []byte,
	resourceFileName string,
	encryptionKey string,
) error {
	var err error
	var encryptedData []byte
	funct := "uploadData"

	logrus.Infof("resourcepath needs to be created: %v", resourcePath)
	if err := os.MkdirAll(resourcePath, 0777); err != nil {
		errMsg := fmt.Sprintf("%s: error creating resourcepath: %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	filePath := filepath.Join(resourcePath, resourceFileName)
	// Encrypt data before writing with passed encryption key
	if encryptionKey != "" {
		if encryptedData, err = crypto.Encrypt(data, encryptionKey); err != nil {
			logrus.Errorf("nfs %s: encryption failed :%v, writing unencrypted data", funct, err)
			return err
		}
		data = encryptedData
	}
	//TODO: Writing with 777 permision .. Any security implication ???
	err = os.WriteFile(filePath, data, 0777)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	return nil
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func initResourceCollector() resourcecollector.ResourceCollector {
	QPS := kdmputils.DefaultQPS
	Burst := kdmputils.DefaultBurst
	kdmpData, err := core.Instance().GetConfigMap(kdmputils.KdmpConfigmapName, kdmputils.KdmpConfigmapNamespace)
	if err != nil {
		logrus.Warnf("failed reading config map %v: %v", kdmputils.KdmpConfigmapName, err)
		logrus.Warnf("default to %v for QPS ans Burst value", kdmputils.DefaultQPS)
	} else {
		QPS, err = strconv.Atoi(kdmpData.Data[kdmputils.QPSKey])
		if err != nil {
			logrus.Debugf("initResourceCollector: conversion of qps value failed, assigning default value [100] : %v", err)
			QPS = kdmputils.DefaultQPS
		}
		Burst, err = strconv.Atoi(kdmpData.Data[kdmputils.BurstKey])
		if err != nil {
			logrus.Debugf("initResourceCollector: conversion of burst value failed, assigning default value [100] : %v", err)
			Burst = kdmputils.DefaultBurst
		}
	}
	resourceCollector := resourcecollector.ResourceCollector{
		Driver: nil,
		QPS:    float32(QPS),
		Burst:  Burst,
	}

	if err := resourceCollector.Init(nil); err != nil {
		logrus.Errorf("Error initializing ResourceCollector: %v", err)
		os.Exit(1)
	}

	return resourceCollector
}

func getEncryptionKey(bkpNamespace string,
	backup *stork_api.ApplicationBackup) (string, error) {
	uploadLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, bkpNamespace)
	if err != nil {
		return "", err
	}

	if uploadLocation.Location.EncryptionV2Key == "" {
		// Give it a Best effort to obtain the key, it can be inside BL CR named secret
		logrus.Infof("Failed to get encryption detail from backuplocation CR, Attempting to obtain from k8s secret...")
		cloudCredSecret, err := core.Instance().GetSecret(uploadLocation.Name, bkpNamespace)
		if err != nil {
			return "", err
		}
		return string(cloudCredSecret.Data["encryptionKey"]), nil
	}

	return uploadLocation.Location.EncryptionV2Key, nil
}

func getAnnotationValueFromApplicationBackup(backup *stork_api.ApplicationBackup, key string) string {
	var val string
	if _, ok := backup.Annotations[key]; ok {
		val = backup.Annotations[key]
	}
	return val
}
