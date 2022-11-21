package nfs

import (
	"fmt"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/sched-ops/k8s/core"
	kdmpschedops "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/k8s/storage"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/portworx/sched-ops/task"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	k8shelper "k8s.io/component-helpers/storage/volume"
)

const (
	bindCompletedKey      = "pv.kubernetes.io/bind-completed"
	boundByControllerKey  = "pv.kubernetes.io/bound-by-controller"
	storageClassKey       = "volume.beta.kubernetes.io/storage-class"
	storageProvisioner    = "volume.beta.kubernetes.io/storage-provisioner"
	storageNodeAnnotation = "volume.kubernetes.io/selected-node"
	// KdmpAnnotation annotation for created by
	KdmpAnnotation = "stork.libopenstorage.org/created-by"
	// StorkAnnotation controller annotation
	StorkAnnotation       = "stork.libopenstorage.org/kdmp"
	defaultTimeout        = 1 * time.Minute
	progressCheckInterval = 5 * time.Second
	volumeinitialDelay    = 2 * time.Second
	volumeFactor          = 1.5
	volumeSteps           = 15
)

var volumeAPICallBackoff = wait.Backoff{
	Duration: volumeinitialDelay,
	Factor:   volumeFactor,
	Steps:    volumeSteps,
}

func newRestoreVolumeCommand() *cobra.Command {
	restoreCommand := &cobra.Command{
		Use:   "restore-vol",
		Short: "Download vol resource and create pvc",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(restoreVolResourcesAndApply(appRestoreCRName, restoreNamespace, rbCrName, rbCrNamespace))
		},
	}
		restoreCommand.PersistentFlags().StringVarP(&restoreNamespace, "restore-namespace", "", "", "Namespace for restore CR")
		restoreCommand.PersistentFlags().StringVarP(&appRestoreCRName, "app-cr-name", "", "", "application restore CR name")
		restoreCommand.PersistentFlags().StringVarP(&rbCrName, "rb-cr-name", "", "", "Name for resourcebackup CR to update job status")
		restoreCommand.PersistentFlags().StringVarP(&rbCrNamespace, "rb-cr-namespace", "", "", "Namespace for resourcebackup CR to update job status")

	return restoreCommand
}

func convertAppBkpVolInfoToResourceVolInfo(
	volInfo []*storkapi.ApplicationBackupVolumeInfo,
) (resVolInfo []*kdmpapi.ResourceBackupVolumeInfo) {
	restoreVolumeInfos := make([]*kdmpapi.ResourceBackupVolumeInfo, 0)
	for _, vol := range volInfo {
		resInfo := &kdmpapi.ResourceBackupVolumeInfo{}

		resInfo.PersistentVolumeClaim = vol.PersistentVolumeClaim
		resInfo.PersistentVolumeClaimUID = vol.PersistentVolumeClaimUID
		resInfo.Namespace = vol.Namespace
		resInfo.Volume = vol.Volume
		resInfo.BackupID = vol.BackupID
		resInfo.DriverName = vol.DriverName
		resInfo.Status = kdmpapi.ResourceBackupStatus(vol.Status)
		resInfo.Zones = vol.Zones
		resInfo.Reason = "Restore in progress"
		resInfo.Options = vol.Options
		resInfo.TotalSize = vol.TotalSize
		resInfo.ActualSize = vol.ActualSize
		resInfo.StorageClass = vol.StorageClass
		resInfo.Provisioner = vol.Provisioner
		resInfo.VolumeSnapshot = vol.VolumeSnapshot
		restoreVolumeInfos = append(restoreVolumeInfos, resInfo)
	}

	return restoreVolumeInfos
}

func convertAppRestoreVolInfoToResourceVolInfo(
	volInfo []*storkapi.ApplicationRestoreVolumeInfo,
) (resVolInfo []*kdmpapi.ResourceRestoreVolumeInfo) {
	restoreVolumeInfos := make([]*kdmpapi.ResourceRestoreVolumeInfo, 0)
	for _, vol := range volInfo {
		//restoreVolInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		resInfo := &kdmpapi.ResourceRestoreVolumeInfo{}

		resInfo.PersistentVolumeClaim = vol.PersistentVolumeClaim
		resInfo.PersistentVolumeClaimUID = vol.PersistentVolumeClaimUID
		resInfo.DriverName = vol.DriverName
		resInfo.Status = kdmpapi.ResourceBackupStatus(vol.Status)
		resInfo.Zones = vol.Zones
		resInfo.Reason = "Restore in progress"
		resInfo.Options = vol.Options
		resInfo.TotalSize = vol.TotalSize
		resInfo.SourceVolume = vol.SourceVolume
		resInfo.SourceNamespace = vol.SourceNamespace
		resInfo.RestoreVolume = vol.RestoreVolume
		restoreVolumeInfos = append(restoreVolumeInfos, resInfo)

	}

	return restoreVolumeInfos
}

func restoreVolResourcesAndApply(
	applicationCRName string,
	restoreNamespace string,
	rbCrName string,
	rbCrNamespace string,
) error {
	funct := "restoreVolResourcesAndApply"
	dynamicInterface, err := getDynamicInterface()
	if err != nil {
		return err
	}
	err = executor.CreateNamespacesFromMapping(applicationCRName, restoreNamespace)
	if err != nil {
		//update resourcebackup CR with status and reason
		logrus.Errorf("restore resources for [%v/%v] failed with error: %v", rbCrNamespace, rbCrName, err.Error())
		st := kdmpapi.ResourceBackupProgressStatus{
			Status:             kdmpapi.ResourceBackupStatusFailed,
			Reason:             err.Error(),
			ProgressPercentage: 0,
		}

		err = executor.UpdateResourceBackupStatus(st, rbCrName, rbCrNamespace)
		if err != nil {
			logrus.Errorf("failed to update resorucebackup[%v/%v] status after hitting error in create namespace : %v", rbCrNamespace, rbCrName, err)
		}
		return err
	}

	restore, err := storkops.Instance().GetApplicationRestore(applicationCRName, restoreNamespace)
	if err != nil {
		logrus.Errorf("Error getting restore cr: %v", err)
		return err
	}

	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup: %v", err)
		return err
	}
	// Iterate over all the vol info from the backup spec

	objects, err := downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error downloading resources: %v", err)
		return err
	}
	driver, err := volume.Get("kdmp")
	if err != nil {
		return err
	}
	existingRestoreVolInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	info := storkapi.ObjectInfo{
		GroupVersionKind: metav1.GroupVersionKind{
			Group:   "core",
			Version: "v1",
			Kind:    "PersistentVolumeClaim",
		},
	}
	objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
	pvcCount := 0
	restoreDone := 0
	driverName := "kdmp"
	backupVolumeInfoMappings := make(map[string][]*storkapi.ApplicationBackupVolumeInfo)
	for _, namespace := range backup.Spec.Namespaces {
		if _, ok := restore.Spec.NamespaceMapping[namespace]; !ok {
			continue
		}
		for _, volumeBackup := range backup.Status.Volumes {
			if volumeBackup.Namespace != namespace {
				continue
			}
			// If a list of resources was specified during restore check if
			// this PVC was included
			info.Name = volumeBackup.PersistentVolumeClaim
			info.Namespace = volumeBackup.Namespace
			if len(objectMap) != 0 {
				if val, present := objectMap[info]; !present || !val {
					continue
				}
			}

			pvcCount++
			isVolRestoreDone := false
			for _, statusVolume := range restore.Status.Volumes {
				if statusVolume.SourceVolume == volumeBackup.Volume {
					isVolRestoreDone = true
					break
				}
			}
			if isVolRestoreDone {
				restoreDone++
				continue
			}

			if backupVolumeInfoMappings[driverName] == nil {
				backupVolumeInfoMappings[driverName] = make([]*storkapi.ApplicationBackupVolumeInfo, 0)
			}
			backupVolumeInfoMappings[driverName] = append(backupVolumeInfoMappings[driverName], volumeBackup)
		}
	}

	rc := initResourceCollector()
	// Iterate over bkp vol info and create the pvc
	resourceBackupVolInfos := make([]*kdmpapi.ResourceBackupVolumeInfo, 0)
	existingResourceBackupVolInfos := make([]*kdmpapi.ResourceRestoreVolumeInfo, 0)

	for _, bkpvInfo := range backupVolumeInfoMappings {
		backupVolInfos := bkpvInfo
		// Skip pv/pvc if replacepolicy is set to retain to avoid creating
		if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyRetain {
			backupVolInfos, existingRestoreVolInfos, err = skipVolumesFromRestoreList(restore, objects, driver, backup.Status.Volumes)
			if err != nil {
				log.ApplicationRestoreLog(restore).Errorf("Error while checking pvcs: %v", err)
				return err
			}
			logrus.Tracef("backupVolInfos: %+v", backupVolInfos)
			logrus.Tracef("existingRestoreVolInfos: %+v", existingRestoreVolInfos)
		}

		// Convert ApplicationRestoreVolumeInfo to ResourceRestoreVolumeInfo
		resourceBackupVolInfos = convertAppBkpVolInfoToResourceVolInfo(backupVolInfos)
		existingResourceBackupVolInfos = convertAppRestoreVolInfoToResourceVolInfo(existingRestoreVolInfos)
		preRestoreObjects, err := GetPreRestoreResources(backup, restore, objects)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("Error getting PreRestore Resources: %v", err)
			return err
		}

		if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
			objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
			objectBasedOnIncludeResources := make([]runtime.Unstructured, 0)
			for _, o := range objects {
				skip, err := rc.PrepareResourceForApply(
					o,
					objects,
					objectMap,
					restore.Spec.NamespaceMapping,
					nil, // no need to set storage class mappings at this stage
					nil,
					restore.Spec.IncludeOptionalResourceTypes,
					nil,
				)
				if err != nil {
					return err
				}
				if !skip {
					objectBasedOnIncludeResources = append(
						objectBasedOnIncludeResources,
						o,
					)
				}
			}
			tempObjects, err := getNamespacedObjectsToDelete(
				restore,
				objectBasedOnIncludeResources,
			)
			if err != nil {
				return err
			}

			err = rc.DeleteResources(
				dynamicInterface,
				tempObjects)
			if err != nil {
				return err
			}
		}
		for _, vInfo := range bkpvInfo {
			restoreNamespace, ok := restore.Spec.NamespaceMapping[vInfo.Namespace]
			if !ok {
				return fmt.Errorf("restore namespace mapping not found: %s", vInfo.Namespace)

			}
			pvc, err := volume.GetPVCFromObjects(preRestoreObjects, vInfo)
			if err != nil {
				return err
			}
			pvc.Namespace = restoreNamespace
			// TODO:
			// Also need to add zone supports for cloud provider (EBS)
			// We have to make sure restore pod comes up on the same zone where PVC is
			// provisioned
			logrus.Tracef("content of pvc being created: %+v", pvc)
			logrus.Infof("creating pvc [%v/%v]", pvc.Namespace, pvc.Name)
			_, err = core.Instance().CreatePersistentVolumeClaim(pvc)
			if err != nil {
				if !k8s_errors.IsAlreadyExists(err) {
					return fmt.Errorf("failed to create PVC %s: %s", pvc.Name, err.Error())
				}
				logrus.Infof("skipping pvc creation [%v/%v] as it already exists", pvc.Namespace, pvc.Name)
			}
			// Once a PVC is created, update the reference to it in the resourceBackup CR and later
			// check if PVC is bounded or not
			// Check the PVC state to be in bounded state and update the resourceBackup CR with the same
			rb, err := kdmpschedops.Instance().GetResourceBackup(rbCrName, rbCrNamespace)
			if err != nil {
				errMsg := fmt.Sprintf("error reading ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
				return fmt.Errorf(errMsg)
			}
			// TODO: In future we can optimize this path where we can directly get runtime.Unstructured from the preRestoreObjects
			// for the respective pvc
			o, err := fetchObjectFromPVC(preRestoreObjects, pvc, restore.Spec.NamespaceMapping)
			if err != nil {
				// Proceed to next PVC
				continue
			}
			if err = updateResourceStatus(
				rb,
				o,
				kdmpapi.ResourceRestoreStatusInProgress,
				"PVC Bound in progress"); err != nil {
				return err
			}
			_, err = kdmpschedops.Instance().UpdateResourceBackup(rb)
			if err != nil {
				errMsg := fmt.Sprintf("error updating ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
				return fmt.Errorf(errMsg)
			}
		}
	}

	// Check if all the PVC's are in bounded state
	rb, err := kdmpschedops.Instance().GetResourceBackup(rbCrName, rbCrNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("error reading ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
		return fmt.Errorf(errMsg)
	}
	// Updating vol info

	rb.VolumesInfo = resourceBackupVolInfos
	rb.ExistingVolumesInfo = existingResourceBackupVolInfos
	// Updation of rb cr happens inside isPVCsBounded()
	err = isPVCsBounded(rb)
	if err != nil {
		return err
	}

	rbUpdated, err := kdmpschedops.Instance().GetResourceBackup(rbCrName, rbCrNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("error reading ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
		return fmt.Errorf(errMsg)
	}
	logrus.Infof("%s rb cr after update: %+v", funct, rbUpdated)
	logrus.Infof("Completed job successfully")
	return nil
}

func fetchObjectFromPVC(
	preRestoreObjects []runtime.Unstructured,
	pvc *v1.PersistentVolumeClaim,
	namespaceMapping map[string]string,
) (runtime.Unstructured, error) {
	// For the given PVC fetch the object from preRestoreObjects
	for _, o := range preRestoreObjects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}
		if objectType.GetKind() == "PersistentVolumeClaim" {
			var tempPVC v1.PersistentVolumeClaim
			err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &tempPVC)
			if err != nil {
				return nil, err
			}
			restoreNamespace, ok := namespaceMapping[tempPVC.Namespace]
			if !ok {
				return nil, fmt.Errorf("sivakumar restore namespace mapping not found: %s namespaceMapping %v ", restoreNamespace, namespaceMapping)
			}
			if tempPVC.Name == pvc.Name && restoreNamespace == pvc.Namespace {
				// update the namespace based on the namespace mapping
				tempPVC.Namespace = pvc.Namespace
				object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&tempPVC)
				if err != nil {
					logrus.Errorf("unable to convert pvc[%v/%v] to unstruct objects, err: %v", pvc.Namespace, pvc.Name, err)
					return nil, err
				}
				o.SetUnstructuredContent(object)
				return o, nil
			}

		}
	}

	return nil, nil
}

func waitForPVCBound(
	res *kdmpapi.ResourceRestoreResourceInfo,
) (bool, error) {
	// wait for pvc to get bound
	var pvc *v1.PersistentVolumeClaim
	var err error
	var errMsg string
	wErr := wait.ExponentialBackoff(volumeAPICallBackoff, func() (bool, error) {
		pvc, err = core.Instance().GetPersistentVolumeClaim(res.Name, res.Namespace)
		if err != nil {
			return false, err
		}

		if pvc.Status.Phase != v1.ClaimBound {
			errMsg = fmt.Sprintf("pvc status: expected %s, got %s", v1.ClaimBound, pvc.Status.Phase)
			logrus.Debugf("%v", errMsg)
			return false, nil
		}

		return true, nil
	})

	if wErr != nil {
		logrus.Errorf("%v", wErr)
		return false, fmt.Errorf("%s:%s", wErr, errMsg)
	}
	return true, nil
}

func isPVCsBounded(
	rb *kdmpapi.ResourceBackup,
) error {
	funct := "isPVCsBounded"
	tResources := make([]*kdmpapi.ResourceRestoreResourceInfo, 0)
	cleanupTask := func() (interface{}, bool, error) {
		for _, res := range rb.Status.Resources {
			tempPVC, err := core.Instance().GetPersistentVolumeClaim(res.Name, res.Namespace)
			logrus.Tracef("%s tempPVC: %v", funct, tempPVC)
			if err != nil {
				return nil, true, fmt.Errorf("failed to fetch PVC %s: %s", res.Name, err.Error())
			}
			storageClassName := k8shelper.GetPersistentVolumeClaimClass(tempPVC)
			isBounded := false
			if storageClassName != "" {
				var checkErr error
				var sc *storagev1.StorageClass
				sc, checkErr = storage.Instance().GetStorageClass(storageClassName)
				if checkErr != nil {
					return "", true, checkErr
				}
				if *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
					isBounded, err = waitForPVCBound(res)
					if err != nil {
						return "", true, err
					}
				}
			} else {
				isBounded, err = waitForPVCBound(res)
				if err != nil {
					return "", true, err
				}
			}
			if isBounded {
				// update the pvc resource status to successful
				res.Status = kdmpapi.ResourceRestoreStatusSuccessful
				res.Reason = utils.PvcBoundSuccessMsg
				tResources = append(tResources, res)
			}
		}
		return "", false, nil
	}

	if _, err := task.DoRetryWithTimeout(cleanupTask, defaultTimeout, progressCheckInterval); err != nil {
		errMsg := fmt.Sprintf("max retries done, resourceBackup: [%v/%v] failed with %v", rb.Namespace, rb.Name, err)
		logrus.Errorf("%v", errMsg)
		// Exhausted all retries, fail the CR
		rb.Status.Status = kdmpapi.ResourceBackupStatusFailed
		rb.Status.Reason = utils.PvcBoundFailedMsg
		rb.Status.ProgressPercentage = 0
	} else {
		rb.Status.Resources = tResources
		rb.Status.Status = kdmpapi.ResourceBackupStatusSuccessful
		rb.Status.Reason = utils.PvcBoundSuccessMsg
		rb.Status.ProgressPercentage = 100
	}

	rb1, err := kdmpschedops.Instance().UpdateResourceBackup(rb)
	if err != nil {
		errMsg := fmt.Sprintf("error updating ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
		return fmt.Errorf(errMsg)
	}

	logrus.Infof("%s rb.volinfo: %+v", funct, rb1.VolumesInfo)
	return nil
}

func getNamespacedObjectsToDelete(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	tempObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		// Skip PVs, we will let the PVC handle PV deletion where needed
		if objectType.GetKind() != "PersistentVolume" {
			tempObjects = append(tempObjects, o)
		}
	}

	return tempObjects, nil
}

func skipVolumesFromRestoreList(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
	driver volume.Driver,
	volInfo []*storkapi.ApplicationBackupVolumeInfo,
) ([]*storkapi.ApplicationBackupVolumeInfo, []*storkapi.ApplicationRestoreVolumeInfo, error) {
	existingInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	newVolInfos := make([]*storkapi.ApplicationBackupVolumeInfo, 0)
	for _, bkupVolInfo := range volInfo {
		restoreVolInfo := &storkapi.ApplicationRestoreVolumeInfo{}
		val, ok := restore.Spec.NamespaceMapping[bkupVolInfo.Namespace]
		if !ok {
			logrus.Infof("skipping namespace %s for restore", bkupVolInfo.Namespace)
			continue
		}

		// get corresponding pvc object from objects list
		pvcObject, err := volume.GetPVCFromObjects(objects, bkupVolInfo)
		if err != nil {
			return newVolInfos, existingInfos, err
		}

		ns := val
		pvc, err := core.Instance().GetPersistentVolumeClaim(pvcObject.Name, ns)
		if err != nil {
			if k8s_errors.IsNotFound(err) {
				newVolInfos = append(newVolInfos, bkupVolInfo)
				continue
			}
			return newVolInfos, existingInfos, fmt.Errorf("error getting pvc %s/%s: %v", ns, pvcObject.Name, err)
		}

		restoreVolInfo.PersistentVolumeClaim = bkupVolInfo.PersistentVolumeClaim
		restoreVolInfo.PersistentVolumeClaimUID = bkupVolInfo.PersistentVolumeClaimUID
		restoreVolInfo.SourceNamespace = bkupVolInfo.Namespace
		restoreVolInfo.SourceVolume = bkupVolInfo.Volume
		restoreVolInfo.DriverName = driver.String()
		restoreVolInfo.Status = storkapi.ApplicationRestoreStatusRetained
		restoreVolInfo.RestoreVolume = pvc.Spec.VolumeName
		restoreVolInfo.TotalSize = bkupVolInfo.TotalSize
		restoreVolInfo.Reason = fmt.Sprintf("Skipped from volume restore as policy is set to %s and pvc already exists", storkapi.ApplicationRestoreReplacePolicyRetain)
		existingInfos = append(existingInfos, restoreVolInfo)
	}

	return newVolInfos, existingInfos, nil
}

// GetPreRestoreResources fetches k8s resources
func GetPreRestoreResources(
	backup *storkapi.ApplicationBackup,
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	return getRestorePVCs(backup, restore, objects)
}

func getRestorePVCs(
	backup *storkapi.ApplicationBackup,
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	pvcs := []runtime.Unstructured{}
	// iterate through all of objects and process only pvcs
	// check if source pvc is present in storageclass mapping
	// update pvc storage class if found in storageclass mapping
	// TODO: need to make sure pv name remains updated
	for _, object := range objects {
		objectType, err := meta.TypeAccessor(object)
		if err != nil {
			return nil, err
		}

		switch objectType.GetKind() {
		case "PersistentVolumeClaim":
			var pvc v1.PersistentVolumeClaim
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object.UnstructuredContent(), &pvc); err != nil {
				return nil, err
			}
			sc := k8shelper.GetPersistentVolumeClaimClass(&pvc)
			if val, ok := restore.Spec.StorageClassMapping[sc]; ok {
				pvc.Spec.StorageClassName = &val
			}
			// If pvc storageClassName is empty, we want to pick up the
			// default storageclass configured on the cluster.
			// Default storage class will not selected, if the storageclass
			// is empty. So setting it to nil.
			if pvc.Spec.StorageClassName != nil {
				if len(*pvc.Spec.StorageClassName) == 0 {
					pvc.Spec.StorageClassName = nil
				}
			}
			pvc.Spec.VolumeName = ""
			pvc.Spec.DataSource = nil
			if pvc.Annotations != nil {
				delete(pvc.Annotations, bindCompletedKey)
				delete(pvc.Annotations, boundByControllerKey)
				delete(pvc.Annotations, storageClassKey)
				delete(pvc.Annotations, storageProvisioner)
				delete(pvc.Annotations, storageNodeAnnotation)
				pvc.Annotations[KdmpAnnotation] = StorkAnnotation
			}
			o, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&pvc)
			if err != nil {
				logrus.Errorf("unable to convert pvc to unstruct objects, err: %v", err)
				return nil, err
			}
			object.SetUnstructuredContent(o)
			pvcs = append(pvcs, object)
		default:
			continue
		}
	}
	return pvcs, nil
}
