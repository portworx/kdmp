package nfs

import (
	"fmt"
	// "strconv"
	"time"

	"github.com/libopenstorage/stork/drivers/volume"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	// "github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/sched-ops/k8s/core"
	kdmpschedops "github.com/portworx/sched-ops/k8s/kdmp"
	storkops "github.com/portworx/sched-ops/k8s/stork"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
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

func newRestoreVolumeCommand() *cobra.Command {
	restoreCommand := &cobra.Command{
		Use:   "restore-vol",
		Short: "Download vol resource and create pvc",
		Run: func(c *cobra.Command, args []string) {
			err := restoreVolResourcesAndApply(appRestoreCRName, restoreNamespace, rbCrName, rbCrNamespace)
			if err != nil {
				// Update the resource backup status with error
				rbStatus := kdmpapi.ResourceBackupProgressStatus{
					Status:             kdmpapi.ResourceBackupStatusFailed,
					Reason:             err.Error(),
					ProgressPercentage: 0,
				}
				_, err = executor.UpdateStatusInResourceBackup(rbStatus, rbCrName, rbCrNamespace)
				if err != nil {
					logrus.Errorf("failed to update resorucebackup[%v/%v] status after hitting error in create namespace : %v", rbCrNamespace, rbCrName, err)
				}
			}
			executor.HandleErr(err)
		},
	}
	restoreCommand.PersistentFlags().StringVarP(&restoreNamespace, "restore-namespace", "", "", "Namespace for restore CR")
	restoreCommand.PersistentFlags().StringVarP(&appRestoreCRName, "app-cr-name", "", "", "application restore CR name")
	restoreCommand.PersistentFlags().StringVarP(&rbCrName, "rb-cr-name", "", "", "Name for resourcebackup CR to update job status")
	restoreCommand.PersistentFlags().StringVarP(&rbCrNamespace, "rb-cr-namespace", "", "", "Namespace for resourcebackup CR to update job status")

	return restoreCommand
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
		logrus.Errorf("%v:%v Error in creating namespace: %v", restoreNamespace, applicationCRName, err)
		return err
	}

	restore, err := storkops.Instance().GetApplicationRestore(applicationCRName, restoreNamespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error in restore cr: %v", err)
		return err
	}

	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup cr: %v", err)
		return err
	}
	// Iterate over all the vol info from the backup spec

	objects, err := downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error downloading resources: %v", err)
		return err
	}
	var isCsiDriver bool
	for _, volume := range backup.Status.Volumes {
		if volume.DriverName == "csi" {
			isCsiDriver = true
		}
	}
	var storageClassByte []byte
	if isCsiDriver {
		storageClassByte, err = downloadStorageClass(backup, restore.Spec.BackupLocation, restore.Namespace)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("Error downloading storageclass json file: %v", err)
			return err
		}
	}
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
	backupVolumeInfoMappings := make(map[string][]*storkapi.ApplicationBackupVolumeInfo)
	for _, namespace := range backup.Spec.Namespaces {
		if _, ok := restore.Spec.NamespaceMapping[namespace]; !ok {
			continue
		}
		for _, volumeBackup := range backup.Status.Volumes {
			driverName := volumeBackup.DriverName
			if volumeBackup.Namespace != namespace || volumeBackup.Status == storkapi.ApplicationBackupStatusFailed {
				continue
			}

			// Skip portworx volumes in case of nfsRestore PB-6687
			if driverName == "pxd" {
				logrus.Debugf("%s : Skipping portworx volume %s", funct, volumeBackup.Volume)
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
	existingRestoreVolInfos := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	restoreCompleteList := make([]*storkapi.ApplicationRestoreVolumeInfo, 0)
	driverToRestoreCompleteListMap := make(map[string][]*storkapi.ApplicationBackupVolumeInfo)
	// var sErr error
	// var restoreVolumeInfos []*storkapi.ApplicationRestoreVolumeInfo

	for driverName, bkpvInfo := range backupVolumeInfoMappings {
		driver, err := volume.Get(driverName)
		if err != nil {
			return err
		}
		backupVolInfos := bkpvInfo
		// Skip pv/pvc if replacepolicy is set to retain to avoid creating
		if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyRetain {
			backupVolInfos, existingRestoreVolInfos, err = skipVolumesFromRestoreList(restore, objects, driver, backupVolInfos)
			if err != nil {
				log.ApplicationRestoreLog(restore).Errorf("Error while checking pvcs: %v", err)
				return err
			}
			logrus.Tracef("backupVolInfos: %+v", backupVolInfos)
			logrus.Tracef("existingRestoreVolInfos: %+v", existingRestoreVolInfos)
		}
		driverToRestoreCompleteListMap[driverName] = backupVolInfos

		// restoreCompleteList = append(restoreCompleteList, existingRestoreVolInfos...)
		preRestoreObjects, err := driver.GetPreRestoreResources(backup, restore, objects, storageClassByte)
		if err != nil {
			log.ApplicationRestoreLog(restore).Errorf("Error getting PreRestore Resources: %v", err)
			return err
		}
		rb, err := kdmpschedops.Instance().GetResourceBackup(rbCrName, rbCrNamespace)
		if err != nil {
			errMsg := fmt.Sprintf("error reading ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
			return fmt.Errorf(errMsg)
		}
		if driverName != "kdmp" {
			if err := applyResources(restore, rb, preRestoreObjects); err != nil {
				log.ApplicationRestoreLog(restore).Errorf("Error in applyResources: %v", err)
				return err
			}
		}
		var opts resourcecollector.Options
		if len(restore.Spec.RancherProjectMapping) != 0 {
			rancherProjectMapping := getRancherProjectMapping(restore)
			opts = resourcecollector.Options{
				RancherProjectMappings: rancherProjectMapping,
			}
		}

		if (driverName == "csi" || driverName == "kdmp") && restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
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
					&opts,
					restore.Spec.BackupLocation,
					restore.Namespace,
				)
				if err != nil {
					log.ApplicationRestoreLog(restore).Errorf("Error from PrepareResourceForApply: %v", err)
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
				log.ApplicationRestoreLog(restore).Errorf("Error from getNamespacedObjectsToDelete: %v", err)
				return err
			}

			err = rc.DeleteResources(
				dynamicInterface,
				tempObjects,
				nil)
			if err != nil {
				log.ApplicationRestoreLog(restore).Errorf("Error from DeleteResources: %v", err)
				return err
			}
		}
		/*
			// Get restore volume batch sleep interval
			volumeBatchSleepInterval, err := time.ParseDuration(k8sutils.DefaultRestoreVolumeBatchSleepInterval)
			if err != nil {
				logrus.Infof("error in parsing default restore volume sleep interval %s",
					k8sutils.DefaultRestoreVolumeBatchSleepInterval)
			}
			RestoreVolumeBatchSleepInterval, err := k8sutils.GetConfigValue(k8sutils.StorkControllerConfigMapName,
				metav1.NamespaceSystem, k8sutils.RestoreVolumeBatchSleepIntervalKey)
			if err != nil {
				logrus.Infof("error in reading %v cm, switching to default restore volume sleep interval",
					k8sutils.StorkControllerConfigMapName)
			} else {
				if len(RestoreVolumeBatchSleepInterval) != 0 {
					volumeBatchSleepInterval, err = time.ParseDuration(RestoreVolumeBatchSleepInterval)
					if err != nil {
						logrus.Infof("error in conversion of volumeBatchSleepInterval: %v", err)
					}
				}
			}
			// Get restore volume batch count
			batchCount := k8sutils.DefaultRestoreVolumeBatchCount
			restoreVolumeBatchCount, err := k8sutils.GetConfigValue(k8sutils.StorkControllerConfigMapName, metav1.NamespaceSystem, k8sutils.RestoreVolumeBatchCountKey)
			if err != nil {
				logrus.Debugf("error in reading %v cm, defaulting restore volume batch count", k8sutils.StorkControllerConfigMapName)
			} else {
				if len(restoreVolumeBatchCount) != 0 {
					batchCount, err = strconv.Atoi(restoreVolumeBatchCount)
					if err != nil {
						logrus.Debugf("error in conversion of restoreVolumeBatchCount: %v", err)
					}
				}
			}
			for i := 0; i < len(backupVolInfos); i += batchCount {
				batchVolInfo := backupVolInfos[i:min(i+batchCount, len(backupVolInfos))]
				restoreVolumeInfos, sErr = driver.StartRestore(restore, batchVolInfo, preRestoreObjects)
				if sErr != nil {
					if _, ok := sErr.(*volume.ErrStorageProviderBusy); ok {
						msg := fmt.Sprintf("Volume restores are in progress. Restores are failing for some volumes"+
							" since the storage provider is busy. Restore will be retried. Error: %v", err)
						log.ApplicationRestoreLog(restore).Errorf(msg)
						rb, err := kdmpschedops.Instance().GetResourceBackup(rbCrName, rbCrNamespace)
						if err != nil {
							errMsg := fmt.Sprintf("error reading ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
							return fmt.Errorf(errMsg)
						}
						// Update the success status to resource backup CR
						rb.Status.Status = kdmpapi.ResourceBackupStatusInProgress
						rb.Status.Reason = msg
						rb.Status.ProgressPercentage = 0

						_, err = kdmpschedops.Instance().UpdateResourceBackup(rb)
						if err != nil {
							errMsg := fmt.Sprintf("error updating ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
							return fmt.Errorf(errMsg)
						}
						return nil
					}
					message := fmt.Sprintf("Error starting Application Restore for volumes: %v", sErr)
					log.ApplicationRestoreLog(restore).Errorf(message)
					return sErr
				}
				time.Sleep(volumeBatchSleepInterval)
				restoreCompleteList = append(restoreCompleteList, restoreVolumeInfos...)
			}
		*/
	}
	// Check if all the PVC's are in bounded state
	rb, err := kdmpschedops.Instance().GetResourceBackup(rbCrName, rbCrNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("error reading ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
		return fmt.Errorf(errMsg)
	}
	// Update the success status to resource backup CR
	rb.DriverToRestoreCompleteListMap = driverToRestoreCompleteListMap
	rb.ExistingVolumeInfoList = existingRestoreVolInfos
	rb.Status.Status = kdmpapi.ResourceBackupStatusSuccessful
	rb.Status.Reason = utils.PvcBoundSuccessMsg
	rb.Status.ProgressPercentage = 100
	rb.RestoreCompleteList = restoreCompleteList

	rb, err = kdmpschedops.Instance().UpdateResourceBackup(rb)
	if err != nil {
		errMsg := fmt.Sprintf("error updating ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
		return fmt.Errorf(errMsg)
	}
	logrus.Infof("%s rb cr after update: %+v", funct, rb)
	logrus.Infof("Completed job successfully")
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
		pvName := pvc.Spec.VolumeName
		var zones []string
		// If PVC is present, fetch the corresponding PV spec and get the zone information
		if driver.String() == volume.GCEDriverName || driver.String() == volume.AWSDriverName {
			pv, err := core.Instance().GetPersistentVolume(pvName)
			if err != nil && !k8s_errors.IsNotFound(err) {
				return newVolInfos, existingInfos, fmt.Errorf("erorr getting pv %s: %v", pvName, err)
			}

			if driver.String() == volume.GCEDriverName {
				zones = volume.GetGCPZones(pv)
			}
			if driver.String() == volume.AWSDriverName {
				zones, err = volume.GetAWSZones(pv)
				if err != nil {
					return newVolInfos, existingInfos, fmt.Errorf("erorr getting AWS zones : %v", err)
				}
			}
			// if fetched zones are empty, assign it to the source zones
			if len(zones) == 0 {
				zones = bkupVolInfo.Zones
			}
		}

		restoreVolInfo.PersistentVolumeClaim = bkupVolInfo.PersistentVolumeClaim
		restoreVolInfo.PersistentVolumeClaimUID = bkupVolInfo.PersistentVolumeClaimUID
		restoreVolInfo.SourceNamespace = bkupVolInfo.Namespace
		restoreVolInfo.SourceVolume = bkupVolInfo.Volume
		restoreVolInfo.DriverName = driver.String()
		restoreVolInfo.Status = storkapi.ApplicationRestoreStatusRetained
		restoreVolInfo.RestoreVolume = pvc.Spec.VolumeName
		restoreVolInfo.TotalSize = bkupVolInfo.TotalSize
		restoreVolInfo.Zones = zones
		restoreVolInfo.Reason = fmt.Sprintf("Skipped from volume restore as policy is set to %s and pvc already exists", storkapi.ApplicationRestoreReplacePolicyRetain)
		existingInfos = append(existingInfos, restoreVolInfo)
	}

	return newVolInfos, existingInfos, nil
}
