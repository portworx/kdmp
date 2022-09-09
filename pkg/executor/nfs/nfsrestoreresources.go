package nfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/libopenstorage/stork/drivers/volume"
	"github.com/libopenstorage/stork/drivers/volume/kdmp"
	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/crypto"
	"github.com/libopenstorage/stork/pkg/k8sutils"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/portworx/kdmp/pkg/executor"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

var (
	namespace            string
	applicationrestoreCR string
)

func newRestoreResourcesCommand() *cobra.Command {
	restoreCommand := &cobra.Command{
		Use:   "backup",
		Short: "Start a resource backup to nfs target",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(restoreResources(namespace, applicationrestoreCR))
		},
	}
	restoreCommand.Flags().StringVarP(&bkpNamespace, "namespace", "", "", "Namespace for restore command")
	restoreCommand.Flags().StringVarP(&applicationCRName, "app-cr-name", "", "", "application restore CR name")

	return restoreCommand
}

func restoreResources(
	namespace, applicationCRName string,
) error {
	restore, err := storkops.Instance().GetApplicationRestore(applicationCRName, namespace)
	if err != nil {
		logrus.Errorf("Error getting restore cr: %v", err)
		return err
	}

	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting backup: %v", err)
		return err
	}
	objects, err := downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error downloading resources: %v", err)
		return err
	}

	if err := applyResources(restore, objects); err != nil {
		return err
	}
	return nil
}

func downloadCRD(
	resourcePath string,
	resourceFileName string,
	encryptionKey string,
) error {
	var crds []*apiextensionsv1beta1.CustomResourceDefinition
	var crdsV1 []*apiextensionsv1.CustomResourceDefinition
	crdData, err := downloadObject(resourcePath, resourceFileName, encryptionKey)
	if err != nil {
		return err
	}
	// No CRDs were uploaded
	if crdData == nil {
		return nil
	}
	if err = json.Unmarshal(crdData, &crds); err != nil {
		return err
	}
	if err = json.Unmarshal(crdData, &crdsV1); err != nil {
		return err
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("error getting cluster config: %v", err)
	}

	client, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return err
	}

	regCrd := make(map[string]bool)
	for _, crd := range crds {
		crd.ResourceVersion = ""
		regCrd[crd.GetName()] = false
		if _, err := client.ApiextensionsV1beta1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
			regCrd[crd.GetName()] = true
			logrus.Warnf("error registering crds v1beta1 %v,%v", crd.GetName(), err)
			continue
		}
		// wait for crd to be ready
		if err := k8sutils.ValidateCRD(client, crd.GetName()); err != nil {
			logrus.Warnf("Unable to validate crds v1beta1 %v,%v", crd.GetName(), err)
		}
	}

	for _, crd := range crdsV1 {
		if val, ok := regCrd[crd.GetName()]; ok && val {
			crd.ResourceVersion = ""
			var updatedVersions []apiextensionsv1.CustomResourceDefinitionVersion
			// try to apply as v1 crd
			var err error
			if _, err = client.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{}); err == nil || errors.IsAlreadyExists(err) {
				logrus.Infof("registered v1 crds %v,", crd.GetName())
				continue
			}
			// updated fields
			crd.Spec.PreserveUnknownFields = false
			for _, version := range crd.Spec.Versions {
				isTrue := true
				if version.Schema == nil {
					openAPISchema := &apiextensionsv1.CustomResourceValidation{
						OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{XPreserveUnknownFields: &isTrue},
					}
					version.Schema = openAPISchema
				} else {
					version.Schema.OpenAPIV3Schema.XPreserveUnknownFields = &isTrue
				}
				updatedVersions = append(updatedVersions, version)
			}
			crd.Spec.Versions = updatedVersions

			if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(context.TODO(), crd, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
				logrus.Warnf("error registering crdsv1 %v,%v", crd.GetName(), err)
				continue
			}
			// wait for crd to be ready
			if err := k8sutils.ValidateCRDV1(client, crd.GetName()); err != nil {
				logrus.Warnf("Unable to validate crdsv1 %v,%v", crd.GetName(), err)
			}

		}
	}

	return nil
}

func downloadObject(
	resourcePath string,
	resourceFileName string,
	encryptionKey string,
) ([]byte, error) {
	filePath := filepath.Join(resourcePath, resourceFileName)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("getting file content of %s failed: %v", filePath, err)
	}

	if len(encryptionKey) > 0 {
		var decryptData []byte
		if decryptData, err = crypto.Decrypt(data, encryptionKey); err != nil {
			logrus.Errorf("nfs downloadObject: decrypt failed :%v, returning data direclty", err)
			return data, nil
		}
		return decryptData, nil
	}
	return data, nil
}

func downloadResources(
	backup *storkapi.ApplicationBackup,
	backupLocation string,
	namespace string,
) ([]runtime.Unstructured, error) {
	funct := "downloadResources"
	repo, err := executor.ParseCloudCred()
	if err != nil {
		logrus.Errorf("%s: error parsing cloud cred: %v", funct, err)
		return nil, err
	}
	bkpDir := filepath.Join("/tmp", repo.Path, bkpNamespace, backup.ObjectMeta.Name, string(backup.ObjectMeta.UID))

	restoreLocation, err := storkops.Instance().GetBackupLocation(backup.Spec.BackupLocation, namespace)
	if err != nil {
		return nil, err
	}
	// create CRD resource first
	if err := downloadCRD(bkpDir, crdFile, restoreLocation.Location.EncryptionV2Key); err != nil {
		return nil, fmt.Errorf("error downloading CRDs: %v", err)
	}
	data, err := downloadObject(bkpDir, resourcesFile, restoreLocation.Location.EncryptionV2Key)
	if err != nil {
		return nil, fmt.Errorf("error downloading resources: %v", err)
	}

	objects := make([]*unstructured.Unstructured, 0)
	if err = json.Unmarshal(data, &objects); err != nil {
		return nil, err
	}
	runtimeObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		runtimeObjects = append(runtimeObjects, o)
	}
	return runtimeObjects, nil
}

func getPVNameMappings(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) (map[string]string, error) {
	pvNameMappings := make(map[string]string)
	for _, vInfo := range restore.Status.Volumes {
		if vInfo.SourceVolume == "" {
			return nil, fmt.Errorf("SourceVolume missing for restore")
		}
		if vInfo.RestoreVolume == "" {
			return nil, fmt.Errorf("RestoreVolume missing for restore")
		}
		pvNameMappings[vInfo.SourceVolume] = vInfo.RestoreVolume
	}
	return pvNameMappings, nil
}

func getNamespacedPVCLocation(pvc *v1.PersistentVolumeClaim) string {
	return fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
}

// getPVCToPVMapping constructs a mapping of PVC name/namespace to PV objects
func getPVCToPVMapping(allObjects []runtime.Unstructured) (map[string]*v1.PersistentVolume, error) {

	// Get mapping of PVC name to PV name
	pvNameToPVCName := make(map[string]string)
	for _, o := range allObjects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		// If a PV, assign it to the mapping based on the claimRef UID
		if objectType.GetKind() == "PersistentVolumeClaim" {
			pvc := &v1.PersistentVolumeClaim{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), pvc); err != nil {
				return nil, fmt.Errorf("error converting to persistent volume: %v", err)
			}

			pvNameToPVCName[pvc.Spec.VolumeName] = getNamespacedPVCLocation(pvc)
		}
	}

	// Get actual mapping of PVC name to PV object
	pvcNameToPV := make(map[string]*v1.PersistentVolume)
	for _, o := range allObjects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		// If a PV, assign it to the mapping based on the claimRef UID
		if objectType.GetKind() == "PersistentVolume" {
			pv := &v1.PersistentVolume{}
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), pv); err != nil {
				return nil, fmt.Errorf("error converting to persistent volume: %v", err)
			}

			pvcName := pvNameToPVCName[pv.Name]

			// add this PVC name/PV obj mapping
			pvcNameToPV[pvcName] = pv
		}
	}

	return pvcNameToPV, nil
}

func isGenericCSIPersistentVolume(pv *v1.PersistentVolume) (bool, error) {
	driverName, err := volume.GetPVDriver(pv)
	if err != nil {
		return false, err
	}
	if driverName == "csi" {
		return true, nil
	}
	return false, nil
}
func isGenericPersistentVolume(pv *v1.PersistentVolume, volInfos []*storkapi.ApplicationRestoreVolumeInfo) (bool, error) {
	for _, vol := range volInfos {
		if vol.DriverName == kdmp.GetGenericDriverName() && vol.RestoreVolume == pv.Name {
			return true, nil
		}
	}
	return false, nil
}

func isGenericCSIPersistentVolumeClaim(pvc *v1.PersistentVolumeClaim, volInfos []*storkapi.ApplicationRestoreVolumeInfo) (bool, error) {
	for _, vol := range volInfos {
		if vol.DriverName == kdmp.GetGenericDriverName() && vol.PersistentVolumeClaim == pvc.Name {
			return true, nil
		}
	}
	return false, nil
}

func removeCSIVolumesBeforeApply(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) ([]runtime.Unstructured, error) {
	tempObjects := make([]runtime.Unstructured, 0)
	// Get PVC to PV mapping first for checking if a PVC is bound to a generic CSI PV
	pvcToPVMapping, err := getPVCToPVMapping(objects)
	if err != nil {
		return nil, fmt.Errorf("failed to get PVC to PV mapping: %v", err)
	}
	for _, o := range objects {
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return nil, err
		}

		switch objectType.GetKind() {
		case "PersistentVolume":
			// check if this PV is a generic CSI one
			var pv v1.PersistentVolume
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &pv); err != nil {
				return nil, fmt.Errorf("error converting to persistent volume: %v", err)
			}

			// Check if this PV is a generic CSI one
			isGenericCSIPVC, err := isGenericCSIPersistentVolume(&pv)
			if err != nil {
				return nil, fmt.Errorf("failed to check if PV was provisioned by a CSI driver: %v", err)
			}
			isGenericDriverPV, err := isGenericPersistentVolume(&pv, restore.Status.Volumes)
			if err != nil {
				return nil, err
			}
			// Only add this object if it's not a generic CSI PV
			if !isGenericCSIPVC && !isGenericDriverPV {
				tempObjects = append(tempObjects, o)
			} else {
				log.ApplicationRestoreLog(restore).Debugf("skipping CSI PV in restore: %s", pv.Name)
			}

		case "PersistentVolumeClaim":
			// check if this PVC is a generic CSI one
			var pvc v1.PersistentVolumeClaim
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(o.UnstructuredContent(), &pvc); err != nil {
				return nil, fmt.Errorf("error converting PVC object: %v: %v", o, err)
			}

			// Find the matching PV for this PVC
			pv, ok := pvcToPVMapping[getNamespacedPVCLocation(&pvc)]
			if !ok {
				log.ApplicationRestoreLog(restore).Debugf("failed to find PV for PVC %s during CSI volume skip. Will not skip volume", pvc.Name)
				tempObjects = append(tempObjects, o)
				continue
			}

			// We have found a PV for this PVC. Check if it is a generic CSI PV
			// that we do not already have native volume driver support for.
			isGenericCSIPVC, err := isGenericCSIPersistentVolume(pv)
			if err != nil {
				return nil, err
			}
			isGenericDriverPVC, err := isGenericCSIPersistentVolumeClaim(&pvc, restore.Status.Volumes)
			if err != nil {
				return nil, err
			}

			// Only add this object if it's not a generic CSI PVC
			if !isGenericCSIPVC && !isGenericDriverPVC {
				tempObjects = append(tempObjects, o)
			} else {
				log.ApplicationRestoreLog(restore).Debugf("skipping PVC in restore: %s", pvc.Name)
			}

		default:
			// add all other objects
			tempObjects = append(tempObjects, o)
		}
	}

	return tempObjects, nil
}

func updateResourceStatus(
	restore *storkapi.ApplicationRestore,
	object runtime.Unstructured,
	status storkapi.ApplicationRestoreStatusType,
	reason string,
) error {
	var updatedResource *storkapi.ApplicationRestoreResourceInfo
	gkv := object.GetObjectKind().GroupVersionKind()
	metadata, err := meta.Accessor(object)
	if err != nil {
		log.ApplicationRestoreLog(restore).Errorf("Error getting metadata for object %v %v", object, err)
		return err
	}
	for _, resource := range restore.Status.Resources {
		if resource.Name == metadata.GetName() &&
			resource.Namespace == metadata.GetNamespace() &&
			(resource.Group == gkv.Group || (resource.Group == "core" && gkv.Group == "")) &&
			resource.Version == gkv.Version &&
			resource.Kind == gkv.Kind {
			updatedResource = resource
			break
		}
	}
	if updatedResource == nil {
		updatedResource = &storkapi.ApplicationRestoreResourceInfo{
			ObjectInfo: storkapi.ObjectInfo{
				Name:      metadata.GetName(),
				Namespace: metadata.GetNamespace(),
				GroupVersionKind: metav1.GroupVersionKind{
					Group:   gkv.Group,
					Version: gkv.Version,
					Kind:    gkv.Kind,
				},
			},
		}
		restore.Status.Resources = append(restore.Status.Resources, updatedResource)
	}

	updatedResource.Status = status
	updatedResource.Reason = reason
	return nil
}

func applyResources(
	restore *storkapi.ApplicationRestore,
	objects []runtime.Unstructured,
) error {
	resourceCollector := initResourceCollector()
	dynamicInterface, err := getDynamicInterface()
	if err != nil {
		return err
	}
	pvNameMappings, err := getPVNameMappings(restore, objects)
	if err != nil {
		return err
	}
	objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
	tempObjects := make([]runtime.Unstructured, 0)
	for _, o := range objects {
		skip, err := resourceCollector.PrepareResourceForApply(
			o,
			objects,
			objectMap,
			restore.Spec.NamespaceMapping,
			restore.Spec.StorageClassMapping,
			pvNameMappings,
			restore.Spec.IncludeOptionalResourceTypes,
			restore.Status.Volumes,
		)
		if err != nil {
			return err
		}
		if !skip {
			tempObjects = append(tempObjects, o)
		}
	}
	objects = tempObjects

	// skip CSI PV/PVCs before applying
	objects, err = removeCSIVolumesBeforeApply(restore, objects)
	if err != nil {
		return err
	}
	// First delete the existing objects if they exist and replace policy is set
	// to Delete
	if restore.Spec.ReplacePolicy == storkapi.ApplicationRestoreReplacePolicyDelete {
		err = resourceCollector.DeleteResources(
			dynamicInterface,
			objects)
		if err != nil {
			return err
		}
	}

	for _, o := range objects {
		metadata, err := meta.Accessor(o)
		if err != nil {
			return err
		}
		objectType, err := meta.TypeAccessor(o)
		if err != nil {
			return err
		}

		log.ApplicationRestoreLog(restore).Infof("Applying %v %v/%v", objectType.GetKind(), metadata.GetNamespace(), metadata.GetName())
		retained := false

		err = resourceCollector.ApplyResource(
			dynamicInterface,
			o)
		if err != nil && errors.IsAlreadyExists(err) {
			switch restore.Spec.ReplacePolicy {
			case storkapi.ApplicationRestoreReplacePolicyDelete:
				log.ApplicationRestoreLog(restore).Errorf("Error deleting %v %v during restore: %v", objectType.GetKind(), metadata.GetName(), err)
			case storkapi.ApplicationRestoreReplacePolicyRetain:
				log.ApplicationRestoreLog(restore).Warningf("Error deleting %v %v during restore, ReplacePolicy set to Retain: %v", objectType.GetKind(), metadata.GetName(), err)
				retained = true
				err = nil
			}
		}

		if err != nil {
			if err := updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusFailed,
				fmt.Sprintf("Error applying resource: %v", err)); err != nil {
				return err
			}
		} else if retained {
			if err := updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusRetained,
				"Resource restore skipped as it was already present and ReplacePolicy is set to Retain"); err != nil {
				return err
			}
		} else {
			if err := updateResourceStatus(
				restore,
				o,
				storkapi.ApplicationRestoreStatusSuccessful,
				"Resource restored successfully"); err != nil {
				return err
			}
		}
	}
	return nil
}

func getDynamicInterface() (dynamic.Interface, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("error getting cluster config: %v", err)
	}

	return dynamic.NewForConfig(config)
}
