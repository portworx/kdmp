package nfs

import (
	"fmt"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/log"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/kdmp/pkg/executor"
	kdmpschedops "github.com/portworx/sched-ops/k8s/kdmp"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newProcessVMResourcesCommand() *cobra.Command {
	restoreCommand := &cobra.Command{
		Use:   "process-vm-resource",
		Short: "Start a vm includeresource processing for vm restore from nfs target",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(restoreVMResources(applicationrestoreCR, restoreNamespace, rbCrName, rbCrNamespace))
		},
	}
	restoreCommand.Flags().StringVar(&restoreNamespace, "restore-namespace", "", "Namespace for restore CR")
	restoreCommand.Flags().StringVar(&applicationrestoreCR, "app-cr-name", "", "application restore CR name")
	restoreCommand.Flags().StringVar(&rbCrName, "rb-cr-name", "", "Name for resourcebackup CR to update job status")
	restoreCommand.Flags().StringVar(&rbCrNamespace, "rb-cr-namespace", "", "Namespace for resourcebackup CR to update job status")

	return restoreCommand
}

func restoreVMResources(
	applicationCRName string,
	restoreNamespace string,
	rbCrName string,
	rbCrNamespace string,
) error {

	updateStatusOnError := func(err error) error {
		//update ResourceBackup CR with status and reason
		logrus.Errorf("restore resources for [%v/%v] failed with error: %v", rbCrNamespace, rbCrName, err.Error())
		st := kdmpapi.ResourceBackupProgressStatus{
			Status:             kdmpapi.ResourceBackupStatusFailed,
			Reason:             err.Error(),
			ProgressPercentage: 0,
		}

		err = executor.UpdateResourceBackupStatus(st, rbCrName, rbCrNamespace)
		if err != nil {
			logrus.Errorf("failed to update ResourceBackup[%v/%v] status after hitting error in processing vm resources: %v", rbCrNamespace, rbCrName, err)
		}
		return err
	}

	restore, err := storkops.Instance().GetApplicationRestore(applicationCRName, restoreNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("error getting restore cr [%v/%v]: %v", rbCrNamespace, rbCrName, err)
		return updateStatusOnError(fmt.Errorf(errMsg))
	}
	rb, err := kdmpschedops.Instance().GetResourceBackup(rbCrName, rbCrNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("error reading ResourceBackup CR[%v/%v]: %v", rbCrNamespace, rbCrName, err)
		log.ApplicationRestoreLog(restore).Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	backup, err := storkops.Instance().GetApplicationBackup(restore.Spec.BackupName, restore.Namespace)
	if err != nil {
		errMsg := fmt.Sprintf("error getting backup cr [%v/%v]: %v", rbCrNamespace, rbCrName, err)
		log.ApplicationRestoreLog(restore).Errorf(errMsg)
		return updateStatusOnError(fmt.Errorf(errMsg))
	}
	objects, err := downloadResources(backup, restore.Spec.BackupLocation, restore.Namespace)
	if err != nil {
		errMsg := fmt.Sprintf("error downloading resources from backupLocation [%v/%v]: %v", rbCrNamespace, rbCrName, err)
		log.ApplicationRestoreLog(restore).Errorf(errMsg)
		return updateStatusOnError(fmt.Errorf(errMsg))
	}
	// get objectMap of includeResources
	objectMap := storkapi.CreateObjectsMap(restore.Spec.IncludeResources)
	includeResourceList, err := resourcecollector.GetVMResourcesFromResourceObject(objects, objectMap)
	if err != nil {
		errMsg := fmt.Sprintf("error creating VM includeResource list [%v/%v]: %v", rbCrNamespace, rbCrName, err)
		log.ApplicationRestoreLog(restore).Errorf(errMsg)
		return updateStatusOnError(fmt.Errorf(errMsg))
	}
	resourceInfoList := make([]*kdmpapi.ResourceRestoreResourceInfo, 0)
	if len(includeResourceList) != 0 {
		for _, obj := range includeResourceList {
			info := &kdmpapi.ResourceRestoreResourceInfo{
				ObjectInfo: kdmpapi.ObjectInfo{
					Name:      obj.Name,
					Namespace: obj.Namespace,
					GroupVersionKind: metav1.GroupVersionKind{
						Group:   obj.Group,
						Version: obj.Version,
						Kind:    obj.Kind,
					},
				},
			}
			resourceInfoList = append(resourceInfoList, info)
		}
	}
	rb.Status.Resources = resourceInfoList
	// update ResourceBackup CR with status and reason
	st := kdmpapi.ResourceBackupProgressStatus{
		Status:             kdmpapi.ResourceBackupStatusSuccessful,
		Reason:             utils.ProcessVMResourceSuccessMsg,
		ProgressPercentage: 100,
	}
	rb.Status.Status = kdmpapi.ResourceBackupStatusSuccessful
	rb.Status.Reason = st.Reason
	rb.Status.ProgressPercentage = st.ProgressPercentage
	_, err = kdmpschedops.Instance().UpdateResourceBackup(rb)
	if err != nil {
		errMsg := fmt.Sprintf("error updating ResourceBackup CR[%v/%v]: %v", rb.Name, rb.Namespace, err)
		log.ApplicationRestoreLog(restore).Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}
