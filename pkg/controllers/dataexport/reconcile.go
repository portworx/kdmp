package dataexport

import (
	"context"
	"fmt"
	"reflect"

	"github.com/libopenstorage/stork/pkg/controllers"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/sched-ops/k8s/core"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
)

// Data export label names/keys.
const (
	LabelController     = "kdmp.portworx.com/controller"
	LabelControllerName = "controller-name"
)

func (c *Controller) sync(ctx context.Context, in *kdmpapi.DataExport) (bool, error) {
	if in == nil {
		return false, nil
	}

	dataExport := *in

	// TODO: validate DataExport resource
	driver, err := driversinstance.Get(string(dataExport.Spec.Type))
	if err != nil {
		return false, fmt.Errorf("%q driver: not found", dataExport.Spec.Type)
	}

	if dataExport.DeletionTimestamp != nil {
		if !controllers.ContainsFinalizer(&dataExport, cleanupFinalizer) {
			return false, nil
		}

		if err = c.cleanUp(driver, dataExport); err != nil {
			return true, fmt.Errorf("%s: cleanup: %s", reflect.TypeOf(dataExport), err)
		}

		controllers.RemoveFinalizer(&dataExport, cleanupFinalizer)
		return true, c.client.Update(ctx, &dataExport)
	}

	// set stage
	if dataExport.Status.Stage == "" {
		dataExport.Status.Stage = kdmpapi.DataExportStageInitial
		dataExport.Status.Status = kdmpapi.DataExportStatusInitial
		return true, c.client.Update(ctx, &dataExport)
	}

	switch dataExport.Status.Stage {
	case kdmpapi.DataExportStageInitial:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
			return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// ensure srd/dst volumes are available
		if err := c.checkClaims(&dataExport); err != nil {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusFailed, err.Error())
		}

		return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	case kdmpapi.DataExportStageTransferScheduled:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageTransferInProgress
			return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// start data transfer
		id, err := driver.StartJob(
			drivers.WithSourcePVC(dataExport.Spec.Source.PersistentVolumeClaim.GetName()),
			drivers.WithDestinationPVC(dataExport.Spec.Destination.PersistentVolumeClaim.GetName()),
			drivers.WithNamespace(dataExport.Spec.Source.PersistentVolumeClaim.GetNamespace()),
			drivers.WithLabels(jobLabels(dataExport.GetName())),
		)
		if err != nil {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusFailed, err.Error())
		}

		dataExport.Status.TransferID = id
		return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	case kdmpapi.DataExportStageTransferInProgress:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageFinal
			return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// get transfer job status
		progress, err := driver.JobStatus(dataExport.Status.TransferID)
		if err != nil {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusFailed, err.Error())
		}
		dataExport.Status.ProgressPercentage = progress

		// transfer in progress
		if !drivers.IsTransferCompleted(progress) {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusInProgress, string(progress))
		}

		return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	case kdmpapi.DataExportStageFinal:
		if dataExport.Status.Status != kdmpapi.DataExportStatusSuccessful {
			// TODO: is it required to remove the rsync job? (it contains data transfer logs but volumes are still mounted)
			return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusSuccessful, ""))
		}
	}

	return false, nil
}

func (c *Controller) cleanUp(driver drivers.Interface, de kdmpapi.DataExport) error {
	if driver == nil {
		return fmt.Errorf("driver is nil")
	}
	if de.Status.TransferID == "" {
		return nil
	}
	return driver.DeleteJob(de.Status.TransferID)
}

func (c *Controller) updateStatus(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, errMsg string) error {
	if isStatusEqual(de, status, errMsg) {
		return nil
	}
	return c.client.Update(context.TODO(), setStatus(de, status, errMsg))
}

func (c *Controller) checkClaims(de *kdmpapi.DataExport) error {
	srcPVC := de.Spec.Source.PersistentVolumeClaim
	if err := c.ensureUnmountedPVC(srcPVC.Name, srcPVC.Namespace, de.Name); err != nil {
		return fmt.Errorf("source pvc: %s/%s: %v", srcPVC.Namespace, srcPVC.Name, err)
	}

	dstPVC := de.Spec.Destination.PersistentVolumeClaim
	// TODO: use size of src pvc if not provided
	if err := c.ensureUnmountedPVC(dstPVC.Name, dstPVC.Namespace, de.Name); err != nil {
		// create pvc if it's not found
		if errors.IsNotFound(err) {
			return c.client.Create(context.TODO(), dstPVC)
		}

		return fmt.Errorf("destination pvc: %s/%s: %v", dstPVC.Namespace, dstPVC.Name, err)
	}

	return nil
}

func (c *Controller) ensureUnmountedPVC(name, namespace, dataExportName string) error {
	pvc, err := core.Instance().GetPersistentVolumeClaim(name, namespace)
	if err != nil {
		return err
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return fmt.Errorf("status: expected %s, got %s", corev1.ClaimBound, pvc.Status.Phase)
	}

	// check if pvc is mounted
	pods, err := getMountPods(pvc.Name, pvc.Namespace)
	if err != nil {
		return fmt.Errorf("get mounted pods: %v", err)
	}
	mounted := make([]corev1.Pod, 0)
	for _, pod := range pods {
		// pvc is mounted to pod created for this volume
		if pod.Labels[LabelControllerName] == dataExportName {
			continue
		}
		mounted = append(mounted, pod)
	}
	if len(mounted) > 0 {
		return fmt.Errorf("mounted to %v pods", toPodNames(pods))
	}

	return nil
}

func getMountPods(pvcName, namespace string) ([]corev1.Pod, error) {
	return core.Instance().GetPodsUsingPVC(pvcName, namespace)
}

func toPodNames(objs []corev1.Pod) []string {
	out := make([]string, 0)
	for _, o := range objs {
		out = append(out, o.Name)
	}
	return out
}

func setStatus(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, reason string) *kdmpapi.DataExport {
	de.Status.Status = status
	de.Status.Reason = reason
	return de
}

func isStatusEqual(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, reason string) bool {
	return de.Status.Status == status && de.Status.Reason == reason
}

func jobLabels(DataExportName string) map[string]string {
	return map[string]string{
		LabelController:     DataExportName,
		LabelControllerName: DataExportName,
	}
}
