package dataexport

import (
	"context"
	"fmt"
	"reflect"

	"github.com/libopenstorage/stork/pkg/controllers"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/driversinstance"
	"github.com/portworx/kdmp/pkg/snapshots"
	"github.com/portworx/kdmp/pkg/snapshots/snapshotsinstance"
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

	// set the initial stage
	if dataExport.Status.Stage == "" {
		// TODO: validate DataExport resource & update status?
		dataExport.Status.Stage = kdmpapi.DataExportStageInitial
		dataExport.Status.Status = kdmpapi.DataExportStatusInitial
		return true, c.client.Update(ctx, &dataExport)
	}

	driver, err := driversinstance.Get(string(dataExport.Spec.Type))
	if err != nil {
		return false, err
	}

	snapshotter, err := snapshotsinstance.GetForStorageClass(dataExport.Spec.SnapshotStorageClass)
	if err != nil {
		return false, fmt.Errorf("get snapshotter for a storage provider: %s", err)
	}

	if dataExport.DeletionTimestamp != nil {
		if !controllers.ContainsFinalizer(&dataExport, cleanupFinalizer) {
			return false, nil
		}

		if err = c.cleanUp(driver, snapshotter, dataExport); err != nil {
			return true, fmt.Errorf("%s: cleanup: %s", reflect.TypeOf(dataExport), err)
		}

		controllers.RemoveFinalizer(&dataExport, cleanupFinalizer)
		return true, c.client.Update(ctx, &dataExport)
	}

	switch dataExport.Status.Stage {
	case kdmpapi.DataExportStageInitial:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
			if hasSnapshotStage(&dataExport) {
				dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotScheduled
			}
			return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// ensure srd/dst volumes are available
		if err := c.checkClaims(&dataExport); err != nil {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusFailed, err.Error())
		}

		return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	// TODO: 'merge' scheduled&inProgress stages
	case kdmpapi.DataExportStageSnapshotScheduled:
		return c.stageSnapshotScheduled(ctx, snapshotter, &dataExport)
	case kdmpapi.DataExportStageSnapshotInProgress:
		return c.stageSnapshotInProgress(ctx, snapshotter, &dataExport)
	case kdmpapi.DataExportStageSnapshotRestore:
		return c.stageSnapshotRestore(ctx, snapshotter, &dataExport)
	case kdmpapi.DataExportStageTransferScheduled:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageTransferInProgress
			return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		srcPVC := dataExport.Spec.Source.PersistentVolumeClaim.Name

		// use snapshot pvc in the dst namespace if it's available
		if dataExport.Status.SnapshotPVCName != "" {
			srcPVC = dataExport.Status.SnapshotPVCName
		}

		// start data transfer
		id, err := driver.StartJob(
			drivers.WithSourcePVC(srcPVC),
			drivers.WithDestinationPVC(dataExport.Spec.Destination.PersistentVolumeClaim.GetName()),
			drivers.WithNamespace(dataExport.Spec.Destination.PersistentVolumeClaim.GetNamespace()),
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
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			return false, nil
		}

		if err := c.cleanUp(driver, snapshotter, dataExport); err != nil {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusFailed, err.Error())
		}

		return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	}

	return false, nil
}

func (c *Controller) stageSnapshotScheduled(ctx context.Context, snapshotter snapshots.Driver, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotInProgress
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	if snapshotter == nil {
		return false, fmt.Errorf("snapshot driver is nil")
	}

	srcPVC, dstPVC := dataExport.Spec.Source.PersistentVolumeClaim, dataExport.Spec.Destination.PersistentVolumeClaim
	name, namespace, err := snapshotter.CreateSnapshot(
		snapshots.PVCName(srcPVC.Name),
		snapshots.PVCNamespace(srcPVC.Namespace),
		snapshots.RestoreNamespaces(dstPVC.Namespace),
		snapshots.SnapshotClassName(dataExport.Spec.SnapshotStorageClass),
	)
	if err != nil {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, err.Error())
	}

	dataExport.Status.SnapshotID = name
	dataExport.Status.SnapshotNamespace = namespace
	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
}

func (c *Controller) stageSnapshotInProgress(ctx context.Context, snapshotter snapshots.Driver, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotRestore
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	if snapshotter == nil {
		return false, fmt.Errorf("snapshot driver is nil")
	}

	srcPvc := dataExport.Spec.Source.PersistentVolumeClaim
	status, err := snapshotter.SnapshotStatus(dataExport.Status.SnapshotID, srcPvc.Namespace)
	if err != nil {
		// TODO: use status 'unknown'?
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, err.Error())
	}

	if status == snapshots.StatusFailed {
		// TODO: pass a reason
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, "")
	}

	if status != snapshots.StatusReady {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, "")
	}

	return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
}

func (c *Controller) stageSnapshotRestore(ctx context.Context, snapshotter snapshots.Driver, dataExport *kdmpapi.DataExport) (bool, error) {
	if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
		// set to the next stage
		dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
	}

	if snapshotter == nil {
		return false, fmt.Errorf("snapshot driver is nil")
	}

	pvc, err := c.restoreSnapshot(ctx, snapshotter, dataExport)
	if err != nil {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, err.Error())
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, fmt.Sprintf("snapshot pvc phase is %q", pvc.Status.Phase))
	}

	dataExport.Status.SnapshotPVCName = pvc.Name
	dataExport.Status.SnapshotPVCNamespace = pvc.Namespace
	return true, c.updateStatus(dataExport, kdmpapi.DataExportStatusSuccessful, "")
}

func (c *Controller) cleanUp(driver drivers.Interface, snapshotter snapshots.Driver, de kdmpapi.DataExport) error {
	if driver == nil {
		return fmt.Errorf("driver is nil")
	}

	if snapshotter == nil {
		return fmt.Errorf("snapshot driver is nil")
	}

	if hasSnapshotStage(&de) {
		if de.Status.SnapshotID != "" && de.Status.SnapshotNamespace != "" {
			if err := snapshotter.DeleteSnapshot(de.Status.SnapshotID, de.Status.SnapshotNamespace); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("delete %s/%s snapshot: %s", de.Status.SnapshotNamespace, de.Status.SnapshotID, err)
			}
		}

		if de.Status.SnapshotPVCName != "" && de.Status.SnapshotPVCNamespace != "" {
			if err := core.Instance().DeletePersistentVolumeClaim(de.Status.SnapshotPVCName, de.Status.SnapshotPVCNamespace); err != nil && !errors.IsNotFound(err) {
				return fmt.Errorf("delete %s/%s pvc: %s", de.Status.SnapshotPVCNamespace, de.Status.SnapshotPVCName, err)
			}
		}
	}

	if de.Status.TransferID != "" {
		if err := driver.DeleteJob(de.Status.TransferID); err != nil {
			return fmt.Errorf("delete %s job: %s", de.Status.TransferID, err)
		}
	}

	return nil
}

func (c *Controller) updateStatus(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, errMsg string) error {
	if isStatusEqual(de, status, errMsg) {
		return nil
	}
	return c.client.Update(context.TODO(), setStatus(de, status, errMsg))
}

func (c *Controller) restoreSnapshot(ctx context.Context, snapshotter snapshots.Driver, de *kdmpapi.DataExport) (*corev1.PersistentVolumeClaim, error) {
	if snapshotter == nil {
		return nil, fmt.Errorf("snapshot driver is nil")
	}

	srcPvc := de.Spec.Source.PersistentVolumeClaim
	srcPvc, err := core.Instance().GetPersistentVolumeClaim(srcPvc.Name, srcPvc.Namespace)
	if err != nil {
		return nil, err
	}

	restoreSpec := corev1.PersistentVolumeClaimSpec{
		StorageClassName: srcPvc.Spec.StorageClassName,
		AccessModes:      srcPvc.Spec.AccessModes,
		Resources:        srcPvc.Spec.Resources,
	}
	pvc, err := snapshotter.RestoreVolumeClaim(
		snapshots.Name(de.Status.SnapshotID),
		snapshots.Namespace(de.Status.SnapshotNamespace),
		snapshots.PVCName(toSnapshotPVCName(srcPvc.Name)),
		snapshots.PVCNamespace(de.Spec.Destination.PersistentVolumeClaim.Namespace),
		snapshots.PVCSpec(restoreSpec),
	)
	if err != nil {
		return nil, fmt.Errorf("restore pvc from %s snapshot: %s", de.Status.SnapshotID, err)
	}

	de.Status.SnapshotPVCName = pvc.Name
	de.Status.SnapshotPVCNamespace = pvc.Namespace
	return pvc, nil
}

func (c *Controller) checkClaims(de *kdmpapi.DataExport) error {
	srcPVC := de.Spec.Source.PersistentVolumeClaim
	if err := c.ensureUnmountedPVC(srcPVC.Name, srcPVC.Namespace, de.Name); err != nil {
		return fmt.Errorf("source pvc: %s/%s: %v", srcPVC.Namespace, srcPVC.Name, err)
	}

	dstPVC := de.Spec.Destination.PersistentVolumeClaim
	// TODO: use size of src pvc if not provided
	// TODO: create pvc on the DataTransfer stage
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
	// TODO: as there is snapshot stage this can be deleted. check just destination pvc?
	//pods, err := getMountPods(pvc.Name, pvc.Namespace)
	//if err != nil {
	//	return fmt.Errorf("get mounted pods: %v", err)
	//}
	//mounted := make([]corev1.Pod, 0)
	//for _, pod := range pods {
	//	// pvc is mounted to pod created for this volume
	//	if pod.Labels[LabelControllerName] == dataExportName {
	//		continue
	//	}
	//	mounted = append(mounted, pod)
	//}
	//if len(mounted) > 0 {
	//	return fmt.Errorf("mounted to %v pods", toPodNames(pods))
	//}

	return nil
}

//func getMountPods(pvcName, namespace string) ([]corev1.Pod, error) {
//	return core.Instance().GetPodsUsingPVC(pvcName, namespace)
//}
//
//func toPodNames(objs []corev1.Pod) []string {
//	out := make([]string, 0)
//	for _, o := range objs {
//		out = append(out, o.Name)
//	}
//	return out
//}

func toSnapshotPVCName(name string) string {
	return fmt.Sprintf("snap-%s", name)
}

func hasSnapshotStage(de *kdmpapi.DataExport) bool {
	// TODO: ckech src/dst namespace and pvc status (mounted?)
	return de.Spec.SnapshotStorageClass != ""
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
