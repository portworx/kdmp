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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
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

	dataExport := in.DeepCopy()

	// set the initial stage
	if dataExport.Status.Stage == "" {
		dataExport.Status.Stage = kdmpapi.DataExportStageInitial
		dataExport.Status.Status = kdmpapi.DataExportStatusInitial
		return true, c.client.Update(ctx, dataExport)
	}
	// TODO: set defaults
	if dataExport.Spec.Type == "" {
		dataExport.Spec.Type = drivers.Rsync
	}

	// TODO: validate DataExport resource & update status?
	driver, err := driversinstance.Get(string(dataExport.Spec.Type))
	if err != nil {
		return false, err
	}

	snapshotter, err := snapshotsinstance.Get(snapshots.ExternalStorage)
	if err != nil {
		return false, fmt.Errorf("get snapshotter for a storage provider: %s", err)
	}

	if dataExport.DeletionTimestamp != nil {
		if !controllers.ContainsFinalizer(dataExport, cleanupFinalizer) {
			return false, nil
		}

		if err = c.cleanUp(driver, snapshotter, dataExport); err != nil {
			return true, fmt.Errorf("%s: cleanup: %s", reflect.TypeOf(dataExport), err)
		}

		controllers.RemoveFinalizer(dataExport, cleanupFinalizer)
		return true, c.client.Update(ctx, dataExport)
	}

	switch dataExport.Status.Stage {
	case kdmpapi.DataExportStageInitial:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageTransferScheduled
			if hasSnapshotStage(dataExport) {
				dataExport.Status.Stage = kdmpapi.DataExportStageSnapshotScheduled
			}
			return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// ensure srd/dst volumes are available
		if err := c.checkClaims(dataExport); err != nil {
			msg := fmt.Sprintf("failed to check volume claims: %s", err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}

		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	// TODO: 'merge' scheduled&inProgress&restore stages
	case kdmpapi.DataExportStageSnapshotScheduled:
		return c.stageSnapshotScheduled(ctx, snapshotter, dataExport)
	case kdmpapi.DataExportStageSnapshotInProgress:
		return c.stageSnapshotInProgress(ctx, snapshotter, dataExport)
	case kdmpapi.DataExportStageSnapshotRestore:
		return c.stageSnapshotRestore(ctx, snapshotter, dataExport)
	case kdmpapi.DataExportStageTransferScheduled:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageTransferInProgress
			return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		if _, err := c.ensureDestinationPVC(ctx, dataExport); err != nil {
			msg := fmt.Sprintf("destination PVC check failed: %s", err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
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
			msg := fmt.Sprintf("failed to start a data transfer job: %s", err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}

		dataExport.Status.TransferID = id
		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	case kdmpapi.DataExportStageTransferInProgress:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageFinal
			return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// get transfer job status
		progress, err := driver.JobStatus(dataExport.Status.TransferID)
		if err != nil {
			msg := fmt.Sprintf("failed to get %s job status: %s", dataExport.Status.TransferID, err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}
		dataExport.Status.ProgressPercentage = progress

		// transfer in progress
		if !drivers.IsTransferCompleted(progress) {
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, string(progress))
		}

		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	case kdmpapi.DataExportStageFinal:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			return false, nil
		}

		if err := c.cleanUp(driver, snapshotter, dataExport); err != nil {
			msg := fmt.Sprintf("failed to remove resources: %s", err)
			return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
		}

		return true, c.client.Update(ctx, setStatus(dataExport, kdmpapi.DataExportStatusSuccessful, ""))
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
	)
	if err != nil {
		msg := fmt.Sprintf("failed to create a snapshot: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
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
		msg := fmt.Sprintf("failed to get a snapshot status: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
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
		msg := fmt.Sprintf("failed to restore a snapshot: %s", err)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusFailed, msg)
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		msg := fmt.Sprintf("snapshot pvc phase is %q, expected- %q", pvc.Status.Phase, corev1.ClaimBound)
		return false, c.updateStatus(dataExport, kdmpapi.DataExportStatusInProgress, msg)
	}

	dataExport.Status.SnapshotPVCName = pvc.Name
	dataExport.Status.SnapshotPVCNamespace = pvc.Namespace
	return true, c.updateStatus(dataExport, kdmpapi.DataExportStatusSuccessful, "")
}

func (c *Controller) cleanUp(driver drivers.Interface, snapshotter snapshots.Driver, de *kdmpapi.DataExport) error {
	if driver == nil {
		return fmt.Errorf("driver is nil")
	}
	if snapshotter == nil {
		return fmt.Errorf("snapshot driver is nil")
	}

	if hasSnapshotStage(de) {
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
		StorageClassName: pointer.StringPtr(de.Spec.SnapshotStorageClass),
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
	if srcPVC == nil {
		return fmt.Errorf("source pvc should be provided")
	}
	// ignore a check for mounted pods if a source pvc has a snapshot (data will be copied from the snapshot)
	src, err := c.checkPVC(srcPVC, !hasSnapshotStage(de))
	if err != nil {
		return fmt.Errorf("source pvc: %v", err)
	}
	srcPVC = src

	dstPVC := de.Spec.Destination.PersistentVolumeClaim
	if dstPVC == nil {
		return fmt.Errorf("destination pvc should be provided")
	}
	dstPVCexits := true
	dst, err := c.checkPVC(dstPVC, true)
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("destination pvc: %v", err)
		}

		// check provided pvc spec if destination pvc doesn't exist
		if err = isValidDestinationSpec(dstPVC.Spec); err != nil {
			return fmt.Errorf("destination pvc: %v", err)
		}

		dstPVCexits = false
	}
	if dst != nil {
		dstPVC = dst
	}

	srcReq := srcPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	dstReq := dstPVC.Spec.Resources.Requests[corev1.ResourceStorage]
	// dstReq < srcReq
	if dstPVCexits && dstReq.Cmp(srcReq) == -1 {
		return fmt.Errorf("size of the destination pvc (%s) is less than of the source one (%s)", dstReq.String(), srcReq.String())
	}

	return nil
}

func (c *Controller) checkPVC(in *corev1.PersistentVolumeClaim, checkMounts bool) (*corev1.PersistentVolumeClaim, error) {
	if in.Name == "" || in.Namespace == "" {
		return nil, fmt.Errorf("name and namespace should be provided")
	}
	pvc, err := core.Instance().GetPersistentVolumeClaim(in.Name, in.Namespace)
	if err != nil {
		return nil, err
	}
	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, fmt.Errorf("status: expected %s, got %s", corev1.ClaimBound, pvc.Status.Phase)
	}

	if checkMounts {
		pods, err := core.Instance().GetPodsUsingPVC(pvc.Name, pvc.Namespace)
		if err != nil {
			return nil, fmt.Errorf("get mounted pods: %v", err)
		}
		if len(pods) > 0 {
			return nil, fmt.Errorf("mounted to %v pods", toPodNames(pods))
		}
	}

	return pvc, nil
}

func (c *Controller) ensureDestinationPVC(ctx context.Context, de *kdmpapi.DataExport) (*corev1.PersistentVolumeClaim, error) {
	dst := de.Spec.Destination.PersistentVolumeClaim
	dstPVC, err := core.Instance().GetPersistentVolumeClaim(dst.Name, dst.Namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			return nil, fmt.Errorf("get destination pvc: %s", err)
		}
		// create a volume otherwise
	}
	if err == nil {
		return dstPVC, nil
	}

	// Create a volume claim if it's not found
	spec := dst.Spec
	if err := isValidClaimSpec(spec); err != nil {
		// use source spec parameters if destination on is invalid
		src := de.Spec.Source.PersistentVolumeClaim
		srcPVC, err := core.Instance().GetPersistentVolumeClaim(src.Name, src.Namespace)
		if err != nil {
			return nil, fmt.Errorf("get source pvc: %s", err)
		}
		spec = mergeSpec(spec, srcPVC.Spec)
	}

	// TODO: copy annotations and labels?
	dstPVC = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dst.Name,
			Namespace: dst.Namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      spec.AccessModes,
			Resources:        spec.Resources,
			StorageClassName: spec.StorageClassName,
			VolumeMode:       spec.VolumeMode,
		},
	}
	return core.Instance().CreatePersistentVolumeClaim(dstPVC)
}

func toPodNames(objs []corev1.Pod) []string {
	out := make([]string, 0)
	for _, o := range objs {
		out = append(out, o.Name)
	}
	return out
}

func toSnapshotPVCName(name string) string {
	return fmt.Sprintf("snap-%s", name)
}

func hasSnapshotStage(de *kdmpapi.DataExport) bool {
	return de.Spec.SnapshotStorageClass != ""
}

func isValidClaimSpec(spec corev1.PersistentVolumeClaimSpec) error {
	if spec.StorageClassName == nil || *spec.StorageClassName == "" {
		return fmt.Errorf("storageClassName should be set")
	}
	if spec.Resources.Requests == nil {
		return fmt.Errorf("requests should be set")
	}
	if len(spec.AccessModes) == 0 {
		return fmt.Errorf("accessModes should be set")
	}
	return nil
}

func isValidDestinationSpec(spec corev1.PersistentVolumeClaimSpec) error {
	if spec.StorageClassName == nil || *spec.StorageClassName == "" {
		return fmt.Errorf("storageClassName should be set")
	}
	return nil
}

func mergeSpec(dst, src corev1.PersistentVolumeClaimSpec) corev1.PersistentVolumeClaimSpec {
	if dst.StorageClassName == nil {
		dst.StorageClassName = src.StorageClassName
	}
	if dst.Resources.Requests == nil {
		dst.Resources.Requests = src.Resources.Requests
	}
	if dst.Resources.Limits == nil {
		dst.Resources.Limits = src.Resources.Limits
	}
	if dst.AccessModes == nil {
		dst.AccessModes = src.AccessModes
	}
	if dst.VolumeMode == nil {
		dst.VolumeMode = src.VolumeMode
	}
	return dst
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
