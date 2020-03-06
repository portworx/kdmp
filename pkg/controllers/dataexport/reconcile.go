package dataexport

import (
	"context"
	"fmt"
	"reflect"

	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Data export label names/keys.
const (
	LabelController     = "kdmp.portworx.com/controller"
	LabelControllerName = "controller-name"
)

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = kdmpapi.SchemeGroupVersion.WithKind(reflect.TypeOf(kdmpapi.DataExport{}).Name())

func (c *Controller) sync(ctx context.Context, in *kdmpapi.DataExport) (bool, error) {
	if in == nil {
		return false, nil
	}

	dataExport := *in

	if dataExport.DeletionTimestamp != nil {
		return false, batch.Instance().DeleteJob(toJobName(dataExport.Name), dataExport.Namespace)
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

		// create a rsync job
		err := c.client.Create(context.TODO(), jobFrom(&dataExport))
		if err != nil && !errors.IsAlreadyExists(err) {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusFailed, err.Error())
		}

		return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusSuccessful, ""))
	case kdmpapi.DataExportStageTransferInProgress:
		if dataExport.Status.Status == kdmpapi.DataExportStatusSuccessful {
			// set to the next stage
			dataExport.Status.Stage = kdmpapi.DataExportStageFinal
			return true, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusInitial, ""))
		}

		// check if a rsync job is created
		rsyncJob, err := batch.Instance().GetJob(toJobName(dataExport.Name), dataExport.Namespace)
		if err != nil {
			return false, c.updateStatus(&dataExport, kdmpapi.DataExportStatusFailed, err.Error())
		}

		// transfer in progress
		if !isJobCompleted(rsyncJob) {
			// TODO: update data transfer progress percentage
			if isStatusEqual(&dataExport, kdmpapi.DataExportStatusInProgress, "") {
				return false, nil
			}
			return false, c.client.Update(ctx, setStatus(&dataExport, kdmpapi.DataExportStatusInProgress, ""))
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

func jobLabels(DataExportName string) map[string]string {
	return map[string]string{
		LabelController:     DataExportName,
		LabelControllerName: DataExportName,
	}
}

func toJobName(DataExportName string) string {
	return fmt.Sprintf("job-%s", DataExportName)
}

func isJobCompleted(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func setStatus(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, reason string) *kdmpapi.DataExport {
	de.Status.Status = status
	de.Status.Reason = reason
	return de
}

func isStatusEqual(de *kdmpapi.DataExport, status kdmpapi.DataExportStatus, reason string) bool {
	return de.Status.Status == status && de.Status.Reason == reason
}

func jobFrom(dataExport *kdmpapi.DataExport) *batchv1.Job {
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            toJobName(dataExport.Name),
			Namespace:       dataExport.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(dataExport, controllerKind)},
			Labels:          jobLabels(dataExport.Name),
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:     jobLabels(dataExport.Name),
					Finalizers: nil,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "rsync",
							Image:   "eeacms/rsync",
							Command: []string{"/bin/sh", "-x", "-c", "ls -la /src; ls -la /dst/; rsync -avz /src/ /dst"},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "src-vol",
									MountPath: "/src",
								},
								{
									Name:      "dst-vol",
									MountPath: "/dst",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "src-vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: dataExport.Spec.Source.PersistentVolumeClaim.Name,
								},
							},
						},
						{
							Name: "dst-vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: dataExport.Spec.Destination.PersistentVolumeClaim.Name,
								},
							},
						},
					},
				},
			},
		},
	}
}
