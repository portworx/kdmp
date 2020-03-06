package dataexport

import (
	"context"
	"fmt"
	"reflect"

	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
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

func (c *Controller) sync(ctx context.Context, de *kdmpapi.DataExport) error {
	if de == nil {
		return nil
	}

	new := &kdmpapi.DataExport{}
	reflect.Copy(reflect.ValueOf(new), reflect.ValueOf(de))

	logrus.Debugf("handling %s/%s DataExport", new.Namespace, new.Name)

	if new.DeletionTimestamp != nil {
		return c.deleteJob(toJobName(new.Name), new.Namespace)
	}

	if err := c.stageInitial(new); err != nil {
		return fmt.Errorf("stage initial: %v", err)
	}

	if err := c.stageTransfer(new); err != nil {
		return fmt.Errorf("stage transfer: %v", err)
	}

	if err := c.stageFinal(new); err != nil {
		return fmt.Errorf("stage final: %v", err)
	}

	if !reflect.DeepEqual(new, de) {
		return c.client.Update(ctx, new)
	}

	return nil
}

func (c *Controller) stageInitial(de *kdmpapi.DataExport) error {
	if de.Status.Stage != "" {
		return nil
	}

	de.Status.Stage = kdmpapi.DataExportStageInitial
	de.Status.Status = kdmpapi.DataExportStatusInitial

	// ensure srd/dst volumes are available
	if err := c.checkClaims(de); err != nil {
		return c.updateStatus(de, err.Error())
	}

	return nil
}

func (c *Controller) stageTransfer(de *kdmpapi.DataExport) error {
	// Stage: transfer in progress
	de.Status.Stage = kdmpapi.DataExportStageTransferScheduled

	// check if a rsync job is created
	viJob, err := c.getJob(toJobName(de.Name), de.Namespace)
	if err != nil {
		if !errors.IsNotFound(err) {
			return c.updateStatus(de, err.Error())
		}

		// create a job if it's not exist
		viJob = jobFrom(de)
		if err = c.client.Create(context.TODO(), viJob); err != nil {
			return c.updateStatus(de, err.Error())
		}
	}

	// transfer in progress
	if !isJobCompleted(viJob) {
		de.Status.Stage = kdmpapi.DataExportStageTransferInProgress
		de.Status.Status = kdmpapi.DataExportStatusInProgress
		// TODO: update data transfer progress percentage

		return c.updateStatus(de, "")
	}

	return nil
}

func (c *Controller) stageFinal(de *kdmpapi.DataExport) error {
	de.Status.Stage = kdmpapi.DataExportStageFinal
	// TODO: is it required to remove the rsync job? (it contains data transfer logs)

	de.Status.Status = kdmpapi.DataExportStatusSuccessful
	return nil
}

func (c *Controller) updateStatus(de *kdmpapi.DataExport, errMsg string) error {
	if errMsg != "" {
		de.Status.Status = kdmpapi.DataExportStatusFailed
		de.Status.Reason = errMsg
	}
	return c.client.Update(context.TODO(), de)
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

func (c *Controller) ensureUnmountedPVC(name, namespace, viName string) error {
	pvc, err := c.getPVC(name, namespace)
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
		if pod.Labels[LabelControllerName] == viName {
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

func jobFrom(vi *kdmpapi.DataExport) *batchv1.Job {
	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Job",
			APIVersion: "batch/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            toJobName(vi.Name),
			Namespace:       vi.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(vi, controllerKind)},
			Labels:          jobLabels(vi.Name),
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:     jobLabels(vi.Name),
					Finalizers: nil,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyOnFailure,
					Containers: []corev1.Container{
						{
							Name:    "rsync",
							Image:   "eeacms/rsync",
							Command: []string{"/bin/sh", "-c", "rsync -avz /src/ /dst"},
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
									ClaimName: vi.Spec.Source.PersistentVolumeClaim.Name,
								},
							},
						},
						{
							Name: "dst-vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: vi.Spec.Destination.PersistentVolumeClaim.Name,
								},
							},
						},
					},
				},
			},
		},
	}
}
