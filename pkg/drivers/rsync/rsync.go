package rsync

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/sched-ops/k8s/batch"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Rsync driver label names/keys.
const (
	LabelDriver = "kdmp.portworx.com/driver-name"
)

// Driver is a rsync implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.Rsync
}

// StartJob creates a job for data transfer between volumes.
func (d Driver) StartJob(opts ...drivers.JobOption) (id string, err error) {
	o := drivers.JobOpts{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", err
			}
		}
	}

	rsyncJob, err := jobFor(o.SourcePVCName, o.DestinationPVCName, o.Namespace, o.Labels)
	if err != nil {
		return "", err
	}
	job, err := batch.Instance().CreateJob(rsyncJob)
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", err
	}

	return namespacedName(job.Namespace, job.Name), nil
}

// DeleteJob stops data transfer between volumes.
func (d Driver) DeleteJob(id string) error {
	namespace, name, err := parseJobID(id)
	if err != nil {
		return err
	}

	return batch.Instance().DeleteJob(name, namespace)
}

// JobStatus returns a progress status for a data transfer.
func (d Driver) JobStatus(id string) (progress int, err error) {
	namespace, name, err := parseJobID(id)
	if err != nil {
		return -1, err
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		return -1, err
	}
	if !isJobCompleted(job) {
		// TODO: update progress
		return 0, nil
	}

	return drivers.TransferProgressCompleted, nil
}

func jobFor(srcVol, dstVol, namespace string, labels map[string]string) (*batchv1.Job, error) {
	uuidv4, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("build job id: %s", err)
	}

	labels = addJobLabels(labels)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toJobName(uuidv4.String()[:6]),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
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
									ClaimName: srcVol,
								},
							},
						},
						{
							Name: "dst-vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: dstVol,
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func namespacedName(namespace, name string) string {
	v := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	return v.String()
}

func parseJobID(id string) (namespace, name string, err error) {
	v := strings.Split(id, string(types.Separator))
	if len(v) != 2 {
		return "", "", fmt.Errorf("invalid job id")
	}
	return v[0], v[1], nil
}

func toJobName(id string) string {
	return fmt.Sprintf("import-rsync-%s", id)
}

func addJobLabels(labels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[LabelDriver] = drivers.Rsync
	return labels
}

func isJobCompleted(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
