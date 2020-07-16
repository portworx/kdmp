package resticbackup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/portworx/kdmp/pkg/drivers"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/batch"
	coreops "github.com/portworx/sched-ops/k8s/core"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	secretKey   = "secret"
	secretValue = "resticsecret"
	secretMount = "/tmp/resticsecret"
)

// Driver is a rsync implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.ResticBackup
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

	if err := d.validate(o); err != nil {
		return "", err
	}

	resticSecretName := toJobName(o.SourcePVCName)

	if _, err := coreops.Instance().CreateSecret(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resticSecretName,
			Namespace: o.Namespace,
		},
		StringData: map[string]string{
			secretKey: secretValue,
		},
	}); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create a secret for a restic password: %s", err)
	}

	rsyncJob, err := jobFor(
		o.Namespace,
		o.SourcePVCName,
		o.BackupLocationName,
		o.BackupLocationNamespace,
		resticSecretName,
		o.Labels)
	if err != nil {
		return "", err
	}
	if _, err = batch.Instance().CreateJob(rsyncJob); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", err
	}

	return namespacedName(rsyncJob.Namespace, rsyncJob.Name), nil
}

// DeleteJob stops data transfer between volumes.
func (d Driver) DeleteJob(id string) error {
	namespace, name, err := parseJobID(id)
	if err != nil {
		return err
	}

	if err := coreops.Instance().DeleteSecret(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
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
	if isJobFailed(job) {
		return -1, fmt.Errorf("transfer is failed, check %s/%s job for details", namespace, name)
	}

	// restic executor updates a volumebackup object with a progress details
	vb, err := kdmpops.Instance().GetVolumeBackup(name, namespace)
	if err != nil {
		return -1, err
	}

	if len(vb.Status.LastKnownError) > 0 {
		return -1, errors.New(vb.Status.LastKnownError)
	}

	return int(vb.Status.ProgressPercentage), nil
}

func (d Driver) validate(o drivers.JobOpts) error {
	if o.BackupLocationName == "" {
		return fmt.Errorf("backuplocation name should be set")
	}
	if o.BackupLocationNamespace == "" {
		return fmt.Errorf("backuplocation namespace should be set")
	}
	return nil
}

func jobFor(
	namespace,
	pvcName,
	backuplocationName,
	backuplocationNamespace,
	secretName string,
	labels map[string]string) (*batchv1.Job, error) {
	labels = addJobLabels(labels)

	// create role & rolebinding
	serviceAccountName := "kdmp-admin" // TODO: create a service account - pod requires permissions to get backuplocation and update volumebackup

	image := "portworx/resticexecutor"
	if customImage := strings.TrimSpace(os.Getenv("RESTICEXECUTOR_IMAGE")); customImage != "" {
		image = customImage
	}

	cmd := strings.Join([]string{
		"cd",
		"/data;",
		"/resticexecutor",
		"backup",
		"--backup-location",
		backuplocationName,
		"--namespace",
		backuplocationNamespace,
		"--volume-backup-name",
		toJobName(pvcName),
		"--repository",
		toRepoName(pvcName, namespace),
		"--secret-file-path",
		filepath.Join(secretMount, secretKey),
		"--source-path",
		".",
	}, " ")

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toJobName(pvcName),
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy:      corev1.RestartPolicyOnFailure,
					ServiceAccountName: serviceAccountName,
					Containers: []corev1.Container{
						{
							Name:  "resticexecutor",
							Image: image,
							Command: []string{
								"/bin/sh",
								"-x",
								"-c",
								cmd,
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "vol",
									MountPath: "/data",
								},
								{
									Name:      "secret",
									MountPath: secretMount,
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "vol",
							VolumeSource: corev1.VolumeSource{
								PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
									ClaimName: pvcName,
								},
							},
						},
						{
							Name: "secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: secretName,
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
	return fmt.Sprintf("resticbackup-%s", id)
}

func toRepoName(pvcName, pvcNamespace string) string {
	return fmt.Sprintf("restic/%s-%s", pvcNamespace, pvcName)
}

func addJobLabels(labels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.ResticBackup
	return labels
}

func isJobFailed(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}
