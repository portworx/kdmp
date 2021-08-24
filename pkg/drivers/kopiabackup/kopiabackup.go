package kopiabackup

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/sched-ops/k8s/batch"
	coreops "github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	skipResourceAnnotation = "stork.libopenstorage.org/skip-resource"
)

// Driver is a resticbackup implementation of the data export interface.
type Driver struct{}

// Name returns a name of the driver.
func (d Driver) Name() string {
	return drivers.KopiaBackup
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
		logrus.Infof("line 40 StartJob err: %v", err)
		return "", err
	}

	jobName := toJobName(o.DataExportName, o.Namespace, o.SourcePVCName)
	logrus.Infof("line 63 startjonb jobname: %v", jobName)
	job, err := buildJob(jobName, o)
	if err != nil {
		logrus.Infof("line 66 StartJob err: %v", err)
		return "", err
	}
	if _, err = batch.Instance().CreateJob(job); err != nil && !apierrors.IsAlreadyExists(err) {
		logrus.Infof("line 70 StartJob err: %v", err)
		return "", err
	}
	logrus.Infof("line 73 StartJob")
	return utils.NamespacedName(job.Namespace, job.Name), nil
}

// DeleteJob stops data transfer between volumes.
func (d Driver) DeleteJob(id string) error {
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return err
	}

	if err := coreops.Instance().DeleteSecret(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if err := utils.CleanServiceAccount(name, namespace); err != nil {
		return err
	}

	if err = batch.Instance().DeleteJob(name, namespace); err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	return nil
}

// JobStatus returns a progress status for a data transfer.
func (d Driver) JobStatus(id string) (*drivers.JobStatus, error) {
	namespace, name, err := utils.ParseJobID(id)
	if err != nil {
		return utils.ToJobStatus(0, err.Error()), nil
	}

	job, err := batch.Instance().GetJob(name, namespace)
	if err != nil {
		return nil, err
	}
	if utils.IsJobFailed(job) {
		errMsg := fmt.Sprintf("check %s/%s job for details: %s", namespace, name, drivers.ErrJobFailed)
		return utils.ToJobStatus(0, errMsg), nil
	}

	// restic executor updates a volumebackup object with a progress details
	vb, err := kdmpops.Instance().GetVolumeBackup(name, namespace)
	if err != nil {
		return nil, err
	}

	return utils.ToJobStatus(vb.Status.ProgressPercentage, vb.Status.LastKnownError), nil
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
	jobName,
	namespace,
	pvcName,
	backuplocationName string,
	//backuplocationNamespace string,
	resources corev1.ResourceRequirements,
	labels map[string]string) (*batchv1.Job, error) {
	backupName := jobName

	labels = addJobLabels(labels)

	cmd := strings.Join([]string{
		"/kopiaexecutor",
		"backup",
		"--volume-backup-name",
		backupName,
		"--repository",
		toRepoName(pvcName, namespace),
		"--credentials",
		backuplocationName,
		"--secret-file-path",
		filepath.Join(drivers.KopiaSecretMount, drivers.KopiaSecretKey),
		"--source-path",
		"/data",
	}, " ")
	logrus.Infof("line 150")
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
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
					ImagePullSecrets:   utils.ToImagePullSecret(utils.KopiaExecutorImageSecret()),
					ServiceAccountName: jobName,
					Containers: []corev1.Container{
						{
							Name:            "kopiaexecutor",
							Image:           utils.KopiaExecutorImage(),
							ImagePullPolicy: corev1.PullAlways,
							Command: []string{
								"/bin/sh",
								"-x",
								"-c",
								cmd,
							},
							Resources: resources,
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "vol",
									MountPath: "/data",
								},
								{
									Name:      "cred-secret",
									MountPath: drivers.KopiaCredSecretMount,
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
							Name: "cred-secret",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: backuplocationName,
								},
							},
						},
					},
				},
			},
		},
	}, nil
}

func toJobName(dataExportName, ns, pvcName string) string {
	return fmt.Sprintf("%s-%s-%s", dataExportName, ns, pvcName)
}

func toRepoName(pvcName, pvcNamespace string) string {
	return fmt.Sprintf("%s-%s", pvcNamespace, pvcName)
}

func addJobLabels(labels map[string]string) map[string]string {
	if labels == nil {
		labels = make(map[string]string)
	}

	labels[drivers.DriverNameLabel] = drivers.KopiaBackup
	return labels
}

func buildJob(jobName string, o drivers.JobOpts) (*batchv1.Job, error) {
	resources, err := utils.ResticResourceRequirements()
	if err != nil {
		return nil, err
	}

	if err := utils.SetupServiceAccount(jobName, o.Namespace, roleFor()); err != nil {
		return nil, err
	}
	
	pods, err := coreops.Instance().GetPodsUsingPVC(o.SourcePVCName, o.Namespace)
	if err != nil {
		return nil, err
	}

	// run a "live" backup if a pvc is mounted (mount a kubelet directory with pod volumes)
	if len(pods) > 0 {
		return jobForLiveBackup(
			jobName,
			o.Namespace,
			o.SourcePVCName,
			o.BackupLocationName,
			o.BackupLocationNamespace,
			pods[0],
			resources,
			o.Labels,
		)
	}

	return jobFor(
		jobName,
		o.Namespace,
		o.SourcePVCName,
		o.BackupLocationName,
		//o.BackupLocationNamespace,
		resources,
		o.Labels,
	)
}

// TODO: backuplocations permission will be removed in later changes when we read credentials from secret
func roleFor() *rbacv1.Role {
	return &rbacv1.Role{
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"stork.libopenstorage.org"},
				Resources: []string{"backuplocations"},
				Verbs:     []string{"get", "list"},
			},
			{
				APIGroups: []string{"kdmp.portworx.com"},
				Resources: []string{"volumebackups"},
				Verbs:     []string{rbacv1.VerbAll},
			},
		},
	}
}
