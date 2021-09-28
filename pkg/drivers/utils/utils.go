package utils

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"
	"os"
	"strings"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/portworx/kdmp/pkg/version"
	"github.com/portworx/sched-ops/k8s/core"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	defaultPXNamespace = "kube-system"
	kdmpConfig         = "kdmp-config"
)

// NamespacedName returns a name in form "<namespace>/<name>".
func NamespacedName(namespace, name string) string {
	v := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	return v.String()
}

// ParseJobID parses input string as namespaced name ("<namespace>/<name>").
func ParseJobID(id string) (namespace, name string, err error) {
	v := strings.SplitN(id, string(types.Separator), 2)
	if len(v) != 2 {
		return "", "", fmt.Errorf("invalid job id")
	}
	return v[0], v[1], nil
}

// IsJobCompleted checks if a kubernetes job is completed.
func IsJobCompleted(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// IsJobFailed checks if a kubernetes job is failed.
func IsJobFailed(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// ToJobStatus returns a job status for provided parameters.
func ToJobStatus(progress float64, errMsg string) *drivers.JobStatus {
	if len(errMsg) > 0 {
		return &drivers.JobStatus{
			State:  drivers.JobStateFailed,
			Reason: errMsg,
		}
	}

	if drivers.IsTransferCompleted(progress) {
		return &drivers.JobStatus{
			State:            drivers.JobStateCompleted,
			ProgressPercents: progress,
		}
	}

	return &drivers.JobStatus{
		State:            drivers.JobStateInProgress,
		ProgressPercents: progress,
	}
}

// GetConfigValue read configmap and return the value of the requested parameter
func GetConfigValue(key string) string {
	configMap, err := core.Instance().GetConfigMap(
		kdmpConfig,
		defaultPXNamespace,
	)
	if err != nil {
		log.Errorf("Failed to read configmap.")
		if os.Getenv(key) != "" {
			return os.Getenv(key)
		} else {
			return ""
		}

	}
	return configMap.Data[key]
}

// ResticExecutorImage returns a docker image that contains resticexecutor binary.
func ResticExecutorImage() string {
	if customImage := strings.TrimSpace(GetConfigValue(drivers.ResticExecutorImageKey)); customImage != "" {
		return customImage
	}
	// use a versioned docker image
	return strings.Join([]string{drivers.ResticExecutorImage, version.Get().GitVersion}, ":")
}

// ResticExecutorImageSecret returns an image pull secret for the resticexecutor image.
func ResticExecutorImageSecret() string {
	return strings.TrimSpace(GetConfigValue(drivers.ResticExecutorImageSecretKey))
}

// KopiaExecutorImage returns a docker image that contains kopiaexecutor binary.
func KopiaExecutorImage() string {
	if customImage := strings.TrimSpace(GetConfigValue(drivers.KopiaExecutorImageKey)); customImage != "" {
		return customImage
	}
	// use a versioned docker image
	return strings.Join([]string{drivers.KopiaExecutorImage, version.Get().GitVersion}, ":")
}

// KopiaExecutorImageSecret returns an image pull secret for the resticexecutor image.
func KopiaExecutorImageSecret() string {
	return strings.TrimSpace(GetConfigValue(drivers.KopiaExecutorImageSecretKey))
}

// RsyncImage returns a docker image that contains rsync binary.
func RsyncImage() string {
	if customImage := strings.TrimSpace(GetConfigValue(drivers.RsyncImageKey)); customImage != "" {
		return customImage
	}
	return drivers.RsyncImage
}

// RsyncImageSecret returns an image pull secret for the rsync image.
func RsyncImageSecret() string {
	return strings.TrimSpace(GetConfigValue(drivers.RsyncImageSecretKey))
}

// RsyncCommandFlags allows to change rsync command flags.
func RsyncCommandFlags() string {
	return strings.TrimSpace(GetConfigValue(drivers.RsyncFlags))
}

// RsyncOpenshiftSCC is used to set a custom openshift security context constraints for a rsync deployment.
func RsyncOpenshiftSCC() string {
	return strings.TrimSpace(GetConfigValue(drivers.RsyncOpenshiftSCC))
}

// ToImagePullSecret converts a secret name to the ImagePullSecret struct.
func ToImagePullSecret(name string) []corev1.LocalObjectReference {
	if name == "" {
		return nil
	}
	return []corev1.LocalObjectReference{
		{
			Name: name,
		},
	}

}

// JobResourceRequirements returns JobResourceRequirements for the executor container.
func JobResourceRequirements() (corev1.ResourceRequirements, error) {
	requestCPU := drivers.DefaultResticExecutorRequestCPU
	if customRequestCPU := GetConfigValue(drivers.ResticExecutorRequestCPU); customRequestCPU != "" {
		requestCPU = customRequestCPU
	}
	requestMem := drivers.DefaultResticExecutorRequestMemory
	if customRequestMemory := GetConfigValue(drivers.ResticExecutorRequestMemory); customRequestMemory != "" {
		requestMem = customRequestMemory
	}
	limitCPU := drivers.DefaultResticExecutorLimitCPU
	if customLimitCPU := GetConfigValue(drivers.ResticExecutorLimitCPU); customLimitCPU != "" {
		limitCPU = customLimitCPU
	}
	limitMem := drivers.DefaultResticExecutorLimitMemory
	if customLimitMemory := GetConfigValue(drivers.ResticExecutorLimitMemory); customLimitMemory != "" {
		limitMem = customLimitMemory
	}
	return toResourceRequirements(requestCPU, requestMem, limitCPU, limitMem)
}

// RsyncResourceRequirements returns ResourceRequirements for the rsync container.
func RsyncResourceRequirements() (corev1.ResourceRequirements, error) {
	requestCPU := drivers.DefaultRsyncRequestCPU
	if customRequestCPU := GetConfigValue(drivers.RsyncRequestCPU); customRequestCPU != "" {
		requestCPU = customRequestCPU
	}
	requestMem := drivers.DefaultRsyncRequestMemory
	if customRequestMemory := GetConfigValue(drivers.RsyncRequestMemory); customRequestMemory != "" {
		requestMem = customRequestMemory
	}
	limitCPU := drivers.DefaultRsyncLimitCPU
	if customLimitCPU := GetConfigValue(drivers.RsyncLimitCPU); customLimitCPU != "" {
		limitCPU = customLimitCPU
	}
	limitMem := drivers.DefaultRsyncLimitMemory
	if customLimitMemory := GetConfigValue(drivers.RsyncLimitMemory); customLimitMemory != "" {
		limitMem = customLimitMemory
	}
	return toResourceRequirements(requestCPU, requestMem, limitCPU, limitMem)
}

func toResourceRequirements(requestCPU, requestMem, limitCPU, limitMem string) (corev1.ResourceRequirements, error) {
	requestCPUQ, err := resource.ParseQuantity(requestCPU)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q requestCPU: %s", requestCPU, err)
	}
	requestMemQ, err := resource.ParseQuantity(requestMem)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q requestMemory: %s", requestMem, err)
	}
	limitCPUQ, err := resource.ParseQuantity(limitCPU)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q limitCPU: %s", limitCPU, err)
	}
	limitMemQ, err := resource.ParseQuantity(limitMem)
	if err != nil {
		return corev1.ResourceRequirements{}, fmt.Errorf("failed to parse %q limitMemory: %s", limitMem, err)
	}
	return corev1.ResourceRequirements{
		Requests: map[corev1.ResourceName]resource.Quantity{
			corev1.ResourceCPU:    requestCPUQ,
			corev1.ResourceMemory: requestMemQ,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    limitCPUQ,
			corev1.ResourceMemory: limitMemQ,
		},
	}, nil
}
