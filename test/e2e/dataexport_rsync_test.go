// +build e2e

package e2e

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/utils"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/pointer"
)

const (
	podContainerImage = "nginx"
	podContainerName  = "runner"
	pvcMountPath      = "/data"
	pvcTestFileName   = "testfile"

	storageClassPortworx = "px-test-sc"
)

func TestVolumeClaimRsync(t *testing.T) {
	cases := []struct {
		name string
		cfg  config
	}{
		{
			name: "MissingSourcePVC",
			cfg: config{
				DataExport: &v1alpha1.DataExport{
					ObjectMeta: metav1.ObjectMeta{
						Name: "no-source-pvc",
					},
					Spec: v1alpha1.DataExportSpec{
						Source: v1alpha1.DataExportObjectReference{
							Name: "no-source-pvc",
						},
					},
				},
				ExpectedStatus: v1alpha1.ExportStatus{
					Stage:  v1alpha1.DataExportStageInitial,
					Status: v1alpha1.DataExportStatusFailed,
				},
			},
		},
		{
			name: "MissingDestinationPVC",
			cfg: config{
				SourcePVC: &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "missing-destination-pvc-src",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.StringPtr(storageClassPortworx),
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
					},
				},
				DataExport: &v1alpha1.DataExport{
					ObjectMeta: metav1.ObjectMeta{
						Name: "no-destination-pvc",
					},
					Spec: v1alpha1.DataExportSpec{
						Source: v1alpha1.DataExportObjectReference{
							Name: "missing-destination-pvc-src",
						},
						Destination: v1alpha1.DataExportObjectReference{
							Name: "no-destination-pvc",
						},
					},
				},
				ExpectedStatus: v1alpha1.ExportStatus{
					Stage:  v1alpha1.DataExportStageInitial,
					Status: v1alpha1.DataExportStatusFailed,
				},
			},
		},
		{
			name: "CreatedDestinationPVC",
			cfg: config{
				SourcePVC: &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "created-destination-pvc-src",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.StringPtr(storageClassPortworx),
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
					},
				},
				DestinationPVC: &corev1.PersistentVolumeClaim{
					ObjectMeta: metav1.ObjectMeta{
						Name: "created-destination-pvc-dst",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: pointer.StringPtr(storageClassPortworx),
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
					},
				},
				DataExport: &v1alpha1.DataExport{
					ObjectMeta: metav1.ObjectMeta{
						Name: "created-destination-pvc",
					},
					Spec: v1alpha1.DataExportSpec{
						Source: v1alpha1.DataExportObjectReference{
							Name: "created-destination-pvc-src",
						},
						Destination: v1alpha1.DataExportObjectReference{
							Name: "created-destination-pvc-dst",
						},
					},
				},
				ExpectedStatus: v1alpha1.ExportStatus{
					Stage:  v1alpha1.DataExportStageFinal,
					Status: v1alpha1.DataExportStatusSuccessful,
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := runNamespaced(tc.cfg); err != nil {
				t.Fatal(err)
			}
		})
	}

}

type config struct {
	SourcePVC      *corev1.PersistentVolumeClaim
	DestinationPVC *corev1.PersistentVolumeClaim
	DataExport     *v1alpha1.DataExport
	ExpectedStatus v1alpha1.ExportStatus
}

func (c config) SetNamespace(ns string) {
	if c.SourcePVC != nil {
		c.SourcePVC.Namespace = ns
	}
	if c.DestinationPVC != nil {
		c.DestinationPVC.Namespace = ns
	}
	if c.DataExport != nil {
		c.DataExport.Namespace = ns
		if c.DataExport.Spec.Source.Namespace == "" {
			c.DataExport.Spec.Source.Namespace = ns
		}
		if c.DataExport.Spec.Destination.Namespace == "" {
			c.DataExport.Spec.Destination.Namespace = ns
		}
	}
}

func runNamespaced(cfg config) error {
	ns, err := core.Instance().CreateNamespace(getNamespaceName(), map[string]string{
		"testset-id": fmt.Sprintf("%d", testsSetID),
	})
	if err != nil {
		return fmt.Errorf("create a namespace: %s", err)
	}

	cfg.SetNamespace(ns.Name)
	if err = runRsyncT(cfg); err != nil {
		return fmt.Errorf("namespace %s: %s", ns.Name, err)
	}

	// keep the namespace in case of test error
	if err = ensureNamespaceDeleted(ns.Name, 2*time.Minute); err != nil {
		return fmt.Errorf("delete a %s namespace: %s", ns, err)
	}

	return nil
}

func runRsyncT(cfg config) error {
	if cfg.DataExport == nil {
		return fmt.Errorf("DataExport resource have to be provided")
	}

	if cfg.SourcePVC != nil {
		err := setupPVC(cfg.SourcePVC)
		if err != nil {
			return err
		}
	}

	if cfg.DestinationPVC != nil {
		pvc, err := core.Instance().CreatePersistentVolumeClaim(cfg.DestinationPVC)
		if err != nil {
			return err
		}
		logrus.Infof("pvc %s/%s has been created", pvc.Namespace, pvc.Name)
	}

	// CreateDataExport
	de, err := utils.Instance().CreateDataExport(cfg.DataExport)
	if err != nil {
		return err
	}

	logrus.Infof("dataexport %s/%s has been created", de.Namespace, de.Name)

	// Wait for a DataExport
	pollFunc := func() (bool, error) {
		de, err := utils.Instance().GetDataExport(de.Name, de.Namespace)
		if err != nil {
			return false, err
		}

		logrus.Infof("dataexport %s/%s: status %s/%s", de.Namespace, de.Name, de.Status.Stage, de.Status.Status)

		// check status
		expected := cfg.ExpectedStatus.Stage == de.Status.Stage && cfg.ExpectedStatus.Status == de.Status.Status
		return expected, nil
	}
	if err = wait.Poll(3*time.Second, 3*time.Minute, pollFunc); err != nil {
		return fmt.Errorf("wait for the %s/%s dataexport status: %s", cfg.ExpectedStatus.Stage, cfg.ExpectedStatus.Status, err)
	}

	if isSuccessful(de) {
		// validate:
		// - dst pvc exists,
		// - dst pvc contains data same as the src pvc.

		dstPVC := cfg.DataExport.Spec.Destination

		logrus.Infof("validate data for a %s/%s pvc ", dstPVC.Namespace, dstPVC.Name)
		pvc, err := core.Instance().GetPersistentVolumeClaim(dstPVC.Name, dstPVC.Namespace)
		if err != nil {
			return fmt.Errorf("get destination pvc: %s", err)
		}

		if err = checkPVC(pvc); err != nil {
			return fmt.Errorf("validate data for a destination pvc: %s", err)
		}

		logrus.Infof("data for a %s/%s pvc has been validated", pvc.Namespace, pvc.Name)
	}

	return nil
}

func setupPVC(pvc *corev1.PersistentVolumeClaim) error {
	pvc, err := core.Instance().CreatePersistentVolumeClaim(pvc)
	if err != nil {
		return fmt.Errorf("create a volume claim: %s", err)
	}
	logrus.Infof("pvc %s/%s has been created", pvc.Namespace, pvc.Name)

	// mount pvc to a pod
	pod, err := core.Instance().CreatePod(podTemplateFor(pvc))
	if err != nil {
		return fmt.Errorf("create a pod: %s", err)
	}
	logrus.Infof("pod %s/%s has been created", pod.Namespace, pod.Name)

	if err = core.Instance().ValidatePod(pod, time.Minute, 3*time.Second); err != nil {
		return fmt.Errorf("wait for %s/%s pod is ready: %s", pod.Namespace, pod.Name, err)
	}

	// write some data to the src pvc
	cmd := []string{"touch", filepath.Join(pvcMountPath, pvcTestFileName)}
	out, err := core.Instance().RunCommandInPod(cmd, pod.Name, podContainerName, pod.Namespace)
	if err != nil {
		return fmt.Errorf("write test file to a pvc: %s", err)
	}

	if out != "" {
		logrus.Infof("write test file to a %s/%s pvc: %s", pvc.Namespace, pvc.Name, out)
	}

	// validate dst pvc data
	cmd = []string{"ls", filepath.Join(pvcMountPath)}
	out, err = core.Instance().RunCommandInPod(cmd, pod.Name, podContainerName, pod.Namespace)
	if err != nil {
		return fmt.Errorf("write test file to a pvc: %s", err)
	}

	if strings.TrimSpace(out) != pvcTestFileName {
		return fmt.Errorf("validate test file on the source pvc: got %q, expected - %q", out, pvcTestFileName)
	}

	logrus.Infof("test file to a %s/%s pvc has been written", pvc.Namespace, pvc.Name)
	// unmount src pvc (delete a pod)
	return core.Instance().DeletePod(pod.Name, pod.Namespace, false)
}

func checkPVC(pvc *corev1.PersistentVolumeClaim) error {
	// mount a pod
	pod, err := core.Instance().CreatePod(podTemplateFor(pvc))
	if err != nil {
		return err
	}
	logrus.Infof("pod %s/%s has been created", pod.Namespace, pod.Name)

	if err = core.Instance().ValidatePod(pod, time.Minute, 3*time.Second); err != nil {
		return fmt.Errorf("wait for %s/%s pod is ready: %s", pod.Namespace, pod.Name, err)
	}

	// validate dst pvc data
	cmd := []string{"ls", filepath.Join(pvcMountPath)}
	out, err := core.Instance().RunCommandInPod(cmd, pod.Name, podContainerName, pod.Namespace)
	if err != nil {
		return fmt.Errorf("read test file from a pvc: %s", err)
	}

	if strings.TrimSpace(out) != pvcTestFileName {
		return fmt.Errorf("got %q test file, expexted - %q", out, pvcTestFileName)
	}

	return nil
}

func ensureNamespaceDeleted(name string, waitTime time.Duration) error {
	if err := core.Instance().DeleteNamespace(name); err != nil {
		return err
	}

	timeout := time.Now().Add(waitTime)
	for time.Now().Before(timeout) {
		_, err := core.Instance().GetNamespace(name)
		if err != nil && errors.IsNotFound(err) {
			return nil
		}
	}

	return fmt.Errorf("wait for namespace deletion: timeout exceeded")
}

func podTemplateFor(pvc *corev1.PersistentVolumeClaim) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvc.Name,
			Namespace: pvc.Namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{
				{
					Name:  podContainerName,
					Image: podContainerImage,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "vol",
							MountPath: pvcMountPath,
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvc.Name,
							ReadOnly:  false,
						},
					},
				},
			},
		},
	}
}

func getNamespaceName() string {
	return fmt.Sprintf("test-dataexport-%d", time.Now().UnixNano())
}

func isSuccessful(inst *v1alpha1.DataExport) bool {
	return inst.Status.Stage == v1alpha1.DataExportStageFinal && inst.Status.Status == v1alpha1.DataExportStatusSuccessful
}
