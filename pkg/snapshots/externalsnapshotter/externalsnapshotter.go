package externalsnapshotter

import (
	"fmt"

	"github.com/kubernetes-csi/external-snapshotter/v2/pkg/apis/volumesnapshot/v1beta1"
	"github.com/portworx/kdmp/pkg/snapshots"
	"github.com/portworx/sched-ops/k8s/core"
	"github.com/portworx/sched-ops/k8s/externalsnapshotter"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/pointer"
)

// Driver is a csi implementation of the snapshot driver.
type Driver struct {
}

// Name returns a name of the driver backend.
func (d Driver) Name() string {
	return snapshots.CSI
}

// CreateSnapshot creates a volume snapshot for a pvc.
func (d Driver) CreateSnapshot(opts ...snapshots.Option) (name, namespace string, err error) {
	o := snapshots.Options{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return "", "", err
			}
		}
	}

	// check if volume snapshot class exists
	_, err = externalsnapshotter.Instance().GetSnapshotClass(o.SnapshotClassName)
	if err != nil {
		return "", "", err
	}

	snap := &v1beta1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      toSnapName(o.PVCNamespace, o.PVCName),
			Namespace: o.PVCNamespace,
		},
		Spec: v1beta1.VolumeSnapshotSpec{
			Source: v1beta1.VolumeSnapshotSource{
				PersistentVolumeClaimName: pointer.StringPtr(o.PVCName),
			},
			VolumeSnapshotClassName: pointer.StringPtr(o.SnapshotClassName),
		},
	}

	_, err = externalsnapshotter.Instance().CreateSnapshot(snap)
	if err != nil && !errors.IsAlreadyExists(err) {
		return "", "", err
	}

	return toSnapName(o.PVCNamespace, o.PVCName), o.PVCNamespace, nil
}

// DeleteSnapshot removes a snapshot.
func (d Driver) DeleteSnapshot(name, namespace string) error {
	if err := externalsnapshotter.Instance().DeleteSnapshot(name, namespace); err != nil && !errors.IsNotFound(err) {
		return err
	}
	return nil
}

// SnapshotStatus returns a status for a snapshot.
func (d Driver) SnapshotStatus(name, namespace string) (snapshots.Status, error) {
	snap, err := externalsnapshotter.Instance().GetSnapshot(name, namespace)
	if err != nil {
		return "", err
	}
	return getStatus(snap), nil
}

// RestoreVolumeClaim creates a persistent volume claim from a provided snapshot.
func (d Driver) RestoreVolumeClaim(opts ...snapshots.Option) (*corev1.PersistentVolumeClaim, error) {
	o := snapshots.Options{}
	for _, opt := range opts {
		if opt != nil {
			if err := opt(&o); err != nil {
				return nil, err
			}
		}
	}

	pvc, err := core.Instance().GetPersistentVolumeClaim(o.PVCName, o.PVCNamespace)
	// return a pvc if it's exist
	if err == nil {
		return pvc, nil
	}
	// create a pvc if it's not found
	if !errors.IsNotFound(err) {
		return nil, err
	}

	in := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      o.PVCName,
			Namespace: o.PVCNamespace,
		},
		Spec: o.PVCSpec,
	}
	in.Spec.DataSource = &corev1.TypedLocalObjectReference{
		APIGroup: pointer.StringPtr("snapshot.storage.k8s.io"),
		Kind:     "VolumeSnapshot",
		Name:     o.Name,
	}

	pvc, err = core.Instance().CreatePersistentVolumeClaim(in)
	if err != nil {
		return nil, err
	}

	return pvc, nil
}

func toSnapName(pvcNamespace, pvcName string) string {
	return fmt.Sprintf("%s-%s", pvcNamespace, pvcName)
}

func getStatus(snap *v1beta1.VolumeSnapshot) snapshots.Status {
	if snap.Status.Error != nil {
		// reason -> snap.Status.Error.Message
		return snapshots.StatusFailed
	}

	if snap.Status.ReadyToUse == nil {
		return snapshots.StatusUnknown
	}

	if *snap.Status.ReadyToUse {
		return snapshots.StatusReady
	}

	return snapshots.StatusReady
}
