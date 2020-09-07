package utils

import (
	"fmt"

	coreops "github.com/portworx/sched-ops/k8s/core"
	rbacops "github.com/portworx/sched-ops/k8s/rbac"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SetupServiceAccount create a serviceaccount with enough permissions for resticexecutor.
// (read for BackupLocation, read/write for VolumeBackup)
func SetupServiceAccount(name, namespace string) error {
	if _, err := rbacops.Instance().CreateRole(roleFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s role: %s", name, namespace, err)
	}
	if _, err := rbacops.Instance().CreateRoleBinding(roleBindingFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s rolebinding: %s", name, namespace, err)
	}
	if _, err := coreops.Instance().CreateServiceAccount(serviceAccountFor(name, namespace)); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s/%s serviceaccount: %s", name, namespace, err)
	}
	return nil
}

// CleanServiceAccount removes a serviceaccount with a corresponding role and rolebinding.
func CleanServiceAccount(name, namespace string) error {
	if err := rbacops.Instance().DeleteRole(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s role: %s", name, namespace, err)
	}
	if err := rbacops.Instance().DeleteRoleBinding(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s rolebinding: %s", name, namespace, err)
	}
	if err := coreops.Instance().DeleteServiceAccount(name, namespace); err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("delete %s/%s serviceaccount: %s", name, namespace, err)
	}
	return nil
}

func roleFor(name, namespace string) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
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

func roleBindingFor(name, namespace string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      name,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			Name:     name,
			Kind:     "Role",
			APIGroup: rbacv1.GroupName,
		},
	}
}

func serviceAccountFor(name, namespace string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}
