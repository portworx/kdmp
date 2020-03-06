package dataexport

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (c *Controller) getJob(name, namespace string) (*batchv1.Job, error) {
	if err := checkMetadata(name, namespace); err != nil {
		return nil, err
	}

	into := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if err := c.client.Get(context.TODO(), client.ObjectKey{}, into); err != nil {
		return nil, err
	}
	return into, nil
}

func (c *Controller) deleteJob(name, namespace string) error {
	if err := checkMetadata(name, namespace); err != nil {
		return err
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
	if err := c.client.Delete(context.TODO(), job); !errors.IsNotFound(err) {
		return err
	}
	return nil
}

func (c *Controller) getPVC(name, namespace string) (*corev1.PersistentVolumeClaim, error) {
	if err := checkMetadata(name, namespace); err != nil {
		return nil, err
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	if err := c.client.Get(context.TODO(), client.ObjectKey{}, pvc); err != nil {
		return nil, err
	}

	return pvc, nil
}

func checkMetadata(name, namespace string) error {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(namespace) == "" {
		return fmt.Errorf("name and namespace should not be empty")
	}
	return nil
}
