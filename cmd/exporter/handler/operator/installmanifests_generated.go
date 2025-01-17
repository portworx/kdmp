// This is a generated file. DO NOT EDIT
package operator

import (
	"bytes"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
)

var (
	operatorServiceAccountYaml = ``

	operatorDeploymentYaml = `---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kdmp-operator
  namespace: kube-system
  labels:
    name: kdmp-operator
spec:
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 1
    type: RollingUpdate
  replicas: 1
  selector:
    matchLabels:
      name: kdmp-operator
  template:
    metadata:
      labels:
        name: kdmp-operator
    spec:
      containers:
      - name: kdmp-operator
        image: portworx/kdmp:latest
        imagePullPolicy: Always
        resources:
          requests:
            cpu: 0.5
            memory: 200Mi
          limits:
            cpu: 1
            memory: 500Mi
      serviceAccountName: kdmp-operator
`

	operatorClusterRoleYaml = ``

	operatorClusterRoleBindingYaml = ``
)

type Manifests struct {
	ServiceAccount     corev1.ServiceAccount
	Deployment         appsv1.Deployment
	ClusterRole        rbacv1.ClusterRole
	ClusterRoleBinding rbacv1.ClusterRoleBinding
}

func ParseOperatorManifests() (Manifests, error) {
	manifests := Manifests{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorServiceAccountYaml), 4096).Decode(&manifests.ServiceAccount); err != nil {
		return Manifests{}, err
	}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorDeploymentYaml), 4096).Decode(&manifests.Deployment); err != nil {
		return Manifests{}, err
	}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorClusterRoleYaml), 4096).Decode(&manifests.ClusterRole); err != nil {
		return Manifests{}, err
	}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewBufferString(operatorClusterRoleBindingYaml), 4096).Decode(&manifests.ClusterRoleBinding); err != nil {
		return Manifests{}, err
	}
	return manifests, nil
}
