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
	operatorServiceAccountYaml = `---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kdmp-operator
  namespace: kube-system`

	operatorDeploymentYaml = `---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kdmp-operator
  namespace: kube-system
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
        image: portworx/kdmp
        imagePullPolicy: Always
      serviceAccountName: kdmp-operator
`

	operatorClusterRoleYaml = `---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
   name: kdmp-operator
rules:
  - apiGroups:
      - ""
    resources:
      - persistentvolumeclaims
      - configmaps
      - events
      - secrets
      - serviceaccounts
    verbs:
      - '*'
  - apiGroups:
      - ""
    resources:
      - persistentvolumes
      - pods
    verbs:
      - get
      - list
  - apiGroups:
      - rbac.authorization.k8s.io
    resources:
      - roles
      - rolebindings
    verbs:
      - '*'
  - apiGroups:
      - batch
    resources:
      - jobs
    verbs:
      - '*'
  - apiGroups:
      - apiextensions.k8s.io
    resources:
      - customresourcedefinitions
    verbs:
      - get
      - list
      - create
  - apiGroups:
      - kdmp.portworx.com
    resources:
      - dataexports
      - volumebackups
    verbs:
      - '*'
  - apiGroups:
      - stork.libopenstorage.org
    resources:
      - backuplocations
    verbs:
      - '*'
  - apiGroups:
      - volumesnapshot.external-storage.k8s.io
    resources:
      - volumesnapshotdatas
      - volumesnapshots
    verbs:
      - '*'
  - apiGroups:
      - snapshot.storage.k8s.io
    resources:
      - volumesnapshotclasses
      - volumesnapshotcontents
      - volumesnapshots
    verbs:
      - '*'
  - apiGroups:
      - security.openshift.io
    resources:
      - securitycontextconstraints
    verbs:
      - use`

	operatorClusterRoleBindingYaml = `---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: kdmp-operator
subjects:
  - kind: ServiceAccount
    name: kdmp-operator
    namespace: kube-system
roleRef:
  kind: ClusterRole
  name: kdmp-operator
  apiGroup: rbac.authorization.k8s.io`
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
