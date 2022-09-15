module github.com/portworx/kdmp

go 1.15

require (
	github.com/aquilax/truncate v1.0.0
	github.com/go-openapi/inflect v0.19.0
	github.com/hashicorp/go-version v1.2.1
	github.com/kubernetes-csi/external-snapshotter/client/v4 v4.0.0
	github.com/kubernetes-incubator/external-storage v0.20.4-openstorage-rc7
	github.com/libopenstorage/stork v1.4.1-0.20221103082056-65abc8cc4e80
	github.com/portworx/pxc v0.33.0
	github.com/portworx/sched-ops v1.20.4-rc1.0.20220915143205-3fe6e54a2a4d
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.2-0.20220317124727-77977386932a
	github.com/urfave/cli v1.22.2
	gocloud.dev v0.20.0
	k8s.io/api v0.24.0
	k8s.io/apiextensions-apiserver v0.21.5
	k8s.io/apimachinery v0.24.3
	k8s.io/cli-runtime v0.21.5
	k8s.io/client-go v12.0.0+incompatible
	k8s.io/code-generator v0.21.5
	k8s.io/component-helpers v0.24.0
	k8s.io/kubectl v0.21.5
	k8s.io/utils v0.0.0-20210527160623-6fdb442a123b
	sigs.k8s.io/controller-runtime v0.9.7
)

replace (
	github.com/go-logr/logr => github.com/go-logr/logr v0.4.0
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1
	github.com/kubernetes-csi/external-snapshotter/v2 => github.com/kubernetes-csi/external-snapshotter/v2 v2.1.1
	github.com/kubernetes-incubator/external-storage => github.com/libopenstorage/external-storage v0.20.4-openstorage-rc6
	github.com/kubernetes-incubator/external-storage v0.0.0-00010101000000-000000000000 => github.com/libopenstorage/external-storage v5.3.0-alpha.1.0.20200130041458-d2b33d4448ea+incompatible
	github.com/libopenstorage/openstorage => github.com/libopenstorage/openstorage v1.0.1-0.20220804223926-812cb10d08c4
	github.com/portworx/sched-ops => github.com/portworx/sched-ops v1.20.4-rc1.0.20220915143205-3fe6e54a2a4d
	k8s.io/api => k8s.io/api v0.21.4
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.21.4
	k8s.io/apimachinery => k8s.io/apimachinery v0.21.5-rc.0
	k8s.io/apiserver => k8s.io/apiserver v0.21.4
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.21.4
	k8s.io/client-go => k8s.io/client-go v0.21.4
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.21.4
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.21.4
	k8s.io/code-generator => k8s.io/code-generator v0.21.5-rc.0
	k8s.io/component-base => k8s.io/component-base v0.21.4
	k8s.io/component-helpers => k8s.io/component-helpers v0.21.4
	k8s.io/controller-manager => k8s.io/controller-manager v0.21.4
	k8s.io/cri-api => k8s.io/cri-api v0.21.5-rc.0
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.21.4
	k8s.io/klog/v2 => k8s.io/klog/v2 v2.4.0
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.21.4
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.21.4
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.21.4
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.21.4
	k8s.io/kubectl => k8s.io/kubectl v0.21.4
	k8s.io/kubelet => k8s.io/kubelet v0.21.4
	k8s.io/kubernetes => k8s.io/kubernetes v1.21.4
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.21.4
	k8s.io/metrics => k8s.io/metrics v0.21.4
	k8s.io/mount-utils => k8s.io/mount-utils v0.21.5-rc.0
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.21.4
	k8s.io/sample-cli-plugin => k8s.io/sample-cli-plugin v0.21.4
	k8s.io/sample-controller => k8s.io/sample-controller v0.21.4
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.9.0
)
