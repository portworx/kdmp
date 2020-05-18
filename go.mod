module github.com/portworx/kdmp

go 1.13

require (
	github.com/kubernetes-csi/external-snapshotter/v2 v2.1.1
	github.com/kubernetes-incubator/external-storage v0.0.0-00010101000000-000000000000
	github.com/libopenstorage/stork v1.3.0-beta1.0.20200410032049-df8bcecf8f71
	github.com/portworx/sched-ops v0.0.0-20200506183916-30fe0dc40344
	github.com/sirupsen/logrus v1.4.2
	github.com/stretchr/testify v1.4.0
	github.com/urfave/cli v1.22.2
	golang.org/x/lint v0.0.0-20200302205851-738671d3881b // indirect
	golang.org/x/tools v0.0.0-20200515220128-d3bf790afa53 // indirect
	honnef.co/go/tools v0.0.1-2020.1.4 // indirect
	k8s.io/api v0.17.0
	k8s.io/apiextensions-apiserver v0.16.6
	k8s.io/apimachinery v0.17.1-beta.0
	k8s.io/client-go v11.0.1-0.20190409021438-1a26190bd76a+incompatible
	k8s.io/code-generator v0.16.6
	k8s.io/utils v0.0.0-20190923111123-69764acb6e8e
	sigs.k8s.io/controller-runtime v0.4.0
)

replace (
	github.com/kubernetes-incubator/external-storage => github.com/libopenstorage/external-storage v5.3.0-alpha.1.0.20200130041458-d2b33d4448ea+incompatible
	github.com/porworx/sched-ops => github.com/portworx/sched-ops v0.0.0-20200506183916-30fe0dc40344
	k8s.io/api => k8s.io/api v0.16.6
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.16.6
	k8s.io/apimachinery => k8s.io/apimachinery v0.16.6
	k8s.io/apiserver => k8s.io/apiserver v0.16.6
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.16.6
	k8s.io/client-go => k8s.io/client-go v0.16.6
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.16.6
	k8s.io/cluster-bootstrap => k8s.io/cluster-bootstrap v0.16.6
	k8s.io/code-generator => k8s.io/code-generator v0.16.6
	k8s.io/component-base => k8s.io/component-base v0.16.6
	k8s.io/cri-api => k8s.io/cri-api v0.16.6
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.16.6
	k8s.io/kube-aggregator => k8s.io/kube-aggregator v0.16.6
	k8s.io/kube-controller-manager => k8s.io/kube-controller-manager v0.16.6
	k8s.io/kube-proxy => k8s.io/kube-proxy v0.16.6
	k8s.io/kube-scheduler => k8s.io/kube-scheduler v0.16.6
	k8s.io/kubectl => k8s.io/kubectl v0.16.6
	k8s.io/kubelet => k8s.io/kubelet v0.16.6
	k8s.io/kubernetes => k8s.io/kubernetes v1.16.6
	k8s.io/legacy-cloud-providers => k8s.io/legacy-cloud-providers v0.16.6
	k8s.io/metrics => k8s.io/metrics v0.16.6
	k8s.io/sample-apiserver => k8s.io/sample-apiserver v0.16.6
	sigs.k8s.io/controller-runtime => sigs.k8s.io/controller-runtime v0.4.0
)
