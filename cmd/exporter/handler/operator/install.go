package operator

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/portworx/kdmp/pkg/drivers"
	"github.com/spf13/cobra"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/kubectl/pkg/util/templates"
)

//go:generate go run ./gen

const (
	kdmpOperatorName = "kdmp-operator"
)

var (
	operatorInstallExample = templates.Examples(`
		# Get kdmp operator manifests for the kube-system namespace
		kubectl pxc exporter operator install

		# Get kdmp operator manifests for a custom namespace
		kubectl pxc exporter operator install -n ns1

		# Install a kdmp operator
		kubectl pxc exporter operator install | kubectl create -f -`)
)

// InstallOptions is used for the delete subcommand setup.
type InstallOptions struct {
	namespace                string
	format                   string
	image                    string
	rsyncImage               string
	rsyncPullSecret          string
	rsyncOpenshiftSCC        string
	resticExecutorImage      string
	resticExecutorPullSecret string

	out    io.Writer
	errOut io.Writer
}

func newInstallCmd(out, errOut io.Writer) *cobra.Command {
	o := &InstallOptions{
		out:    out,
		errOut: errOut,
	}

	cmd := &cobra.Command{
		Use:          "install",
		Short:        "Provide kubernetes manifest files for kdmp operator",
		SilenceUsage: true,
		Example:      operatorInstallExample,
		Args:         cobra.ExactArgs(0),
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.complete(args); err != nil {
				return err
			}
			return o.run()
		},
	}

	cmd.Flags().StringVarP(&o.namespace, "namespace", "n", "kube-system", "namespace where to deploy a kdmp operator")
	cmd.Flags().StringVarP(&o.format, "output", "o", "", "print in the provided format (yaml|json)")
	cmd.Flags().StringVarP(&o.image, "image", "", "", "custom kdmp operator image")
	cmd.Flags().StringVarP(&o.rsyncImage, "rsync-image", "", "", "custom rsync image")
	cmd.Flags().StringVarP(&o.rsyncPullSecret, "rsync-pull-secret", "", "", "pull secret name for a custom rsync image")
	cmd.Flags().StringVarP(&o.rsyncOpenshiftSCC, "rsync-openshift-scc", "", "", "openshift security context constraint name to use for rsync jobs")
	cmd.Flags().StringVarP(&o.resticExecutorImage, "resticexecutor-image", "", "", "custom resticexecutor image")
	cmd.Flags().StringVarP(&o.resticExecutorPullSecret, "resticexecutor-pull-secret", "", "", "pull secret name for a custom resticexecutor image")

	return cmd
}

func (o *InstallOptions) complete(args []string) error {
	if o.out == nil {
		o.out = os.Stdout
	}

	if o.errOut == nil {
		o.errOut = os.Stderr
	}

	return nil
}

func (o *InstallOptions) run() error {
	operatorManifests, err := ParseOperatorManifests()
	if err != nil {
		return err
	}

	manifests := []runtime.Object{
		operatorServiceAccount(operatorManifests.ServiceAccount, o.namespace),
		operatorDeployment(operatorManifests.Deployment, o.namespace, o.image, o.rsyncImage, o.rsyncPullSecret, o.rsyncOpenshiftSCC, o.resticExecutorImage, o.resticExecutorPullSecret),
		operatorClusterRole(operatorManifests.ClusterRole),
		operatorClusterRoleBinding(operatorManifests.ClusterRoleBinding, o.namespace),
	}

	raw, err := encode(o.format, manifests)
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(os.Stdout, raw)
	return err
}

func encode(format string, manifests []runtime.Object) (string, error) {
	var p printers.ResourcePrinter
	switch format {
	case "json":
		p = &printers.JSONPrinter{}
	default:
		p = &printers.YAMLPrinter{}
	}

	buff := bytes.NewBufferString("")
	for _, m := range manifests {
		err := p.PrintObj(m, buff)
		if err != nil {
			return "", err
		}
	}
	return buff.String(), nil
}

func operatorServiceAccount(sa corev1.ServiceAccount, namespace string) *corev1.ServiceAccount {
	sa.Name = kdmpOperatorName
	sa.Namespace = namespace
	return &sa
}

func operatorDeployment(
	deploy appsv1.Deployment,
	namespace,
	image,
	rsyncImage,
	rsyncPullSecret,
	rsyncopenshiftSCC,
	resticExecutorImage,
	resticExecutorPullSecret string,
) *appsv1.Deployment {
	envs := make([]corev1.EnvVar, 0)
	for _, env := range []corev1.EnvVar{
		{Name: drivers.RsyncImageKey, Value: rsyncImage, ValueFrom: nil},
		{Name: drivers.RsyncImageSecretKey, Value: rsyncPullSecret, ValueFrom: nil},
		{Name: drivers.RsyncOpenshiftSCC, Value: rsyncopenshiftSCC, ValueFrom: nil},
		{Name: drivers.ResticExecutorImageKey, Value: resticExecutorImage, ValueFrom: nil},
		{Name: drivers.ResticExecutorImageSecretKey, Value: resticExecutorPullSecret, ValueFrom: nil},
	} {
		if env.Value == "" {
			continue
		}
		envs = append(envs, env)
	}

	deploy.Name = kdmpOperatorName
	deploy.Namespace = namespace

	newContainers := make([]corev1.Container, len(deploy.Spec.Template.Spec.Containers))
	for i := range deploy.Spec.Template.Spec.Containers {
		container := deploy.Spec.Template.Spec.Containers[i]
		if container.Name == kdmpOperatorName {
			if image != "" {
				container.Image = image
			}
			container.Env = mergeEnvs(container.Env, envs)
		}

		newContainers[i] = container
	}

	deploy.Spec.Template.Spec.Containers = newContainers
	deploy.Spec.Template.Spec.ServiceAccountName = kdmpOperatorName
	return &deploy
}

func operatorClusterRole(role rbacv1.ClusterRole) *rbacv1.ClusterRole {
	role.Name = kdmpOperatorName
	return &role
}

func operatorClusterRoleBinding(roleBinding rbacv1.ClusterRoleBinding, namespace string) *rbacv1.ClusterRoleBinding {
	roleBinding.Name = kdmpOperatorName
	roleBinding.RoleRef.Name = kdmpOperatorName

	newSubjects := make([]rbacv1.Subject, len(roleBinding.Subjects))
	for i := range roleBinding.Subjects {
		subj := roleBinding.Subjects[i]
		subj.Namespace = namespace

		newSubjects[i] = subj
	}

	roleBinding.Subjects = newSubjects
	return &roleBinding
}

func mergeEnvs(orig []corev1.EnvVar, merge []corev1.EnvVar) []corev1.EnvVar {
	if len(merge) == 0 {
		return orig
	}

	envsMap := make(map[string]corev1.EnvVar)
	for _, env := range orig {
		envsMap[env.Name] = env
	}
	for _, env := range merge {
		envsMap[env.Name] = env
	}

	newEnvs := make([]corev1.EnvVar, 0)
	for _, v := range envsMap {
		newEnvs = append(newEnvs, v)
	}

	return newEnvs
}
