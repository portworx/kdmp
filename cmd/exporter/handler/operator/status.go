package operator

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	appsops "github.com/portworx/sched-ops/k8s/apps"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	operatorStatusExample = templates.Examples(`
		# Get operator deployment details
		kubectl pxc exporter operator status`)
)

// StatusOptions is used for the delete subcommand setup.
type StatusOptions struct {
	out    io.Writer
	errOut io.Writer
}

func newStatusCmd(out, errOut io.Writer) *cobra.Command {
	o := &StatusOptions{
		out:    out,
		errOut: errOut,
	}

	cmd := &cobra.Command{
		Use:          "status",
		Short:        "Provide operator deployment details",
		SilenceUsage: true,
		Example:      operatorStatusExample,
		Args:         cobra.ExactArgs(0),
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.complete(args); err != nil {
				return err
			}
			return o.run()
		},
	}

	return cmd
}

func (o *StatusOptions) complete(args []string) error {
	if o.out == nil {
		o.out = os.Stdout
	}

	if o.errOut == nil {
		o.errOut = os.Stderr
	}

	return nil
}

func (o *StatusOptions) run() error {
	deployList, err := appsops.Instance().ListDeployments("", metav1.ListOptions{
		LabelSelector: "name=kdmp-operator",
	})
	if err != nil {
		if errors.IsNotFound(err) {
			return o.printNotFound()
		}
		return err
	}

	w := bytes.NewBufferString("")
	tw := tabwriter.NewWriter(w, 1, 1, 1, ' ', 0)
	fmt.Fprintln(tw, "DEPLOYMENT\tNAMESPACE\tSTATUS")
	for _, deploy := range deployList.Items {
		fmt.Fprintf(tw, "%s\t%s\t%s\n", deploy.Name, deploy.Namespace, getDeploymentStatus(&deploy))
	}
	if err = tw.Flush(); err != nil {
		return fmt.Errorf("failed to parse table print: %s", err)
	}

	fmt.Fprint(o.out, w.String())
	return nil
}

func (o *StatusOptions) printNotFound() error {
	_, err := fmt.Fprint(o.out, "operator is not installed.")
	return err
}

func getDeploymentStatus(deploy *v1.Deployment) string {
	if deploy != nil && deploy.Status.ReadyReplicas > 0 {
		return "ready"
	}
	return "not ready"
}
