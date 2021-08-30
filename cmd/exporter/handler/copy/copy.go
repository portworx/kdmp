package copy

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/pxc/pkg/commander"
	pxc "github.com/portworx/pxc/pkg/component"
	"github.com/portworx/pxc/pkg/config"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	copyExample = templates.Examples(`
		# Start a job with a custom name
		kubectl pxc exporter copy --source src-pvc --destination dst-pvc --namespaces ns1 copy-src-pvc

		# Start a job with a generated name
		kubectl pxc exporter copy --source src-pvc --destination dst-pvc --namespaces ns1`)
)

// Register this command
var _ = commander.RegisterCommandInit(func() {
	pxc.RootAddCommand(newCopyCmd(nil, nil))
})

// Options is used for the copy subcommand setup.
type Options struct {
	source      string
	destination string
	name        string
	namespace   string
	out         io.Writer
	errOut      io.Writer
}

func newCopyCmd(out, errOut io.Writer) *cobra.Command {
	o := Options{
		out:    out,
		errOut: errOut,
	}

	cmd := &cobra.Command{
		Use:     "copy [name]",
		Short:   "Run a job to copy data between persistent volume claims",
		Example: copyExample,
		Args:    cobra.MaximumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.complete(args); err != nil {
				return err
			}
			return o.run()
		},
	}

	cmd.Flags().StringVarP(&o.source, "source", "", "", "name of the source PVC")
	cmd.Flags().StringVarP(&o.destination, "destination", "", "", "name of the destination PVC")
	cmd.Flags().StringVarP(&o.namespace, "namespace", "", "", "namespace of the PVCs")
	return cmd
}

func (o *Options) complete(args []string) error {
	if len(args) == 1 {
		o.name = args[0]
	}

	if o.source == "" {
		return fmt.Errorf("source should be set")
	}

	if o.destination == "" {
		return fmt.Errorf("destination should be set")
	}

	if o.namespace == "" {
		return fmt.Errorf("namespace should be set")
	}

	if o.out == nil {
		o.out = os.Stdout
	}

	if o.errOut == nil {
		o.errOut = os.Stderr
	}

	return nil
}

func (o *Options) run() error {
	// get a kdmp client
	kdmpclient, err := getKDMPClient()
	if err != nil {
		return err
	}

	// create a dataexport job
	j, err := kdmpclient.CreateDataExport(context.Background(), buildDataExportFor(o.name, o))
	if err != nil {
		return fmt.Errorf("failed to start a dataExport job: %s", err)
	}

	// TODO: table print?
	_, err = fmt.Fprintf(o.out, "Started a job to copy data from %s to %s PVC: %s\n", o.source, o.destination, j.Name)
	return err
}

func getKDMPClient() (kdmpops.Ops, error) {
	kdmpclient := kdmpops.Instance()
	if kdmpclient != nil {
		return kdmpclient, nil
	}

	cfg, err := config.KM().ToRESTConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to configure kubernetes client: %v", err)
	}

	kdmpclient.SetConfig(cfg)
	return kdmpclient, nil
}

func buildDataExportFor(name string, opts *Options) *v1alpha1.DataExport {
	if name == "" {
		name = fmt.Sprintf("copy-%s", opts.source)
	}

	return &v1alpha1.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: opts.namespace,
		},
		Spec: v1alpha1.DataExportSpec{
			Source: v1alpha1.DataExportObjectReference{
				Name:      opts.source,
				Namespace: opts.namespace,
			},
			Destination: v1alpha1.DataExportObjectReference{
				Name:      opts.destination,
				Namespace: opts.namespace,
			},
		},
	}
}
