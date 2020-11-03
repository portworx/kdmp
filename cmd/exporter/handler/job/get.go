package job

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/portworx/pxc/pkg/config"
	"github.com/portworx/pxc/pkg/util"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

// GetOptions is used for the get subcommand setup.
type GetOptions struct {
	name      string
	namespace string
	// output is a output format
	output string
	out    io.Writer
	errOut io.Writer
}

var (
	jobGetExample = templates.Examples(`
		# Get a job 
		kubectl pxc exporter job get job-name --namespace ns1

		# Use flag aliases
		kubectl pxc exporter job get job-name -n ns1`)
)

func newGetCmd(out, errOut io.Writer) *cobra.Command {
	o := &GetOptions{
		out:    out,
		errOut: errOut,
	}

	cmd := &cobra.Command{
		Use:     "get (name)",
		Short:   "Print a data export job",
		Example: jobGetExample,
		Args:    cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.complete(args); err != nil {
				return err
			}
			return o.run()
		},
	}

	cmd.Flags().StringVarP(&o.namespace, "namespace", "n", "", "namespace from which the data export job will be retrieved")
	cmd.Flags().StringVarP(&o.output, "output", "o", "", "print a raw data export object in the provided format (yaml|json)")
	return cmd
}

func (o *GetOptions) complete(args []string) error {
	// validate arguments
	if len(args) == 1 {
		o.name = args[0]
	}
	if o.name == "" {
		return fmt.Errorf("name should be provided")
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

	return isValidFormat(o.output)
}

func (o *GetOptions) run() error {
	// retrieve dataexport jobs
	j, err := getDataExportJobs(o.name, o.namespace)
	if err != nil {
		return fmt.Errorf("failed to retrieve dataExport job: %s", err)
	}

	msg, err := getCmdMessage(o.output, j)
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(o.out, msg)
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

func getDataExportJobs(name, namespace string) (*v1alpha1.DataExport, error) {
	// get a kdmp client
	kdmpclient, err := getKDMPClient()
	if err != nil {
		return nil, err
	}

	return kdmpclient.GetDataExport(name, namespace)
}

func getCmdMessage(format string, job *v1alpha1.DataExport) (string, error) {
	switch format {
	case "json":
		return util.ToJson(job)
	case "yaml":
		return util.ToYaml(job)
	default:

	}
	return getCmdTableMessage(job)
}

func getCmdTableMessage(j *v1alpha1.DataExport) (string, error) {
	w := bytes.NewBufferString("")
	tw := tabwriter.NewWriter(w, 1, 1, 1, ' ', 0)
	fmt.Fprintln(tw, "NAMESPACE\tDATA EXPORT NAME\tSTAGE\tSTATUS")
	fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", j.Namespace, j.Name, j.Status.Stage, j.Status.Status)
	if err := tw.Flush(); err != nil {
		return "", err
	}
	return w.String(), nil
}
