package job

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/pxc/pkg/util"
	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	jobListExample = templates.Examples(`
		# List jobs over all namespace
		kubectl pxc exporter job list

		# List jobs over in a namespace
		kubectl pxc exporter job list --namespace ns1

		# Use flag aliases
		kubectl pxc exporter job list -n ns1`)
)

// ListOptions is used for the list subcommand setup.
type ListOptions struct {
	namespace string
	output    string
	out       io.Writer
	errOut    io.Writer
}

func newListCmd(out, errOut io.Writer) *cobra.Command {
	o := &ListOptions{
		out:    out,
		errOut: errOut,
	}

	cmd := &cobra.Command{
		Use:     "list",
		Short:   "List data export jobs",
		Example: jobListExample,
		Args:    cobra.ExactArgs(0),
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.complete(args); err != nil {
				return err
			}
			return o.run()
		},
	}

	cmd.Flags().StringVarP(&o.namespace, "namespace", "n", "", "namespace from which all the data export jobs will be listed")
	cmd.Flags().StringVarP(&o.output, "output", "o", "", "print a raw data export object in the provided format (yaml|json)")
	return cmd
}

func (o *ListOptions) complete(args []string) error {
	if o.out == nil {
		o.out = os.Stdout
	}

	if o.errOut == nil {
		o.errOut = os.Stderr
	}

	return isValidFormat(o.output)
}

func (o *ListOptions) run() error {
	jobs, err := listDataExportJob(o.namespace)
	if err != nil {
		return fmt.Errorf("failed to get data export job: %s", err)
	}

	msg, err := listCmdMessage(o.output, jobs)
	if err != nil {
		return err
	}

	_, err = fmt.Fprint(o.out, msg)
	return err
}

func listDataExportJob(namespace string) ([]v1alpha1.DataExport, error) {
	// get a kdmp client
	kdmpclient, err := getKDMPClient()
	if err != nil {
		return nil, err
	}

	list, err := kdmpclient.ListDataExports(context.Background(), namespace)
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func listCmdMessage(format string, jobs []v1alpha1.DataExport) (string, error) {
	switch format {
	case "json":
		return util.ToJson(jobs)
	case "yaml":
		return util.ToYaml(jobs)
	}
	return toTableMessage(jobs)
}

func toTableMessage(jobs []v1alpha1.DataExport) (string, error) {
	w := bytes.NewBufferString("")
	tw := tabwriter.NewWriter(w, 1, 1, 1, ' ', 0)
	fmt.Fprintln(tw, "NAMESPACE\tDATA EXPORT NAME\tSTAGE\tSTATUS")
	for _, j := range jobs {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", j.Namespace, j.Name, j.Status.Stage, j.Status.Status)
	}
	if err := tw.Flush(); err != nil {
		return "", err
	}
	return w.String(), nil
}
