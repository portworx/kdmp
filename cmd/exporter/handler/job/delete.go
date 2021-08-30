package job

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/kubectl/pkg/util/templates"
)

var (
	jobDeleteExample = templates.Examples(`
		# Delete a job
		kubectl pxc exporter job delete job-name --namespace job-ns

		# Use flag aliases
		kubectl pxc exporter job delete job-name -n job-ns`)
)

// DeleteOptions is used for the delete subcommand setup.
type DeleteOptions struct {
	name      string
	namespace string
	out       io.Writer
	errOut    io.Writer
}

func newDeleteCmd(out, errOut io.Writer) *cobra.Command {
	o := &DeleteOptions{
		out:    out,
		errOut: errOut,
	}

	cmd := &cobra.Command{
		Use:          "delete (name)",
		Short:        "Remove a data export job",
		SilenceUsage: true,
		Example:      jobDeleteExample,
		Args:         cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if err := o.complete(args); err != nil {
				return err
			}
			return o.run()
		},
	}

	cmd.Flags().StringVarP(&o.namespace, "namespace", "n", "", "namespace where the export job is running")
	return cmd
}

func (o *DeleteOptions) complete(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("name should be provided")
	}
	o.name = args[0]

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

func (o *DeleteOptions) run() error {
	// retrieve dataexport jobs
	if err := deleteDataExportJob(o.name, o.namespace); err != nil {
		return fmt.Errorf("failed to delete dataExport job: %s", err)
	}

	_, err := fmt.Fprintf(o.out, "Data export jobs has been succesfully deleted: %s/%s\n", o.namespace, o.name)
	return err
}

func deleteDataExportJob(name, namespace string) error {
	// get a kdmp client
	kdmpclient, err := getKDMPClient()
	if err != nil {
		return err
	}

	return kdmpclient.DeleteDataExport(context.Background(), name, namespace)
}
