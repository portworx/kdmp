package job

import (
	"fmt"

	"github.com/portworx/pxc/pkg/commander"
	pxc "github.com/portworx/pxc/pkg/component"
	"github.com/spf13/cobra"
)

// Register this command
var _ = commander.RegisterCommandInit(func() {
	jobCmd := &cobra.Command{
		Use:   "job",
		Short: "Manage data export jobs",
		//Long:  "",
		//Example: "",
	}
	jobCmd.AddCommand(newGetCmd(nil, nil))
	jobCmd.AddCommand(newListCmd(nil, nil))
	jobCmd.AddCommand(newDeleteCmd(nil, nil))

	pxc.RootAddCommand(jobCmd)
})

func isValidFormat(output string) error {
	if output == "" {
		return nil
	}

	formats := []string{"yaml", "json"}
	for _, f := range formats {
		if f == output {
			return nil
		}
	}

	return fmt.Errorf("invalid output format - %q. Supports - %v", output, formats)
}
