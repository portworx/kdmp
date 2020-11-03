package operator

import (
	"github.com/portworx/pxc/pkg/commander"
	pxc "github.com/portworx/pxc/pkg/component"
	"github.com/spf13/cobra"
)

// Register this command
var _ = commander.RegisterCommandInit(func() {
	operatorCmd := &cobra.Command{
		Use:   "operator",
		Short: "Manage kdmp operator deployments",
	}

	operatorCmd.AddCommand(newInstallCmd(nil, nil))
	operatorCmd.AddCommand(newStatusCmd(nil, nil))
	pxc.RootAddCommand(operatorCmd)
})
