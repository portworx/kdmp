package nfs

import (
	"os"
	"path/filepath"

	"github.com/portworx/kdmp/pkg/executor"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func newDeleteResourcesCommand() *cobra.Command {
	deleteCommand := &cobra.Command{
		Use:   "delete",
		Short: "Start resource deletion on nfs target",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(deleteResources(namespace, applicationrestoreCR))
		},
	}
	deleteCommand.Flags().StringVarP(&bkpNamespace, "namespace", "", "", "Namespace for delete command")
	deleteCommand.Flags().StringVarP(&applicationCRName, "app-cr-name", "", "", "Application backup CR name")

	return deleteCommand
}

func deleteResources(
	bkpNamespace string,
	applicationCRName string,
) error {
	funct := "deleteResources"
	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		logrus.Errorf("%s: error parsing cloud cred: %v", funct, rErr)
		return rErr
	}
	resourceDir := filepath.Join(repo.Path, bkpNamespace, applicationCRName)

	if err := os.RemoveAll(resourceDir); err != nil {
		logrus.Errorf("%s: error deleting resources: %v", funct, err)
	}

	return nil
}
