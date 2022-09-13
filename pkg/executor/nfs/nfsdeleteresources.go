package nfs

import (
	"os"
	"path/filepath"

	"github.com/portworx/kdmp/pkg/executor"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// ResourceJSONFiles list of resource JSON files
var ResourceJSONFiles = []string{metadataObjectName, namespacesFile, crdFile, resourcesFile}

func newDeleteResourcesCommand() *cobra.Command {
	deleteCommand := &cobra.Command{
		Use:   "delete",
		Short: "Start resource deletion on nfs target",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(deleteResources(namespace, applicationrestoreCR))
		},
	}
	deleteCommand.Flags().StringVarP(&bkpNamespace, "namespace", "", "", "Namespace for restore command")
	deleteCommand.Flags().StringVarP(&applicationCRName, "app-cr-name", "", "", "application restore CR name")

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
	backup, err := storkops.Instance().GetApplicationBackup(applicationCRName, bkpNamespace)
	if err != nil {
		logrus.Infof("%s:error fetching applicationbackup %s: %v", funct, applicationCRName, err)
		return err
	}
	bkpDir := filepath.Join(repo.Path, bkpNamespace, backup.ObjectMeta.Name, string(backup.ObjectMeta.UID))

	for _, file := range ResourceJSONFiles {
		if err := os.Remove(bkpDir + "/" + file); err != nil {
			if !os.IsNotExist(err) {
				logrus.Errorf("%s: error deleting resource: %v", funct, err)
			}
		}
	}
	return nil
}
