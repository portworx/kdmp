package kopia

import (
	"context"
	"fmt"
	"io"

	"github.com/libopenstorage/stork/pkg/objectstore"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gocloud.dev/blob"
	_ "github.com/portworx/sched-ops/k8s/kdmp"
)

func newMaintenanceCommand() *cobra.Command {
	var (
		credSecretName      string
		credSecretNamespace string
	)
	maintenanceCommand := &cobra.Command{
		Use:   "maintenance",
		Short: "maintenance for repo",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(runMaintenance())
		},
	}
	maintenanceCommand.Flags().StringVar(&credSecretName, "cred-secret-name", "", " cred secret name for the repository to run maintenance command")
	maintenanceCommand.Flags().StringVar(&credSecretNamespace, "cred-secret-namespace", "", "cred secret namespace for the repository to run maintenance command")
	return maintenanceCommand
}

// getRepoList - This function will return possible repo in the given bucket blob.
func getRepoList(bucket *blob.Bucket) ([]string, error) {
	// Get the list of repo in the bucket
	iterator := bucket.List(&blob.ListOptions{
		Delimiter: "/",
	})
	repoList := make([]string, 0)
	for {
		object, err := iterator.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			logrus.Errorf("getRepolist: err %v", err)
			return repoList, err
		}
		tempBucket := blob.PrefixedBucket(bucket, object.Key)
		exists, err := tempBucket.Exists(context.TODO(), kopiaRepositoryFile)
		if err != nil {
			logrus.Errorf("getRepoList: checking for presence of %v file failed: %v", err, kopiaRepositoryFile)
			continue
		}
		if exists {
			repoList = append(repoList, object.Key)
		}
	}
	return repoList, nil
}

func runMaintenance() error {
	// Parse using the mounted secrets
	fn := "runMaintenance:"
	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		errMsg := fmt.Sprintf("failed in parsing backuplocation: %s", rErr)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	bl, err := buildStorkBackupLocation(repo)
	if err != nil {
		logrus.Errorf("%v", err)
		return err
	}
	bucket, err := objectstore.GetBucket(bl)
	if err != nil {
		logrus.Errorf("getting bucket details for [%v] failed: %v", repo.Path, err)
		return err
	}
	// The generic backup will be created under generic-backups/ directory in a bucket.
	// So, to get the list of repo in the bucket, get list of enteries under genric-backup dir.
	repo.Name = genericBackupDir + "/"
	bucket = blob.PrefixedBucket(bucket, repo.Name)
	repoList, err := getRepoList(bucket)
	if err != nil {
		logrus.Errorf("getting repo list failed for bucket [%v]: %v", repo.Path, err)
		return err
	}
	for _, repoName := range repoList {
		repo.Name = getBackupPathWithRepoName(repoName)

		if err := runKopiaRepositoryConnect(repo); err != nil {
			errMsg := fmt.Sprintf("repository [%v] connect failed: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
		logrus.Infof("connect to repo completed successfully for repository [%v]", repo.Name)

		if err := runKopiaMaintenanceSet(repo); err != nil {
			errMsg := fmt.Sprintf("maintenance owner set command failed for repo [%v]: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
		logrus.Infof("maintenance set owner command completed successfully for repository [%v]", repo.Name)
		if err := runKopiaMaintenanceExecute(repo); err != nil {
			errMsg := fmt.Sprintf("maintenance full run command failed for repo [%v]: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			return fmt.Errorf(errMsg)
		}
		logrus.Infof("maintenance full run command completed successfully for repository [%v]", repo.Name)
	}

	return nil
}

func getBackupPathWithRepoName(repoName string) string {
	return genericBackupDir + "/" + repoName
}
func runKopiaMaintenanceExecute(repository *executor.Repository) error {
	fn := "runKopiaMaintenanceRun:"
	maintenanceRunCmd, err := kopia.GetMaintenanceRunCommand()
	if err != nil {
		errMsg := fmt.Sprintf("getting maintenance run command for [%v] failed: %v", repository.Name, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	initExecutor := kopia.NewMaintenanceRunExecutor(maintenanceRunCmd)
	if err := initExecutor.Run(); err != nil {
		errMsg := fmt.Sprintf("running maintenance run command for [%v] failed: %v", repository.Name, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	t := func() (interface{}, bool, error) {
		status, err := initExecutor.Status()
		if err != nil {
			return "", false, err
		}
		if status.LastKnownError != nil {
			return "", false, status.LastKnownError
		}

		if status.Done {
			return "", false, nil
		}
		return "", true, fmt.Errorf("maintenance run command status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		return err
	}

	return nil
}

func runKopiaMaintenanceSet(repository *executor.Repository) error {
	fn := "runKopiaMaintenanceSet:"
	maintenanceSetCmd, err := kopia.GetMaintenanceSetCommand()
	if err != nil {
		errMsg := fmt.Sprintf("getting maintenance set command for failed: %v", err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	initExecutor := kopia.NewMaintenanceSetExecutor(maintenanceSetCmd)
	if err := initExecutor.Run(); err != nil {
		errMsg := fmt.Sprintf("running maintenance set command for failed: %v", err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	t := func() (interface{}, bool, error) {
		status, err := initExecutor.Status()
		if err != nil {
			return "", false, err
		}
		if status.LastKnownError != nil {
			return "", false, status.LastKnownError
		}

		if status.Done {
			return "", false, nil
		}
		return "", true, fmt.Errorf("maintenance set command status not available")
	}
	if _, err := task.DoRetryWithTimeout(t, executor.DefaultTimeout, progressCheckInterval); err != nil {
		return err
	}

	return nil
}
