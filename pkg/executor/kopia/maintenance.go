package kopia

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	storkapi "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/objectstore"
	kdmp_api "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/kdmp/pkg/kopia"
	kdmpShedOps "github.com/portworx/sched-ops/k8s/kdmp"
	"github.com/portworx/sched-ops/task"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gocloud.dev/blob"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	maintenanceStatusName      string
	maintenanceStatusNamespace string
)

const (
	fullMaintenanceType    = "full"
	quickMaintenaceTye     = "quick"
	cacheDir               = "/tmp"
	kopiaNFSRepositoryFile = "kopia.repository.f"
)

func newMaintenanceCommand() *cobra.Command {
	var (
		credSecretName      string
		credSecretNamespace string
		maintenanceType     string
		logLevel            string
	)
	maintenanceCommand := &cobra.Command{
		Use:   "maintenance",
		Short: "maintenance for repo",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(runMaintenance(maintenanceType))
		},
	}
	maintenanceCommand.Flags().StringVar(&credSecretName, "cred-secret-name", "", " cred secret name for the repository to run maintenance command")
	maintenanceCommand.Flags().StringVar(&credSecretNamespace, "cred-secret-namespace", "", "cred secret namespace for the repository to run maintenance command")
	maintenanceCommand.Flags().StringVar(&maintenanceStatusName, "maintenance-status-name", "", "backuplocation maintenance status CR name, where repo maintenance status will be stored")
	maintenanceCommand.Flags().StringVar(&maintenanceStatusNamespace, "maintenance-status-namespace", "", "backuplocation maintenance status CR namespace, where repo maintenance status will be stored")
	maintenanceCommand.Flags().StringVar(&maintenanceType, "maintenance-type", "", "full - will run full maintenance and quick - will run quick maintenance")
	maintenanceCommand.Flags().StringVar(&logLevel, "log-level", "", "If debug mode in kopia is to be used")
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

func updateBackupLocationMaintenace(
	maintenanceType string,
	status kdmp_api.RepoMaintenanceStatusType,
	repoName string,
	reason string,
) error {
	fn := "updateBackupLocationMaintenace"
	// get BackupLocationMaintenance status CR for status update.
	backupLocationMaintenance, err := kdmpShedOps.Instance().GetBackupLocationMaintenance(maintenanceStatusName, maintenanceStatusNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("failed in getting backuplocationmaintenance CR [%v:%v]: %v", maintenanceStatusNamespace, maintenanceStatusName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	repoStatus := kdmp_api.RepoMaintenanceStatus{
		LastRunTimestamp: metav1.Now(),
		Status:           status,
		Reason:           reason,
	}
	if maintenanceType == fullMaintenanceType {
		if backupLocationMaintenance.Status.FullMaintenanceRepoStatus == nil {
			backupLocationMaintenance.Status.FullMaintenanceRepoStatus = make(map[string]kdmp_api.RepoMaintenanceStatus)
		}
		backupLocationMaintenance.Status.FullMaintenanceRepoStatus[repoName] = repoStatus
	} else {
		if backupLocationMaintenance.Status.QuickMaintenanceRepoStatus == nil {
			backupLocationMaintenance.Status.QuickMaintenanceRepoStatus = make(map[string]kdmp_api.RepoMaintenanceStatus)
		}
		backupLocationMaintenance.Status.QuickMaintenanceRepoStatus[repoName] = repoStatus
	}
	_, err = kdmpShedOps.Instance().UpdateBackupLocationMaintenance(backupLocationMaintenance)
	if err != nil {
		errMsg := fmt.Sprintf("failed in updating backuplocation maintenace CR [%v:%v]: %v", maintenanceStatusNamespace, maintenanceStatusName, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func runMaintenance(maintenanceType string) error {
	// Parse using the mounted secrets
	fn := "runMaintenance:"
	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		errMsg := fmt.Sprintf("failed in parsing backuplocation: %s", rErr)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	var repoList []string
	if repo.Type == storkapi.BackupLocationNFS {
		repoBaseDir := repo.Path + genericBackupDir + "/"
		listOfSubDirs, err := returnDirList(repoBaseDir)
		if err != nil {
			if os.IsNotExist(err) {
				logrus.Warnf("No directory %v exists, verify if it is a resource only backup", repoBaseDir)
				return nil
			}
			logrus.Errorf("Failed to list sub directories in dir %v : [%v]", repoBaseDir, err)
			return err
		}
		var kopiaFile string
		for _, subDir := range listOfSubDirs {
			kopiaFile = filepath.Join(repoBaseDir, subDir, kopiaNFSRepositoryFile)
			_, err := os.Stat(kopiaFile)
			if os.IsNotExist(err) {
				continue
			} else if err != nil {
				logrus.Errorf("Failed to stat kopia repository file in %v", kopiaFile)
			} else {
				repoList = append(repoList, subDir)
			}
		}
	} else {
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
		repoList, err = getRepoList(bucket)
		if err != nil {
			logrus.Errorf("getting repo list failed for bucket [%v]: %v", repo.Path, err)
			return err
		}
	}

	for _, repoName := range repoList {
		repo.Name = getBackupPathWithRepoName(repoName)
		if err := runKopiaRepositoryConnect(repo); err != nil {
			errMsg := fmt.Sprintf("repository [%v] connect failed: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			statusErr := updateBackupLocationMaintenace(maintenanceType, kdmp_api.RepoMaintenanceStatusFailed, repo.Name, err.Error())
			if statusErr != nil {
				logrus.Warnf("update of %smaintenance status for repo [%v] failed: %v", maintenanceType, repo.Name, statusErr)
			}

			continue
		}
		logrus.Infof("connect to repo completed successfully for repository [%v]", repo.Name)

		if err := runKopiaMaintenanceSet(repo); err != nil {
			errMsg := fmt.Sprintf("maintenance owner set command failed for repo [%v]: %v", repo.Name, err)
			logrus.Errorf("%s: %v", fn, errMsg)
			statusErr := updateBackupLocationMaintenace(maintenanceType, kdmp_api.RepoMaintenanceStatusFailed, repo.Name, err.Error())
			if statusErr != nil {
				logrus.Warnf("update of %smaintenance status for repo [%v] failed: %v", maintenanceType, repo.Name, statusErr)
			}
			continue
		}
		logrus.Infof("maintenance set owner command completed successfully for repository [%v]", repo.Name)
		if maintenanceType == fullMaintenanceType {
			if err := runKopiaMaintenanceExecute(repo); err != nil {
				errMsg := fmt.Sprintf("maintenance full run command failed for repo [%v]: %v", repo.Name, err)
				logrus.Errorf("%s: %v", fn, errMsg)
				statusErr := updateBackupLocationMaintenace(maintenanceType, kdmp_api.RepoMaintenanceStatusFailed, repo.Name, err.Error())
				if statusErr != nil {
					logrus.Warnf("update of %smaintenance status for repo [%v] failed: %v", maintenanceType, repo.Name, statusErr)
				}
				continue
			}
		} else {
			// Quick maintenance case
			if err := runKopiaQuickMaintenanceExecute(repo); err != nil {
				errMsg := fmt.Sprintf("maintenance quick run command failed for repo [%v]: %v", repo.Name, err)
				logrus.Errorf("%s: %v", fn, errMsg)
				statusErr := updateBackupLocationMaintenace(maintenanceType, kdmp_api.RepoMaintenanceStatusFailed, repo.Name, err.Error())
				if statusErr != nil {
					logrus.Warnf("update of %smaintenance status for repo [%v] failed: %v", maintenanceType, repo.Name, statusErr)
				}
				continue
			}
		}

		// Delete the kopia config files as the next connect command may fail because of this
		// Not failing the operation if the clean up of the directory fails
		err := cleanKopiaConfigContents()
		if err != nil {
			logrus.Errorf("failed to remove config contents from directory %s: %v", cacheDir, err)
		}

		statusErr := updateBackupLocationMaintenace(maintenanceType, kdmp_api.RepoMaintenanceStatusSuccess, repo.Name, "")
		if err != nil {
			logrus.Warnf("update of %smaintenance status for repo [%v] failed: %v", maintenanceType, repo.Name, statusErr)
			continue
		}
		logrus.Infof("maintenance full run command completed successfully for repository [%v]", repo.Name)
	}

	return nil
}

func returnDirList(parentDir string) ([]string, error) {
	var files []string
	fileInfo, err := os.ReadDir(parentDir)
	if err != nil {
		return files, err
	}

	for _, file := range fileInfo {
		files = append(files, file.Name())
	}
	return files, nil
}

func cleanKopiaConfigContents() error {
	// cleaning all files starting with kopia in the config directory
	files, err := filepath.Glob(fmt.Sprintf("%s/kopia*", cacheDir))
	if err != nil {
		return err
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			return err
		}
	}
	return nil
}

func getBackupPathWithRepoName(repoName string) string {
	return genericBackupDir + "/" + repoName
}
func runKopiaQuickMaintenanceExecute(repository *executor.Repository) error {
	fn := "runKopiaQuickMaintenanceExecute:"
	maintenanceRunCmd, err := kopia.GetMaintenanceRunCommand()
	if err != nil {
		errMsg := fmt.Sprintf("getting maintenance run command for [%v] failed: %v", repository.Name, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}

	initExecutor := kopia.NewMaintenanceRunExecutor(maintenanceRunCmd)
	if err := initExecutor.Run(logLevel); err != nil {
		errMsg := fmt.Sprintf("running maintenance run command for [%v] failed: %v", repository.Name, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	for {
		time.Sleep(progressCheckInterval)
		status, err := initExecutor.Status()
		if err != nil {
			return err
		}
		if status.LastKnownError != nil {
			return status.LastKnownError
		}

		if status.Done {
			break
		}
	}
	return nil
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
	if err := initExecutor.Run(logLevel); err != nil {
		errMsg := fmt.Sprintf("running maintenance run command for [%v] failed: %v", repository.Name, err)
		logrus.Errorf("%s %v", fn, errMsg)
		return fmt.Errorf(errMsg)
	}
	for {
		time.Sleep(progressCheckInterval)
		status, err := initExecutor.Status()
		if err != nil {
			return err
		}
		if status.LastKnownError != nil {
			return status.LastKnownError
		}

		if status.Done {
			break
		}
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
	if err := initExecutor.Run(logLevel); err != nil {
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
