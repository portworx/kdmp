package jobframework

import (
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	"github.com/portworx/sched-ops/k8s/core"
	v1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
)

// getJobsByType takes the jobType as a param and returns the list of jobs matching the label in all namespaces
func getJobsByType(jobType string) []*v1.JobList {
	labelSelector := "jobtype=" + jobType
	options := metav1.ListOptions{
		LabelSelector: labelSelector,
	}
	getAllNamespaces := getAllNamespaces()
	var allJobList []*v1.JobList
	for _, item := range getAllNamespaces.Items {
		allJobs, _ := batch.Instance().ListAllJobs(item.Name, options)
		allJobList = append(allJobList, allJobs)
	}
	return allJobList
}

func getAllNamespaces() *corev1.NamespaceList {
	labelSelector := map[string]string{}
	allNameSpaces, _ := core.Instance().ListNamespaces(labelSelector)
	return allNameSpaces
}

// activeJobs takes the jobList and returns the count of the active jobs
func activeJobs(jobList []*v1.JobList) int {
	activeJobsCount := 0
	for _, list := range jobList {
		for _, job := range list.Items {
			if job.Status.Active > 0 {
				activeJobsCount++
			}
		}
	}
	return activeJobsCount
}

// jobLimitByType takes the job type and fetches the value from the config map
func jobLimitByType(jobType string) int {
	jobLimit, err := strconv.Atoi(utils.GetConfigValue(jobType))
	// if its not found in configmap the strconv will through error
	if err != nil {
		return 10 // ToDo Decide on the default value if not found in the config map
	}
	return jobLimit
}

// JobCanRun takes the jobType and returns weather the given job can run or not based on limit set
func JobCanRun(jobType string) bool {
	jobsList := getJobsByType(jobType)
	activeJobsCount := activeJobs(jobsList)
	jobLimitCount := jobLimitByType(jobType)
	return jobLimitCount > activeJobsCount
}
