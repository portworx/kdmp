package jobframework

import (
	"fmt"
	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/portworx/sched-ops/k8s/batch"
	v1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strconv"
)

const (
	DefaultNamespace = "kube-system"
)

// getJobsByType takes the jobType as a param and returns the list of jobs matching the label
func getJobsByType(jobType string) *v1.JobList {
	labelSelector := fmt.Sprintf("jobtype=%s", jobType)
	options := metav1.ListOptions{
		LabelSelector: labelSelector,
	}
	allJobs, _ := batch.Instance().ListAllJobs(DefaultNamespace, options)
	return allJobs
}

// activeJobs takes the jobList and returns the count of the active jobs
func activeJobs(jobList *v1.JobList) int {
	activeJobsCount := 0
	for _, job := range jobList.Items {
		if job.Status.Active > 0 {
			activeJobsCount++
		}
	}
	return activeJobsCount
}

// jobLimitByType takes the job type and fetches the value from the config map
func jobLimitByType(jobType string) int {
	jobLimit, _ := strconv.Atoi(utils.GetConfigValue(jobType))
	return jobLimit
}

// JobCanRun takes the jobType and returns weather the given job can run or not based on limit set
func JobCanRun(jobType string) bool {
	jobsList := getJobsByType(jobType)
	activeJobsCount := activeJobs(jobsList)
	jobLimitCount := jobLimitByType(jobType)
	return jobLimitCount >= activeJobsCount
}
