package nfs

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-openapi/inflect"
	stork_api "github.com/libopenstorage/stork/pkg/apis/stork/v1alpha1"
	"github.com/libopenstorage/stork/pkg/resourcecollector"
	kdmpapi "github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/executor"
	"github.com/portworx/sched-ops/k8s/apiextensions"
	"github.com/portworx/sched-ops/k8s/core"
	storkops "github.com/portworx/sched-ops/k8s/stork"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var (
	bkpNamespace      string
	applicationCRName string
	rbCrName          string
	rbCrNamespace     string
	resKinds          map[string]string
)

const (
	metadataObjectName        = "metadata.json"
	namespacesFile            = "namespaces.json"
	crdFile                   = "crds.json"
	resourcesFile             = "resources.json"
	backupResourcesBatchCount = 15
)

func newUploadBkpResourceCommand() *cobra.Command {
	bkpUploadCommand := &cobra.Command{
		Use:   "backup",
		Short: "Start a resource backup to nfs target",
		Run: func(c *cobra.Command, args []string) {
			executor.HandleErr(uploadResources(bkpNamespace, applicationCRName, rbCrName, rbCrNamespace))
		},
	}
	bkpUploadCommand.Flags().StringVarP(&bkpNamespace, "backup-namespace", "", "", "Namespace for backup command")
	bkpUploadCommand.Flags().StringVarP(&applicationCRName, "app-cr-name", "", "", "Namespace for applicationbackup CR whose resource to be backed up")
	bkpUploadCommand.Flags().StringVarP(&rbCrName, "rb-cr-name", "", "", "Name for resourcebackup CR to update job status")
	bkpUploadCommand.Flags().StringVarP(&rbCrNamespace, "rb-cr-namespace", "", "", "Namespace for resourcebackup CR to update job status")

	return bkpUploadCommand
}

func uploadResources(
	bkpNamespace string,
	applicationCRName string,
	rbCrName string,
	rbCrNamespace string,
) error {
	err := uploadBkpResource(bkpNamespace, applicationCRName)
	if err != nil {
		//update resourcebackup CR with status and reason
		st := kdmpapi.ResourceBackupProgressStatus{
			Status: kdmpapi.ResourceBackupStatusFailed,
			Reason: err.Error(),
		}

		err = executor.UpdateResourceBackupStatus(st, rbCrName, rbCrNamespace)
		if err != nil {
			logrus.Errorf("failed to update resorucebackup[%v/%v] status: %v", rbCrNamespace, rbCrName, err)
		}
		return err
	}
	//update resourcebackup CR with status and reason
	st := kdmpapi.ResourceBackupProgressStatus{
		Status: kdmpapi.ResourceBackupStatusSuccessful,
		Reason: "upload resource Successfully",
	}
	err = executor.UpdateResourceBackupStatus(st, rbCrName, rbCrNamespace)
	if err != nil {
		logrus.Errorf("failed to update resorucebackup[%v/%v] status: %v", rbCrNamespace, rbCrName, err)
		return err
	}

	return nil
}

func uploadBkpResource(
	bkpNamespace string,
	applicationCRName string,
) error {
	funct := "uploadBkpResource"
	repo, rErr := executor.ParseCloudCred()
	if rErr != nil {
		errMsg := fmt.Sprintf("%s: error parsing cloud cred: %v", funct, rErr)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	backup, err := storkops.Instance().GetApplicationBackup(applicationCRName, bkpNamespace)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error fetching applicationbackup %s: %v", funct, applicationCRName, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	bkpDir := filepath.Join(repo.Path, bkpNamespace, backup.ObjectMeta.Name, string(backup.ObjectMeta.UID))
	if err := os.MkdirAll(bkpDir, 0777); err != nil {
		errMsg := fmt.Sprintf("%s: error creating backup dir: %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	// First create the required directory
	err = uploadResource(bkpNamespace, backup, bkpDir)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading resources: %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	err = uploadNamespaces(bkpNamespace, backup, bkpDir)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading namespace resource %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	err = uploadCRDResources(resKinds, bkpDir)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading CRD resource %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}

	err = uploadMetadatResources(bkpNamespace, backup, bkpDir)
	if err != nil {
		errMsg := fmt.Sprintf("%s: error uploading metadata resource %v", funct, err)
		logrus.Errorf(errMsg)
		return fmt.Errorf(errMsg)
	}
	return nil
}

func uploadResource(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
) error {
	funct := "uploadResource"
	rc := initResourceCollector()
	resKinds = make(map[string]string)
	objInfo := []stork_api.ObjectInfo{}
	for _, val := range backup.Status.Resources {
		objInfo = append(objInfo, val.ObjectInfo)
	}
	optionalBackupResources := []string{"Job"}
	resourceCollectorOpts := resourcecollector.Options{}

	dummyObjects := stork_api.CreateObjectsMap(objInfo)
	// If there are more number of namespaces, do it in batches
	allObjects := make([]runtime.Unstructured, 0)
	for i := 0; i < len(backup.Spec.Namespaces); i += backupResourcesBatchCount {
		batch := backup.Spec.Namespaces[i:min(i+backupResourcesBatchCount, len(backup.Spec.Namespaces))]
		objects, err := rc.GetResources(
			batch,
			backup.Spec.Selectors,
			dummyObjects,
			optionalBackupResources,
			true,
			resourceCollectorOpts,
		)
		if err != nil {
			logrus.Errorf("error getting resources: %v", err)
			return err
		}

		allObjects = append(allObjects, objects...)
	}
	// For DBG remove it later
	for _, obj := range allObjects {
		metadata, err := meta.Accessor(obj)
		logrus.Infof("*** line 140 metadata: %+v", metadata)
		gvk := obj.GetObjectKind().GroupVersionKind()
		resKinds[gvk.Kind] = gvk.Version
		if err != nil {
			logrus.Infof("%s: %v", funct, err)
			return err
		}
	}

	// TODO: Need to create directory with UID GUID needed
	// for nfs share
	//TODO: Add support for encryption key
	jsonBytes, err := json.MarshalIndent(allObjects, "", " ")
	if err != nil {
		logrus.Infof("%s: %v", funct, err)
		return err
	}
	err = uploadData(resourcePath, jsonBytes, resourcesFile)
	if err != nil {
		logrus.Errorf("%s: %v", funct, err)
		return err
	}

	return nil
}

func uploadNamespaces(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
) error {
	funct := "uploadNamespaces"
	var namespaces []*v1.Namespace

	for _, namespace := range backup.Spec.Namespaces {
		ns, err := core.Instance().GetNamespace(namespace)
		if err != nil {
			logrus.Errorf("%s err: %v", funct, err)
			return err
		}
		ns.ResourceVersion = ""
		namespaces = append(namespaces, ns)
	}
	jsonBytes, err := json.MarshalIndent(namespaces, "", " ")
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	err = uploadData(resourcePath, jsonBytes, namespacesFile)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	return nil
}

func uploadCRDResources(
	resKinds map[string]string,
	resourcePath string,
) error {
	funct := "uploadCRDResources"
	crdList, err := storkops.Instance().ListApplicationRegistrations()
	if err != nil {
		return err
	}
	ruleset := inflect.NewDefaultRuleset()
	ruleset.AddPlural("quota", "quotas")
	ruleset.AddPlural("prometheus", "prometheuses")
	ruleset.AddPlural("mongodbcommunity", "mongodbcommunity")
	var crds []*apiextensionsv1beta1.CustomResourceDefinition
	for _, crd := range crdList.Items {
		for _, v := range crd.Resources {
			if _, ok := resKinds[v.Kind]; !ok {
				continue
			}
			crdName := ruleset.Pluralize(strings.ToLower(v.Kind)) + "." + v.Group
			res, err := apiextensions.Instance().GetCRDV1beta1(crdName, metav1.GetOptions{})
			if err != nil {
				if k8s_errors.IsNotFound(err) {
					continue
				}
				return err
			}
			crds = append(crds, res)
		}

	}
	jsonBytes, err := json.MarshalIndent(crds, "", " ")
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	err = uploadData(resourcePath, jsonBytes, crdFile)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}
	return nil
}

func uploadMetadatResources(
	bkpNamespace string,
	backup *stork_api.ApplicationBackup,
	resourcePath string,
) error {
	funct := "uploadMetadatResources"
	jsonBytes, err := json.MarshalIndent(backup, "", " ")
	if err != nil {
		return err
	}

	err = uploadData(resourcePath, jsonBytes, metadataObjectName)
	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}
	return nil
}

func uploadData(
	resourcePath string,
	data []byte,
	resourceFileName string,
) error {
	funct := "uploadData"
	filePath := filepath.Join(resourcePath, resourceFileName)
	err := ioutil.WriteFile(filePath, data, 0777)

	if err != nil {
		logrus.Errorf("%s err: %v", funct, err)
		return err
	}

	return nil
}

func min(x, y int) int {
	if x <= y {
		return x
	}
	return y
}

func initResourceCollector() resourcecollector.ResourceCollector {
	resourceCollector := resourcecollector.ResourceCollector{
		Driver: nil,
		QPS:    float32(executor.QPS),
		Burst:  executor.Burst,
	}

	if err := resourceCollector.Init(nil); err != nil {
		logrus.Errorf("Error initializing ResourceCollector: %v", err)
		os.Exit(1)
	}

	return resourceCollector
}
