//go:build e2e
// +build e2e

package e2e

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/portworx/kdmp/pkg/utils"
	coreops "github.com/portworx/sched-ops/k8s/core"
	storageops "github.com/portworx/sched-ops/k8s/storage"
	"github.com/sirupsen/logrus"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/pointer"
)

var (
	flagKubeconfig = flag.String("kubeconfig", "", "path to the kubeconfig file")
)

var (
	// testsSetID is unique for each run of the test and is used for removing test assets.
	testsSetID = time.Now().UnixNano()
)

func TestMain(m *testing.M) {
	flag.Parse()

	if err := setup(); err != nil {
		fmt.Fprintf(os.Stderr, "setup: %s\n", err)
		os.Exit(1)
	}

	logrus.Infof("testset-id: %d", testsSetID)
	if exitCode := m.Run(); exitCode != 0 {
		os.Exit(exitCode)
	}

	if err := clean(); err != nil {
		fmt.Fprintf(os.Stderr, "clean: %s\n", err)
		os.Exit(1)
	}
}

func setup() error {
	var config *rest.Config
	var err error
	if kconfig := getKubeconfig(); kconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return err
	}

	coreops.Instance().SetConfig(config)
	storageops.Instance().SetConfig(config)

	kdmpops, err := utils.NewForConfig(config)
	if err != nil {
		return err
	}
	utils.SetInstance(kdmpops)

	if _, err = storageops.Instance().CreateStorageClass(portworxStorageClass()); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("create portworx storageclass: %s", err)
	}

	return nil
}

func clean() error {
	if err := storageops.Instance().DeleteStorageClass(storageClassPortworx); err != nil {
		return fmt.Errorf(" delete %s storageclass: %s", storageClassPortworx, err)
	}
	return nil
}

func portworxStorageClass() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: storageClassPortworx,
		},
		Provisioner: "kubernetes.io/portworx-volume",
		Parameters: map[string]string{
			"repl": "3",
		},
		AllowVolumeExpansion: pointer.BoolPtr(true),
	}
}

func getKubeconfig() string {
	if *flagKubeconfig != "" {
		return *flagKubeconfig
	}
	return os.Getenv("KUBECONFIG")
}
