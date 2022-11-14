package main

import (
	"os"

	_ "github.com/libopenstorage/stork/drivers/volume/aws"
	_ "github.com/libopenstorage/stork/drivers/volume/azure"
	_ "github.com/libopenstorage/stork/drivers/volume/csi"
	_ "github.com/libopenstorage/stork/drivers/volume/gcp"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	nfs_executor "github.com/portworx/kdmp/pkg/executor/nfs"
	"github.com/portworx/kdmp/pkg/version"
	"github.com/sirupsen/logrus"
)

func main() {
	v := version.Get()
	logrus.Infof("Starting nfs executor: %s, build date %s", v.String(), v.BuildDate)
	if err := nfs_executor.NewCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
