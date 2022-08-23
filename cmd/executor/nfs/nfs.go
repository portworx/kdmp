package main

import (
	"os"

	_ "github.com/libopenstorage/stork/drivers/volume/aws"
	_ "github.com/libopenstorage/stork/drivers/volume/azure"
	_ "github.com/libopenstorage/stork/drivers/volume/csi"
	_ "github.com/libopenstorage/stork/drivers/volume/gcp"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	nfs_executor "github.com/portworx/kdmp/pkg/executor/nfs"
)

/*const (
	QPS        = 100
	Burst      = 100
	DriverName = "pxd"
)*/

func main() {
	if err := nfs_executor.NewCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
