package main

import (
	_ "github.com/libopenstorage/stork/drivers/volume/aws"
	_ "github.com/libopenstorage/stork/drivers/volume/azure"
	_ "github.com/libopenstorage/stork/drivers/volume/csi"
	_ "github.com/libopenstorage/stork/drivers/volume/gcp"
	_ "github.com/libopenstorage/stork/drivers/volume/portworx"
	nfs_executor "github.com/portworx/kdmp/pkg/executor/nfs"
	"os"
)

func main() {
	if err := nfs_executor.NewCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
