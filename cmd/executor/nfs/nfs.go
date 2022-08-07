package main

import (
	"os"

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
