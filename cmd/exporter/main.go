package main

import (
	_ "github.com/portworx/kdmp/cmd/exporter/handler"
	"github.com/portworx/kdmp/pkg/version"
	pxc "github.com/portworx/pxc/pkg/component"
)

var (
	// ComponentName is set by the Makefile
	ComponentName = "exporter"
)

func main() {
	c := pxc.NewComponent(&pxc.ComponentConfig{
		Name:    ComponentName,
		Short:   "Exporter utility provides a way to transfer data between PersistentVolumeClaims",
		Version: version.Get().String(),
	})
	c.Execute()
}
