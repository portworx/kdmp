package main

import (
	_ "github.com/portworx/kdmp/cmd/exporter/handler"
	pxc "github.com/portworx/pxc/pkg/component"
)

var (
	// ComponentName is set by the Makefile
	ComponentName = "exporter"

	// ComponentVersion is set by the Makefile
	ComponentVersion = "master"
)

func main() {
	c := pxc.NewComponent(&pxc.ComponentConfig{
		Name:    ComponentName,
		Short:   "Exporter utility provides a way to transfer data between PersistentVolumeClaims",
		Version: ComponentVersion,
	})
	c.Execute()
}
