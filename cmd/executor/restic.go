package main

import (
	"os"

	restic_executor "github.com/portworx/kdmp/pkg/executor/restic"
)

func main() {
	if err := restic_executor.NewCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
