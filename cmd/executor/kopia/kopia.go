package main

import (
	"os"

	kopia_executor "github.com/portworx/kdmp/pkg/executor/kopia"
)

func main() {
	if err := kopia_executor.NewCommand().Execute(); err != nil {
		os.Exit(1)
	}
}
