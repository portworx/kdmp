package handler

import (
	_ "github.com/portworx/kdmp/cmd/exporter/handler/copy"     // add the copy subcommand
	_ "github.com/portworx/kdmp/cmd/exporter/handler/job"      // add the job subcommand
	_ "github.com/portworx/kdmp/cmd/exporter/handler/operator" // add the operator subcommand
)
