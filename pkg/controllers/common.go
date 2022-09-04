package controllers

import (
	"time"
)

var (
	ResyncPeriod                      = 10 * time.Second
	RequeuePeriod                     = 5 * time.Second
	ValidateCRDInterval time.Duration = 10 * time.Second
	ValidateCRDTimeout  time.Duration = 2 * time.Minute

	CleanupFinalizer = "kdmp.portworx.com/finalizer-cleanup"
)
