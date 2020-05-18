package snapshotsinstance

import (
	"fmt"
	"sync"

	"github.com/portworx/kdmp/pkg/snapshots"
	"github.com/portworx/kdmp/pkg/snapshots/externalsnapshotter"
	"github.com/portworx/kdmp/pkg/snapshots/externalstorage"
)

var (
	mu            sync.Mutex
	driversByName = map[string]snapshots.Driver{
		snapshots.ExternalStorage: externalstorage.Driver{},
		snapshots.CSI:             externalsnapshotter.Driver{},
	}
)

// Add append a driver to the drivers list.
func Add(driver snapshots.Driver) error {
	mu.Lock()
	defer mu.Unlock()

	if driver == nil {
		return fmt.Errorf("driver is nil")
	}

	driversByName[driver.Name()] = driver
	return nil
}

// Get retrieves a driver for provided name.
func Get(name string) (snapshots.Driver, error) {
	mu.Lock()
	defer mu.Unlock()

	driver, ok := driversByName[name]
	if !ok || driver == nil {
		return nil, fmt.Errorf("%q driver: not found", name)
	}

	return driver, nil
}

// GetForStorageClass retrieves a driver for provided storage class name.
// It takes a csi driver by default.
func GetForStorageClass(name string) (snapshots.Driver, error) {
	mu.Lock()
	defer mu.Unlock()

	// TODO: GetForStorageClass map?
	switch name {
	case externalstorage.DefaultStorageClass:
		return externalstorage.Driver{}, nil
	}

	return externalsnapshotter.Driver{}, nil
}
