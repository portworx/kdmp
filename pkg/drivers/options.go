package drivers

import (
	"fmt"
	"strings"
)

// JobOption is used for job configuration.
type JobOption func(opts *JobOpts) error

// JobOpts defines all job parameters.
type JobOpts struct {
	SourcePVCName      string
	DestinationPVCName string
	Namespace          string
	Labels             map[string]string
}

// WithSourcePVC is job parameter.
func WithSourcePVC(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("source pvc name should be set")
		}
		opts.SourcePVCName = name
		return nil
	}
}

// WithDestinationPVC is job parameter.
func WithDestinationPVC(name string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf("destination pvc name should be set")
		}
		opts.DestinationPVCName = name
		return nil
	}
}

// WithNamespace is job parameter.
func WithNamespace(ns string) JobOption {
	return func(opts *JobOpts) error {
		if strings.TrimSpace(ns) == "" {
			return fmt.Errorf("namespace should be set")
		}
		opts.Namespace = ns
		return nil
	}
}

// WithLabels is job parameter.
func WithLabels(l map[string]string) JobOption {
	return func(opts *JobOpts) error {
		opts.Labels = l
		return nil
	}
}
