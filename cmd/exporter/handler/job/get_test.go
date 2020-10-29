package job

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/portworx/kdmp/pkg/client/clientset/versioned/fake"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestGetCmdFlags(t *testing.T) {
	type testFlag struct {
		Name, Shorthand, Usage string
	}
	expectedFlags := []testFlag{
		{
			Name:      "namespace",
			Shorthand: "n",
			Usage:     "namespace from which the data export job will be retrieved",
		},
		{
			Name:      "output",
			Shorthand: "o",
			Usage:     "print a raw data export object in the provided format (yaml|json)",
		},
	}

	// check flags
	cmd := newGetCmd(nil, nil)

	flags := make([]testFlag, 0)
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, testFlag{
			Name:      flag.Name,
			Shorthand: flag.Shorthand,
			Usage:     flag.Usage,
		})
	})

	require.Equal(t, expectedFlags, flags, "check cmd flags")
}

func TestGetCmd(t *testing.T) {
	testCases := []struct {
		name        string
		inputArgs   []string
		inputFlags  map[string]string
		createJob   bool
		expectedOut string
		expectedErr error
	}{
		{
			name:        "name-should-be-set",
			expectedErr: fmt.Errorf("name should be provided"),
		},
		{
			name:        "namespace-should-be-set",
			inputArgs:   []string{"job-name"},
			expectedErr: fmt.Errorf("namespace should be set"),
		},
		{
			name:      "job-not-found",
			inputArgs: []string{"job-name"},
			inputFlags: map[string]string{
				"namespace": "n1",
			},
			expectedErr: fmt.Errorf("failed to retrieve dataExport job: dataexports.kdmp.portworx.com \"job-name\" not found"),
		},
		{
			name:      "delete-job",
			inputArgs: []string{"job-name"},
			inputFlags: map[string]string{
				"namespace": "n1",
			},
			createJob:   true,
			expectedOut: "Data export jobs:\nn1/job-name: Final/Successful\n",
		},
	}

	for _, tc := range testCases {
		fakekdmpops := fake.NewSimpleClientset()
		if tc.createJob {
			o := successfulDataExportFrom(tc.inputArgs, tc.inputFlags)
			_, err := fakekdmpops.KdmpV1alpha1().DataExports(o.Namespace).Create(o)
			require.Nil(t, err, tc.name)
		}
		kdmpops.SetInstance(kdmpops.New(fakekdmpops))

		stdout := bytes.NewBufferString("")
		cmd := newGetCmd(stdout, nil)
		for k, v := range tc.inputFlags {
			err := cmd.Flags().Set(k, v)
			require.Nil(t, err, tc.name)
		}

		err := cmd.RunE(cmd, tc.inputArgs)
		require.Equalf(t, tc.expectedErr, err, tc.name)

		outbytes, err := ioutil.ReadAll(stdout)
		require.Nil(t, err, tc.name)
		require.Equalf(t, tc.expectedOut, string(outbytes), tc.name)
	}
}
