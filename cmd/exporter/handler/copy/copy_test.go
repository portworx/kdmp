package copy

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

func TestCopyCmdFlags(t *testing.T) {
	type testFlag struct {
		Name, Shorthand, Usage string
	}
	expectedFlags := []testFlag{
		{
			Name:      "destination",
			Shorthand: "d",
			Usage:     "name of the destination PVC",
		},
		{
			Name:      "namespace",
			Shorthand: "n",
			Usage:     "namespace of the PVCs",
		},
		{
			Name:      "source",
			Shorthand: "s",
			Usage:     "name of the source PVC",
		},
	}

	// check flags
	cmd := newCopyCmd(nil, nil)

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

func TestCopyCmd(t *testing.T) {
	testCases := []struct {
		name        string
		inputArgs   []string
		inputFlags  map[string]string
		expectedOut string
		expectedErr error
	}{
		{
			name:        "source_flag_not_set",
			expectedErr: fmt.Errorf("source should be set"),
		},
		{
			name: "destination_flag_not_set",
			inputFlags: map[string]string{
				"source": "src",
			},
			expectedErr: fmt.Errorf("destination should be set"),
		},
		{
			name: "namespace_flag_not_set",
			inputFlags: map[string]string{
				"source":      "src",
				"destination": "dst",
			},
			expectedErr: fmt.Errorf("namespace should be set"),
		},
		{
			name: "start-job-generated-name",
			inputFlags: map[string]string{
				"source":      "src",
				"destination": "dst",
				"namespace":   "n1",
			},
			expectedOut: "Started a job to copy data from src to dst PVC: copy-src\n",
		},
		{
			name:      "start-job",
			inputArgs: []string{"job-name"},
			inputFlags: map[string]string{
				"source":      "src",
				"destination": "dst",
				"namespace":   "n1",
			},
			expectedOut: "Started a job to copy data from src to dst PVC: job-name\n",
		},
	}

	for _, tc := range testCases {
		fakekdmpops := fake.NewSimpleClientset()
		kdmpops.SetInstance(kdmpops.New(fakekdmpops))

		stdout := bytes.NewBufferString("")
		cmd := newCopyCmd(stdout, nil)
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
