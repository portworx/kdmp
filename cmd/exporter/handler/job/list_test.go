package job

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/portworx/kdmp/pkg/client/clientset/versioned/fake"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestListCmdFlags(t *testing.T) {
	type testFlag struct {
		Name, Shorthand, Usage string
	}
	expectedFlags := []testFlag{
		{
			Name:      "namespace",
			Shorthand: "n",
			Usage:     "namespace from which all the data export jobs will be listed",
		},
		{
			Name:      "output",
			Shorthand: "o",
			Usage:     "print a raw data export object in the provided format (yaml|json)",
		},
	}

	// check flags
	cmd := newListCmd(nil, nil)

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

func TestListCmd(t *testing.T) {
	testCases := []struct {
		name        string
		inputArgs   []string
		inputFlags  map[string]string
		createJob   bool
		expectedOut string
		expectedErr error
	}{
		{
			name: "with-namespace",
			inputFlags: map[string]string{
				"namespace": "namespace",
			},
			createJob:   true,
			expectedOut: "NAMESPACE DATA EXPORT NAME STAGE STATUS\nnamespace job              Final Successful\n",
		},
		{
			name:        "all-namespaces",
			createJob:   true,
			expectedOut: "NAMESPACE DATA EXPORT NAME STAGE STATUS\nnamespace job              Final Successful\n",
		},
	}

	for _, tc := range testCases {
		fakekdmpops := fake.NewSimpleClientset()
		if tc.createJob {
			o := successfulDataExport()
			_, err := fakekdmpops.KdmpV1alpha1().DataExports(o.Namespace).Create(o)
			require.Nil(t, err, tc.name)
		}
		kdmpops.SetInstance(kdmpops.New(fakekdmpops))

		stdout := bytes.NewBufferString("")
		cmd := newListCmd(stdout, nil)
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
