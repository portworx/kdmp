package job

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/portworx/kdmp/pkg/apis/kdmp/v1alpha1"
	"github.com/portworx/kdmp/pkg/client/clientset/versioned/fake"
	kdmpops "github.com/portworx/kdmp/pkg/util/ops"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestDeleteCmdFlags(t *testing.T) {
	type testFlag struct {
		Name, Shorthand, Usage string
	}
	expectedFlags := []testFlag{
		{
			Name:      "namespace",
			Shorthand: "n",
			Usage:     "namespace where the export job is running",
		},
	}

	// check flags
	cmd := newDeleteCmd(nil, nil)

	flags := make([]testFlag, 0)
	cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		flags = append(flags, testFlag{
			Name:      flag.Name,
			Shorthand: flag.Shorthand,
			Usage:     flag.Usage,
		})
	})

	require.Equal(t, expectedFlags, flags, "check delete cmd flags")
}

func TestDeleteCmd(t *testing.T) {
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
			expectedErr: fmt.Errorf("failed to delete dataExport job: dataexports.kdmp.portworx.com \"job-name\" not found"),
		},
		{
			name:      "delete-job",
			inputArgs: []string{"job-name"},
			inputFlags: map[string]string{
				"namespace": "n1",
			},
			createJob:   true,
			expectedOut: "Data export jobs has been succesfully deleted: n1/job-name\n",
		},
	}

	for _, tc := range testCases {
		fakekdmpops := fake.NewSimpleClientset()
		if tc.createJob {
			o := dataExportFrom(tc.inputArgs, tc.inputFlags)
			_, err := fakekdmpops.KdmpV1alpha1().DataExports(o.Namespace).Create(o)
			require.Nil(t, err, tc.name)
		}
		kdmpops.SetInstance(kdmpops.New(fakekdmpops))

		stdout := bytes.NewBufferString("")
		cmd := newDeleteCmd(stdout, nil)
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

func dataExportFrom(args []string, flags map[string]string) *v1alpha1.DataExport {
	var name, namespace string
	if len(args) > 0 {
		name = args[0]
	}
	if flags != nil {
		namespace = flags["namespace"]
	}

	return &v1alpha1.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func successfulDataExportFrom(args []string, flags map[string]string) *v1alpha1.DataExport {
	var name, namespace string
	if len(args) > 0 {
		name = args[0]
	}
	if flags != nil {
		namespace = flags["namespace"]
	}

	return &v1alpha1.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: v1alpha1.ExportStatus{
			Stage:  v1alpha1.DataExportStageFinal,
			Status: v1alpha1.DataExportStatusSuccessful,
		},
	}
}

func successfulDataExport() *v1alpha1.DataExport {
	return &v1alpha1.DataExport{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "job",
			Namespace: "namespace",
		},
		Status: v1alpha1.ExportStatus{
			Stage:  v1alpha1.DataExportStageFinal,
			Status: v1alpha1.DataExportStatusSuccessful,
		},
	}
}
