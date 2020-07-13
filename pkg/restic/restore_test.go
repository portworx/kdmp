package restic

import (
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetRestoreCommand(t *testing.T) {
	testCases := []struct {
		name        string
		repo        string
		snapshotID  string
		secretPath  string
		dstPath     string
		expectedErr error
		expectedCmd *exec.Cmd
	}{
		{
			name:        "dstPath is not provided",
			expectedErr: fmt.Errorf("destination path cannot be empty"),
		},
		{
			name:        "repoName is not provided",
			dstPath:     "dst/path",
			expectedErr: fmt.Errorf("repository name cannot be empty"),
		},
		{
			name:        "secretFilePass is not provided",
			repo:        "test",
			dstPath:     "dst/path",
			expectedErr: fmt.Errorf("secret file path cannot be empty"),
		},
		{
			name:        "default snapshot id",
			repo:        "test",
			dstPath:     "dst/path",
			secretPath:  "secret/path",
			expectedCmd: exec.Command("restic", "restore", "--repo", "test", "--password-file", "secret/path", "--host", "kdmp", "--json", "--target", ".", "latest"),
		},
		{
			name:        "custom snapshot id",
			repo:        "test",
			dstPath:     "dst/path",
			secretPath:  "secret/path",
			snapshotID:  "customID",
			expectedCmd: exec.Command("restic", "restore", "--repo", "test", "--password-file", "secret/path", "--host", "kdmp", "--json", "--target", ".", "customID"),
		},
	}

	for _, tc := range testCases {
		restore, err := GetRestoreCommand(tc.repo, tc.snapshotID, tc.secretPath, tc.dstPath)
		if tc.expectedCmd != nil {
			tc.expectedCmd.Dir = tc.dstPath
		}

		require.Equalf(t, tc.expectedErr, err, "TC: %s", tc.name)
		if err == nil {
			require.Equalf(t, tc.expectedCmd, restore.Cmd(), "TC: %s", tc.name)
		}
	}
}
