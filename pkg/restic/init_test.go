package restic

import (
	"fmt"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetInitCommand(t *testing.T) {
	testCases := []struct {
		name        string
		repo        string
		secretPath  string
		expectedErr error
		expectedCmd *exec.Cmd
	}{
		{
			name:        "repoName_is_not_provided",
			expectedErr: fmt.Errorf("repository name cannot be empty"),
		},
		{
			name:        "secretFilePass_is_not_provided",
			repo:        "test",
			expectedErr: fmt.Errorf("secret file path cannot be empty"),
		},
		{
			name:        "initialized",
			repo:        "test",
			secretPath:  "secret/path",
			expectedCmd: exec.Command("restic", "init", "--repo", "test", "--password-file", "secret/path"),
		},
	}

	for _, tc := range testCases {
		restore, err := GetInitCommand(tc.repo, tc.secretPath)

		require.Equalf(t, tc.expectedErr, err, "TC: %s", tc.name)
		if err == nil {
			require.Equalf(t, tc.expectedCmd, restore.Cmd(), "TC: %s", tc.name)
		}
	}
}
