//go:build unittest
// +build unittest

package restic

import (
	"os"
	"os/exec"
	"testing"
	"time"

	cmdexec "github.com/portworx/kdmp/pkg/executor"
	"github.com/stretchr/testify/require"
)

const (
	testRepoName       = "/tmp/kdmp-restic-tests"
	testSource         = "/tmp/source"
	testSecretFilePath = "/tmp/secret"
)

func TestGetBackupCommand(t *testing.T) {
	cmd, err := GetBackupCommand("fo", "bar", "")
	require.Error(t, err, "expected an error from GetBackupCommand")
	require.Nil(t, cmd, "expected a nil Command")

	cmd, err = GetBackupCommand("foo", "", "src")
	require.Error(t, err, "expected an error from GetBackupCommand")
	require.Nil(t, cmd, "expected a nil Command")

	cmd, err = GetBackupCommand("", "bar", "src")
	require.Error(t, err, "expected an error from GetBackupCommand")
	require.Nil(t, cmd, "expected a nil Command")

	cmd, err = GetBackupCommand("foo", "bar", "src")
	require.NoError(t, err, "unexpected error on GetBackupCommand")

	actualCmd := cmd.Cmd()
	expectedCmd := exec.Command("restic", "backup", "--repo", "foo", "--password-file", "bar", "--host", "kdmp", "--json", ".")
	expectedCmd.Dir = "src"
	require.Equal(t, actualCmd, expectedCmd, "unexpected command parsed")

	cmd.AddArg("arg1")

	actualCmd = cmd.Cmd()
	expectedCmd = exec.Command("restic", "backup", "--repo", "foo", "--password-file", "bar", "--host", "kdmp", "--json", ".", "arg1")
	expectedCmd.Dir = "src"
	require.Equal(t, actualCmd, expectedCmd, "unexpected command parsed")

	cmd.AddFlag("--flag").AddFlag("flag1")

	actualCmd = cmd.Cmd()
	expectedCmd = exec.Command("restic", "backup", "--repo", "foo", "--password-file", "bar", "--host", "kdmp", "--json", "--flag", "flag1", ".", "arg1")
	expectedCmd.Dir = "src"
	require.Equal(t, actualCmd, expectedCmd, "unexpected command parsed")

}

func TestGetProgress(t *testing.T) {
	testData := `
{"message_type":"status","percent_done":0,"total_files":1,"total_bytes":10485760}
{"message_type":"status","percent_done":0.5,"total_files":1,"total_bytes":10485760,"bytes_done":5242880,"current_files":["/tmp/source/10M"]}
{"message_type":"status","percent_done":0.75,"total_files":1,"total_bytes":10485760,"bytes_done":7864320,"current_files":["/tmp/source/10M"]}
{"message_type":"status","percent_done":0.95,"total_files":1,"total_bytes":10485760,"bytes_done":9961472,"current_files":["/tmp/source/10M"]}
`
	testBytes := []byte(testData)
	backupProgress, err := getProgress(testBytes, nil)
	require.NoError(t, err, "unexpected error on get progress")
	require.Equal(t, float64(0.95), backupProgress.PercentDone, "unexpected percentDone")
	require.Equal(t, uint64(9961472), backupProgress.BytesDone, "unexpected bytesDone")
	require.Equal(t, uint64(10485760), backupProgress.TotalBytes, "unexpected totalBytes")
}

func TestGetProgressNotAvailable(t *testing.T) {
	testData := `
`
	testBytes := []byte(testData)
	backupProgress, err := getProgress(testBytes, nil)
	require.EqualError(t, err, "backup progress not available yet", "unexpected error")
	require.Nil(t, backupProgress, "expected nil backup progress")
}

func TestGetProgressUnmarshalFailure(t *testing.T) {
	testData := `
{"foo":"bar"}
`
	testBytes := []byte(testData)
	errBytes := testBytes

	backupProgress, err := getProgress(testBytes, errBytes)
	require.Error(t, err, "expected an error on getProgress")
	require.Nil(t, backupProgress, "expected nil backup progress")
	bErr, ok := err.(*cmdexec.Error)
	require.True(t, ok, "expected error of Error kind")
	require.Contains(t, bErr.Reason, "failed to parse progress of backup")
	require.Equal(t, string(errBytes), bErr.CmdErr, "unexpected cmdErr")
	require.Equal(t, string(testBytes), bErr.CmdErr, "unexpected cmdOutput")
}

func TestGetSummary(t *testing.T) {
	testData := `
{"message_type":"status","percent_done":0.3,"total_files":1,"total_bytes":10485760,"bytes_done":3145728,"current_files":["/tmp/source/10M"]}
{"message_type":"status","percent_done":0.55,"total_files":1,"total_bytes":10485760,"bytes_done":5767168,"current_files":["/tmp/source/10M"]}
{"message_type":"status","percent_done":0.85,"total_files":1,"total_bytes":10485760,"bytes_done":8912896,"current_files":["/tmp/source/10M"]}
{"message_type":"summary","files_new":1,"files_changed":0,"files_unmodified":0,"dirs_new":1,"dirs_changed":0,"dirs_unmodified":0,"data_blobs":1,"tree_blobs":2,"data_added":525023,"total_files_processed":1,"total_bytes_processed":10485760,"total_duration":0.541563334,"snapshot_id":"9310620e"}
`

	testBytes := []byte(testData)
	backupSummary, err := getSummary(testBytes, nil)
	require.NoError(t, err, "unexpected error on getSummary")
	require.Equal(t, uint64(10485760), backupSummary.TotalBytesProcessed)
	require.Equal(t, "summary", backupSummary.MessageType, "unexpected message")
	require.Equal(t, "9310620e", backupSummary.SnapshotID, "unexpected snapshot_id")
}

func TestGetSummaryNotAvailable(t *testing.T) {
	testData := `
`
	testBytes := []byte(testData)
	backupSummary, err := getSummary(testBytes, nil)
	require.Nil(t, backupSummary, "expected nil backup summary")
	bErr, ok := err.(*cmdexec.Error)
	require.True(t, ok, "unexpected error type")
	require.Equal(t, bErr.Reason, "backup summary not available")
}

func TestGetSummaryUnmarshalFailure(t *testing.T) {
	testData := `
adssdgsg
sgssdfsafd
`
	testBytes := []byte(testData)
	backupSummary, err := getSummary(testBytes, nil)
	require.Nil(t, backupSummary, "expected nil backup summary")
	bErr, ok := err.(*cmdexec.Error)
	require.True(t, ok, "unexpected error type")
	require.Contains(t, bErr.Reason, "failed to parse backup summary")
}

func TestGetSummaryNotFound(t *testing.T) {
	testData := `
{"message_type":"status","percent_done":0.3,"total_files":1,"total_bytes":10485760,"bytes_done":3145728,"current_files":["/tmp/source/10M"]}
{"message_type":"status","percent_done":0.55,"total_files":1,"total_bytes":10485760,"bytes_done":5767168,"current_files":["/tmp/source/10M"]}
{"message_type":"status","percent_done":0.85,"total_files":1,"total_bytes":10485760,"bytes_done":8912896,"current_files":["/tmp/source/10M"]}
`
	testBytes := []byte(testData)
	backupSummary, err := getSummary(testBytes, nil)
	require.Nil(t, backupSummary, "expected nil backup summary")
	bErr, ok := err.(*cmdexec.Error)
	require.True(t, ok, "unexpected error type")
	require.Equal(t, bErr.Reason, "could not find backup summary")
}

func TestBackupExecutor(t *testing.T) {
	testSetup(t)
	defer testTeardown(t)

	bc, err := GetBackupCommand(testRepoName, testSecretFilePath, testSource)
	require.NoError(t, err, "unexpected error on GetBackupCommand")

	be := NewBackupExecutor(bc)
	require.NoError(t, be.Run(), "unexpected error on backup executor run")

	foundSummary := false
	// TODO: remove the dependency of this UT on time
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		status, err := be.Status()
		require.NoError(t, err, "unexpected error on backup executor status")
		require.NotNil(t, status, "unexpected nil status")
		if status.Done {
			require.Equal(t, float64(100), status.ProgressPercentage, "unexpected progress percentage")
			require.Equal(t, status.TotalBytes, status.TotalBytesProcessed, "unexpected total bytes")
			foundSummary = true
			break
		} else {

			require.True(t, status.TotalBytesProcessed <= status.TotalBytes, "unexpected total bytes")
		}
	}
	require.True(t, foundSummary, "expected a summary")
}

func testTeardown(t *testing.T) {
	require.NoError(t, os.RemoveAll(testSource))
	require.NoError(t, os.RemoveAll(testRepoName))
	require.NoError(t, os.RemoveAll(testSecretFilePath))
}

func testSetup(t *testing.T) {
	testTeardown(t)

	err := os.WriteFile(testSecretFilePath, []byte("test123"), 0644)
	require.NoError(t, err, "unexpected error on creating secret file")

	cmd := exec.Command("restic", "-r", testRepoName, "-p", testSecretFilePath, "init")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "unexpected error on repo init: ", string(out))

	require.NoError(t, os.MkdirAll(testSource, os.ModeDir))
	ddCmd := exec.Command("dd", "if=/dev/zero", "of=/tmp/source/10M", "bs=1M", "count=10")
	out, err = ddCmd.CombinedOutput()
	require.NoError(t, err, "unexpected error on source generation: ", string(out))
}
