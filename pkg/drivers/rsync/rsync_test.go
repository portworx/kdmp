package rsync

import (
	"fmt"
	"testing"

	"github.com/portworx/kdmp/pkg/drivers/utils"
	"github.com/stretchr/testify/require"
)

func TestToJobName(t *testing.T) {
	id := "1234567890"
	// uid is a 32 character string
	uid := "abcdefghijklmnopqrstuvwxyzabcdef"

	expectedJobName := fmt.Sprintf("import-rsync-%s-%s", id, utils.GetShortUID(uid))
	actualJobName := toJobName(id, uid)

	require.Equal(t, expectedJobName, actualJobName, "unexpected job name")

	// Test when the job name exceeds the maximum length
	longID := "1234567890123456789012345678901234567890123456789012345678901234567890"
	expectedJobName = fmt.Sprintf("import-rsync-%s-%s", longID[:41], utils.GetShortUID(uid))
	actualJobName = toJobName(longID, uid)

	require.Equal(t, expectedJobName, actualJobName, "unexpected job name")

	// Test when the job name exceeds the maximum length and the UID is empty
	expectedJobName = fmt.Sprintf("import-rsync-%s-%s", longID[:49], "")
	actualJobName = toJobName(longID, "")

	require.Equal(t, expectedJobName, actualJobName, "unexpected job name")

	// Test when the ID is empty
	expectedJobName = fmt.Sprintf("import-rsync-%s-%s", "", utils.GetShortUID(uid))
	actualJobName = toJobName("", uid)

	require.Equal(t, expectedJobName, actualJobName, "unexpected job name")

	// Test when the ID and UID are empty
	expectedJobName = fmt.Sprintf("import-rsync-%s-%s", "", "")
	actualJobName = toJobName("", "")

	require.Equal(t, expectedJobName, actualJobName, "unexpected job name")
}
