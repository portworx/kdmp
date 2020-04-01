package drivers

// Known drivers.
const (
	Rsync = "rsync"
)

const (
	// TransferProgressCompleted is a status for a data transfer.
	TransferProgressCompleted = 100
)

// Interface defines a data export driver behaviour.
type Interface interface {
	// Name returns a name of the driver.
	Name() string
	// StartJob creates a job for data transfer between volumes.
	StartJob(opts ...JobOption) (id string, err error)
	// DeleteJob stops data transfer between volumes.
	DeleteJob(id string) error
	// JobStatus returns a progress status for a data transfer.
	JobStatus(id string) (progress int, err error)
}

// IsTransferCompleted allows to check transfer status.
func IsTransferCompleted(progress int) bool {
	return progress == TransferProgressCompleted
}
