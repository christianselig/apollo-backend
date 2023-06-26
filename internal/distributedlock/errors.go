package distributedlock

import "errors"

var (
	ErrLockAcquisitionTimeout = errors.New("timed out acquiring lock")
	ErrLockAlreadyAcquired    = errors.New("lock already acquired")
	ErrLockExpired            = errors.New("releasing an expired lock")
)
