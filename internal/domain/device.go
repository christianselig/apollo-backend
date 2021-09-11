package domain

import "context"

const (
	DeviceGracePeriodDuration            = 3600          // 1 hour
	DeviceActiveAfterReceitCheckDuration = 3600 * 24 * 7 // 1 week
)

type Device struct {
	ID          int64
	APNSToken   string
	Sandbox     bool
	ActiveUntil int64
}

type DeviceRepository interface {
	GetByAPNSToken(ctx context.Context, token string) (Device, error)
	GetByAccountID(ctx context.Context, id int64) ([]Device, error)

	CreateOrUpdate(ctx context.Context, dev *Device) error
	Update(ctx context.Context, dev *Device) error
	Create(ctx context.Context, dev *Device) error
	Delete(ctx context.Context, token string) error

	PruneStale(ctx context.Context, before int64) (int64, error)
}
