package domain

import (
	"context"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const (
	DeviceGracePeriodDuration            = 3600           // 1 hour
	DeviceActiveAfterReceitCheckDuration = 3600 * 24 * 30 // ~1 month
)

type Device struct {
	ID          int64
	APNSToken   string
	Sandbox     bool
	ActiveUntil int64
}

func (dev *Device) Validate() error {
	return validation.ValidateStruct(dev,
		validation.Field(&dev.APNSToken, validation.Required, validation.Length(64, 200)),
	)
}

type DeviceRepository interface {
	GetByID(ctx context.Context, id int64) (Device, error)
	GetByAPNSToken(ctx context.Context, token string) (Device, error)
	GetInboxNotifiableByAccountID(ctx context.Context, id int64) ([]Device, error)
	GetWatcherNotifiableByAccountID(ctx context.Context, id int64) ([]Device, error)
	GetByAccountID(ctx context.Context, id int64) ([]Device, error)

	CreateOrUpdate(ctx context.Context, dev *Device) error
	Update(ctx context.Context, dev *Device) error
	Create(ctx context.Context, dev *Device) error
	Delete(ctx context.Context, token string) error
	SetNotifiable(ctx context.Context, dev *Device, acct *Account, inbox, watcher bool) error
	GetNotifiable(ctx context.Context, dev *Device, acct *Account) (bool, bool, error)

	PruneStale(ctx context.Context, before int64) (int64, error)
}
