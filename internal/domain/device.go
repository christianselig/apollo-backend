package domain

import (
	"context"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const (
	DeviceReceiptCheckPeriodDuration     = 4 * time.Hour
	DeviceActiveAfterReceitCheckDuration = 30 * 24 * time.Hour // ~1 month
	DeviceGracePeriodAfterReceiptExpiry  = 30 * 24 * time.Hour // ~1 month
)

type Device struct {
	ID                   int64
	APNSToken            string
	Sandbox              bool
	ExpiresAt            time.Time
	GracePeriodExpiresAt time.Time
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
	SetNotifiable(ctx context.Context, dev *Device, acct *Account, inbox, watcher, global bool) error
	GetNotifiable(ctx context.Context, dev *Device, acct *Account) (bool, bool, bool, error)

	PruneStale(ctx context.Context, expiry time.Time) (int64, error)
}
