package domain

import "context"

type Device struct {
	ID           int64
	APNSToken    string
	Sandbox      bool
	LastPingedAt int64
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
