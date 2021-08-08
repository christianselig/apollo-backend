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
	CreateOrUpdate(ctx context.Context, dev *Device) error
	Update(ctx context.Context, dev *Device) error
	Create(ctx context.Context, dev *Device) error
	Delete(ctx context.Context, token string) error
}
