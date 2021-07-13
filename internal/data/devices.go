package data

import (
	"context"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type Device struct {
	ID           int64
	APNSToken    string
	Sandbox      bool
	LastPingedAt int64
}

type DeviceModel struct {
	ctx  context.Context
	pool *pgxpool.Pool
}

func (dm *DeviceModel) Upsert(d *Device) error {
	d.LastPingedAt = time.Now().Unix()

	return dm.pool.BeginFunc(dm.ctx, func(tx pgx.Tx) error {
		stmt := `
			INSERT INTO devices (apns_token, sandbox, last_pinged_at)
			VALUES ($1, $2, $3)
			ON CONFLICT(apns_token)
			DO
				UPDATE SET last_pinged_at = $3
			RETURNING id`
		return tx.QueryRow(
			dm.ctx,
			stmt,
			d.APNSToken,
			d.Sandbox,
			d.LastPingedAt,
		).Scan(&d.ID)
	})
}

func (dm *DeviceModel) GetByAPNSToken(token string) (*Device, error) {
	device := &Device{}
	stmt := `
		SELECT id, apns_token, sandbox, last_pinged_at
		FROM devices
		WHERE apns_token = $1`

	if err := dm.pool.QueryRow(dm.ctx, stmt, token).Scan(
		&device.ID,
		&device.APNSToken,
		&device.Sandbox,
		&device.LastPingedAt,
	); err != nil {
		return nil, err
	}
	return device, nil
}

type MockDeviceModel struct{}

func (mdm *MockDeviceModel) Upsert(d *Device) error {
	return nil
}
func (mdm *MockDeviceModel) GetByAPNSToken(token string) (*Device, error) {
	return nil, nil
}
