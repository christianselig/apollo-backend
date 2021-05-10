package data

import (
	"database/sql"
	"errors"
	"time"
)

type Device struct {
	ID           int64
	APNSToken    string
	Sandbox      bool
	LastPingedAt int64
}

type DeviceModel struct {
	DB *sql.DB
}

func (dm *DeviceModel) Upsert(d *Device) error {
	d.LastPingedAt = time.Now().Unix()

	query := `
		INSERT INTO devices (apns_token, sandbox, last_pinged_at)
		VALUES ($1, $2, $3)
		ON CONFLICT(apns_token)
		DO
			UPDATE SET last_pinged_at = $3
		RETURNING id`

	args := []interface{}{d.APNSToken, d.Sandbox, d.LastPingedAt}
	return dm.DB.QueryRow(query, args...).Scan(&d.ID)
}

func (dm *DeviceModel) GetByAPNSToken(token string) (*Device, error) {
	query := `
		SELECT id, apns_token, sandbox, last_pinged_at
		FROM devices
		WHERE apns_token = $1`

	device := &Device{}
	err := dm.DB.QueryRow(query, token).Scan(
		&device.ID,
		&device.APNSToken,
		&device.Sandbox,
		&device.LastPingedAt,
	)

	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, ErrRecordNotFound
		default:
			return nil, err
		}
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
