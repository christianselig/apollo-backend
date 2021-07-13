package data

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
)

type DeviceAccount struct {
	ID        int64
	AccountID int64
	DeviceID  int64
}

type DeviceAccountModel struct {
	ctx  context.Context
	pool *pgxpool.Pool
}

func (dam *DeviceAccountModel) Associate(accountID int64, deviceID int64) error {
	stmt := `
		INSERT INTO devices_accounts (account_id, device_id)
		VALUES ($1, $2)
		ON CONFLICT (account_id, device_id) DO NOTHING
		RETURNING id`
	if _, err := dam.pool.Exec(dam.ctx, stmt, accountID, deviceID); err != nil {
		return err
	}

	// Update account counter
	stmt = `
		UPDATE accounts
		SET device_count = (
			SELECT COUNT(*) FROM devices_accounts WHERE account_id = $1
		)
		WHERE id = $1`
	_, err := dam.pool.Exec(dam.ctx, stmt, accountID)
	return err
}

type MockDeviceAccountModel struct{}

func (mdam *MockDeviceAccountModel) Associate(accountID int64, deviceID int64) error {
	return nil
}
