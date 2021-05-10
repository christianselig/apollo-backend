package data

import "database/sql"

type DeviceAccount struct {
	ID        int64
	AccountID int64
	DeviceID  int64
}

type DeviceAccountModel struct {
	DB *sql.DB
}

func (dam *DeviceAccountModel) Associate(accountID int64, deviceID int64) error {
	query := `
		INSERT INTO devices_accounts (account_id, device_id)
		VALUES ($1, $2)
		ON CONFLICT (account_id, device_id) DO NOTHING
		RETURNING id`

	args := []interface{}{accountID, deviceID}
	if err := dam.DB.QueryRow(query, args...).Err(); err != nil {
		return err
	}

	// Update account counter
	query = `
		UPDATE accounts
		SET device_count = (
			SELECT COUNT(*) FROM devices_accounts WHERE account_id = $1
		)
		WHERE id = $1`
	args = []interface{}{accountID}
	return dam.DB.QueryRow(query, args...).Err()
}

type MockDeviceAccountModel struct{}

func (mdam *MockDeviceAccountModel) Associate(accountID int64, deviceID int64) error {
	return nil
}
