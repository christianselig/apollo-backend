package data

import (
	"database/sql"
	"strings"
)

type Account struct {
	ID            int64
	Username      string
	AccountID     string
	AccessToken   string
	RefreshToken  string
	ExpiresAt     int64
	LastMessageID string
	LastCheckedAt float64
}

func (a *Account) NormalizedUsername() string {
	return strings.ToLower(a.Username)
}

type AccountModel struct {
	DB *sql.DB
}

func (am *AccountModel) Upsert(a *Account) error {
	query := `
		INSERT INTO accounts (username, account_id, access_token, refresh_token, expires_at, last_message_id, device_count, last_checked_at)
		VALUES ($1, $2, $3, $4, $5, '', 0, 0)
		ON CONFLICT(username)
		DO
			UPDATE SET
				access_token = $3,
				refresh_token = $4,
				expires_at = $5,
				last_message_id = $6,
				last_checked_at = $7
		RETURNING id`

	args := []interface{}{a.NormalizedUsername(), a.AccountID, a.AccessToken, a.RefreshToken, a.ExpiresAt, a.LastMessageID, a.LastCheckedAt}
	return am.DB.QueryRow(query, args...).Scan(&a.ID)
}

func (am *AccountModel) Delete(id int64) error {
	return nil
}

type MockAccountModel struct{}

func (mam *MockAccountModel) Upsert(a *Account) error {
	return nil
}

func (mam *MockAccountModel) Delete(id int64) error {
	return nil
}
