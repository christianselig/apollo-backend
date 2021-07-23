package data

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
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
	ctx  context.Context
	pool *pgxpool.Pool
}

func (am *AccountModel) Upsert(a *Account) error {
	return am.pool.BeginFunc(am.ctx, func(tx pgx.Tx) error {
		stmt := `
				INSERT INTO accounts (username, account_id, access_token, refresh_token, expires_at, last_message_id, device_count, last_checked_at)
					VALUES ($1, $2, $3, $4, $5, '', 0, 0)
					ON CONFLICT(username)
					DO
						UPDATE SET
							access_token = $3,
							refresh_token = $4,
							expires_at = $5
					RETURNING id`
		return tx.QueryRow(
			am.ctx,
			stmt,
			a.NormalizedUsername(),
			a.AccountID,
			a.AccessToken,
			a.RefreshToken,
			a.ExpiresAt,
		).Scan(&a.ID)
	})
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
