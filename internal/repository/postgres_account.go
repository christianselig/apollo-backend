package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/christianselig/apollo-backend/internal/domain"
)

type postgresAccountRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresAccount(pool *pgxpool.Pool) domain.AccountRepository {
	return &postgresAccountRepository{pool: pool}
}

func (p *postgresAccountRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.Account, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accs []domain.Account
	for rows.Next() {
		var acc domain.Account
		if err := rows.Scan(
			&acc.ID,
			&acc.Username,
			&acc.AccountID,
			&acc.AccessToken,
			&acc.RefreshToken,
			&acc.ExpiresAt,
			&acc.LastMessageID,
			&acc.LastCheckedAt,
			&acc.LastUnstuckAt,
		); err != nil {
			return nil, err
		}
		accs = append(accs, acc)
	}
	return accs, nil
}

func (p *postgresAccountRepository) GetByID(ctx context.Context, id int64) (domain.Account, error) {
	query := `
		SELECT id, username, account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at, last_unstuck_at
		FROM accounts
		WHERE id = $1`

	accs, err := p.fetch(ctx, query, id)
	if err != nil {
		return domain.Account{}, err
	}

	if len(accs) == 0 {
		return domain.Account{}, domain.ErrNotFound
	}
	return accs[0], nil
}

func (p *postgresAccountRepository) GetByRedditID(ctx context.Context, id string) (domain.Account, error) {
	query := `
		SELECT id, username, account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at, last_unstuck_at
		FROM accounts
		WHERE account_id = $1`

	accs, err := p.fetch(ctx, query, id)
	if err != nil {
		return domain.Account{}, err
	}

	if len(accs) == 0 {
		return domain.Account{}, domain.ErrNotFound
	}

	return accs[0], nil
}
func (p *postgresAccountRepository) CreateOrUpdate(ctx context.Context, acc *domain.Account) error {
	query := `
		INSERT INTO accounts (username, account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at, last_unstuck_at)
		VALUES ($1, $2, $3, $4, $5, '', 0, 0)
		ON CONFLICT(username) DO
			UPDATE SET access_token = $3,
				refresh_token = $4,
				expires_at = $5
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		acc.Username,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.ExpiresAt,
	).Scan(&acc.ID)
}

func (p *postgresAccountRepository) Create(ctx context.Context, acc *domain.Account) error {
	query := `
		INSERT INTO accounts
			(username, account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at, last_unstuck_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		acc.Username,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.ExpiresAt,
		acc.LastMessageID,
		acc.LastCheckedAt,
		acc.LastUnstuckAt,
	).Scan(&acc.ID)
}

func (p *postgresAccountRepository) Update(ctx context.Context, acc *domain.Account) error {
	query := `
		UPDATE accounts
		SET username = $2,
			account_id = $3,
			access_token = $4,
			refresh_token = $5,
			expires_at = $6,
			last_message_id = $7,
			last_checked_at = $8,
			last_unstuck_at = $9
		WHERE id = $1`

	res, err := p.pool.Exec(
		ctx,
		query,
		acc.ID,
		acc.Username,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.ExpiresAt,
		acc.LastMessageID,
		acc.LastCheckedAt,
		acc.LastUnstuckAt,
	)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}

func (p *postgresAccountRepository) Delete(ctx context.Context, id int64) error {
	query := `DELETE FROM accounts WHERE id = $1`
	res, err := p.pool.Exec(ctx, query, id)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}

func (p *postgresAccountRepository) Associate(ctx context.Context, acc *domain.Account, dev *domain.Device) error {
	query := `
		INSERT INTO devices_accounts
			(account_id, device_id)
		VALUES ($1, $2)
		ON CONFLICT(account_id, device_id) DO NOTHING`
	_, err := p.pool.Exec(ctx, query, acc.ID, dev.ID)
	return err
}

func (p *postgresAccountRepository) Disassociate(ctx context.Context, acc *domain.Account, dev *domain.Device) error {
	query := `DELETE FROM devices_accounts WHERE account_id = $1 AND device_id = $2`
	res, err := p.pool.Exec(ctx, query, acc.ID, dev.ID)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}

func (p *postgresAccountRepository) GetByAPNSToken(ctx context.Context, token string) ([]domain.Account, error) {
	query := `
		SELECT accounts.id, username, accounts.account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at
		FROM accounts
		INNER JOIN devices_accounts ON accounts.id = devices_accounts.account_id
		INNER JOIN devices ON devices.id = devices_accounts.device_id
		WHERE devices.apns_token = $1`

	return p.fetch(ctx, query, token)
}

func (p *postgresAccountRepository) PruneStale(ctx context.Context, before int64) (int64, error) {
	query := `
		DELETE FROM accounts
		WHERE expires_at < $1`

	res, err := p.pool.Exec(ctx, query, before)

	return res.RowsAffected(), err
}

func (p *postgresAccountRepository) PruneOrphaned(ctx context.Context) (int64, error) {
	query := `
		WITH accounts_with_device_count AS (
			SELECT accounts.id, COUNT(device_id) AS device_count
			FROM accounts
			LEFT JOIN devices_accounts ON accounts.id = devices_accounts.account_id
			GROUP BY accounts.id
		)
		DELETE FROM accounts WHERE id IN (
			SELECT id
			FROM accounts_with_device_count
			WHERE device_count = 0
		)`

	res, err := p.pool.Exec(ctx, query)

	return res.RowsAffected(), err
}
