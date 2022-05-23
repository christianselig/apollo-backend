package repository

import (
	"context"
	"time"

	"github.com/christianselig/apollo-backend/internal/domain"
)

type postgresAccountRepository struct {
	conn Connection
}

func NewPostgresAccount(conn Connection) domain.AccountRepository {
	return &postgresAccountRepository{conn: conn}
}

func (p *postgresAccountRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.Account, error) {
	rows, err := p.conn.Query(ctx, query, args...)
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
			&acc.TokenExpiresAt,
			&acc.LastMessageID,
			&acc.NextNotificationCheckAt,
			&acc.NextStuckNotificationCheckAt,
			&acc.CheckCount,
		); err != nil {
			return nil, err
		}
		accs = append(accs, acc)
	}
	return accs, nil
}

func (p *postgresAccountRepository) GetByID(ctx context.Context, id int64) (domain.Account, error) {
	query := `
		SELECT id, username, reddit_account_id, access_token, refresh_token, token_expires_at,
			last_message_id, next_notification_check_at, next_stuck_notification_check_at,
			check_count
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
		SELECT id, username, reddit_account_id, access_token, refresh_token, token_expires_at,
			last_message_id, next_notification_check_at, next_stuck_notification_check_at,
			check_count
		FROM accounts
		WHERE reddit_account_id = $1`

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
		INSERT INTO accounts (username, reddit_account_id, access_token, refresh_token, token_expires_at,
			last_message_id, next_notification_check_at, next_stuck_notification_check_at)
		VALUES ($1, $2, $3, $4, $5, '', NOW(), NOW())
		ON CONFLICT(username) DO
			UPDATE SET access_token = $3,
				refresh_token = $4,
				token_expires_at = $5
		RETURNING id`

	return p.conn.QueryRow(
		ctx,
		query,
		acc.Username,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.TokenExpiresAt,
	).Scan(&acc.ID)
}

func (p *postgresAccountRepository) Create(ctx context.Context, acc *domain.Account) error {
	query := `
		INSERT INTO accounts
			(username, reddit_account_id, access_token, refresh_token, token_expires_at,
			last_message_id, next_notification_check_at, next_stuck_notification_check_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`

	return p.conn.QueryRow(
		ctx,
		query,
		acc.Username,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.TokenExpiresAt,
		acc.LastMessageID,
		acc.NextNotificationCheckAt,
		acc.NextStuckNotificationCheckAt,
	).Scan(&acc.ID)
}

func (p *postgresAccountRepository) Update(ctx context.Context, acc *domain.Account) error {
	query := `
		UPDATE accounts
		SET username = $2,
			reddit_account_id = $3,
			access_token = $4,
			refresh_token = $5,
			token_expires_at = $6,
			last_message_id = $7,
			next_notification_check_at = $8,
			next_stuck_notification_check_at = $9,
			check_count = $10
		WHERE id = $1`

	_, err := p.conn.Exec(
		ctx,
		query,
		acc.ID,
		acc.Username,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.TokenExpiresAt,
		acc.LastMessageID,
		acc.NextNotificationCheckAt,
		acc.NextStuckNotificationCheckAt,
		acc.CheckCount,
	)

	return err
}

func (p *postgresAccountRepository) Delete(ctx context.Context, id int64) error {
	query := `DELETE FROM accounts WHERE id = $1`
	_, err := p.conn.Exec(ctx, query, id)
	return err
}

func (p *postgresAccountRepository) Associate(ctx context.Context, acc *domain.Account, dev *domain.Device) error {
	query := `
		INSERT INTO devices_accounts
			(account_id, device_id)
		VALUES ($1, $2)
		ON CONFLICT(account_id, device_id) DO NOTHING`
	_, err := p.conn.Exec(ctx, query, acc.ID, dev.ID)
	return err
}

func (p *postgresAccountRepository) Disassociate(ctx context.Context, acc *domain.Account, dev *domain.Device) error {
	query := `DELETE FROM devices_accounts WHERE account_id = $1 AND device_id = $2`
	_, err := p.conn.Exec(ctx, query, acc.ID, dev.ID)
	return err
}

func (p *postgresAccountRepository) GetByAPNSToken(ctx context.Context, token string) ([]domain.Account, error) {
	query := `
		SELECT accounts.id, username, accounts.reddit_account_id, access_token, refresh_token, token_expires_at,
			last_message_id, next_notification_check_at, next_stuck_notification_check_at,
			check_count
		FROM accounts
		INNER JOIN devices_accounts ON accounts.id = devices_accounts.account_id
		INNER JOIN devices ON devices.id = devices_accounts.device_id
		WHERE devices.apns_token = $1`

	return p.fetch(ctx, query, token)
}

func (p *postgresAccountRepository) PruneStale(ctx context.Context, expiry time.Time) (int64, error) {
	query := `
		DELETE FROM accounts
		WHERE token_expires_at < $1`

	res, err := p.conn.Exec(ctx, query, expiry)

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

	res, err := p.conn.Exec(ctx, query)

	return res.RowsAffected(), err
}
