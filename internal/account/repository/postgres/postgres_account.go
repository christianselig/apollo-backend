package postgres

import (
	"context"
	"fmt"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/jackc/pgx/v4/pgxpool"
)

type postgresAccountRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresAccountRepository(pool *pgxpool.Pool) domain.AccountRepository {
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
		); err != nil {
			return nil, err
		}
		accs = append(accs, acc)
	}

	return accs, nil
}

func (p *postgresAccountRepository) GetByID(ctx context.Context, id int64) (domain.Account, error) {
	query := `
		SELECT id, username, account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at
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
		SELECT id, username, account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at
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

func (p *postgresAccountRepository) Create(ctx context.Context, acc *domain.Account) error {
	query := `
		INSERT INTO accounts
			(username, account_id, access_token, refresh_token, expires_at, last_message_id, last_checked_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id`

	res, err := p.pool.Query(
		ctx,
		query,
		acc.Username,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.ExpiresAt,
		acc.LastMessageID,
		acc.LastCheckedAt,
	)
	if err != nil {
		return err
	}

	return res.Scan(&acc.ID)
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
			last_checked_at = $8
		WHERE id = $1`

	res, err := p.pool.Exec(
		ctx,
		query,
		acc.AccountID,
		acc.AccessToken,
		acc.RefreshToken,
		acc.ExpiresAt,
		acc.LastMessageID,
		acc.LastCheckedAt,
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
