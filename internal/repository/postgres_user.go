package repository

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/christianselig/apollo-backend/internal/domain"
)

type postgresUserRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresUser(pool *pgxpool.Pool) domain.UserRepository {
	return &postgresUserRepository{pool: pool}
}

func (p *postgresUserRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.User, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var uu []domain.User
	for rows.Next() {
		var u domain.User
		if err := rows.Scan(
			&u.ID,
			&u.UserID,
			&u.Name,
			&u.LastCheckedAt,
		); err != nil {
			return nil, err
		}
		uu = append(uu, u)
	}
	return uu, nil
}

func (p *postgresUserRepository) GetByID(ctx context.Context, id int64) (domain.User, error) {
	query := `
		SELECT id, user_id, name, last_checked_at
		FROM users
		WHERE id = $1`

	srs, err := p.fetch(ctx, query, id)

	if err != nil {
		return domain.User{}, err
	}
	if len(srs) == 0 {
		return domain.User{}, domain.ErrNotFound
	}
	return srs[0], nil
}

func (p *postgresUserRepository) GetByName(ctx context.Context, name string) (domain.User, error) {
	query := `
		SELECT id, user_id, name, last_checked_at
		FROM users
		WHERE name = $1`

	name = strings.ToLower(name)

	srs, err := p.fetch(ctx, query, name)

	if err != nil {
		return domain.User{}, err
	}
	if len(srs) == 0 {
		return domain.User{}, domain.ErrNotFound
	}
	return srs[0], nil
}

func (p *postgresUserRepository) CreateOrUpdate(ctx context.Context, u *domain.User) error {
	if err := u.Validate(); err != nil {
		return err
	}

	query := `
		INSERT INTO users (user_id, name)
		VALUES ($1, $2)
		ON CONFLICT(user_id) DO
			UPDATE SET last_checked_at = $3
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		u.UserID,
		u.NormalizedName(),
		u.LastCheckedAt,
	).Scan(&u.ID)
}

func (p *postgresUserRepository) Delete(ctx context.Context, id int64) error {
	query := `DELETE FROM users WHERE id = $1`
	res, err := p.pool.Exec(ctx, query, id)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}
