package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/christianselig/apollo-backend/internal/domain"
)

type postgresWatcherRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresWatcher(pool *pgxpool.Pool) domain.WatcherRepository {
	return &postgresWatcherRepository{pool: pool}
}

func (p *postgresWatcherRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.Watcher, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var watchers []domain.Watcher
	for rows.Next() {
		var watcher domain.Watcher
		if err := rows.Scan(
			&watcher.ID,
			&watcher.CreatedAt,
			&watcher.DeviceID,
			&watcher.AccountID,
			&watcher.SubredditID,
			&watcher.Upvotes,
			&watcher.Keyword,
			&watcher.Flair,
			&watcher.Domain,
		); err != nil {
			return nil, err
		}
		watchers = append(watchers, watcher)
	}
	return watchers, nil
}

func (p *postgresWatcherRepository) GetByID(ctx context.Context, id int64) (domain.Watcher, error) {
	query := `
		SELECT id, created_at, device_id, account_id, subreddit_id, upvotes, keyword, flair, domain
		FROM watchers
		WHERE id = $1`

	watchers, err := p.fetch(ctx, query, id)

	if err != nil {
		return domain.Watcher{}, err
	}
	if len(watchers) == 0 {
		return domain.Watcher{}, domain.ErrNotFound
	}
	return watchers[0], nil
}

func (p *postgresWatcherRepository) GetBySubredditID(ctx context.Context, id int64) ([]domain.Watcher, error) {
	query := `
		SELECT id, created_at, device_id, account_id, subreddit_id, upvotes, keyword, flair, domain
		FROM watchers
		WHERE subreddit_id = $1`

	return p.fetch(ctx, query, id)
}

func (p *postgresWatcherRepository) Create(ctx context.Context, watcher *domain.Watcher) error {
	now := float64(time.Now().UTC().Unix())

	query := `
		INSERT INTO watchers
			(created_at, device_id, account_id, subreddit_id, upvotes, keyword, flair, domain)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		now,
		watcher.DeviceID,
		watcher.AccountID,
		watcher.SubredditID,
		watcher.Upvotes,
		watcher.Keyword,
		watcher.Flair,
		watcher.Domain,
	).Scan(&watcher.ID)
}

func (p *postgresWatcherRepository) Update(ctx context.Context, watcher *domain.Watcher) error {
	query := `
		UPDATE watchers
		SET upvotes = $2,
			keyword = $3,
			flair = $4,
			domain = $5,
		WHERE id = $1`

	res, err := p.pool.Exec(
		ctx,
		query,
		watcher.ID,
		watcher.Upvotes,
		watcher.Keyword,
		watcher.Flair,
		watcher.Domain,
	)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}

func (p *postgresWatcherRepository) Delete(ctx context.Context, id int64) error {
	query := `DELETE FROM watchers WHERE id = $1`
	res, err := p.pool.Exec(ctx, query, id)

	if res.RowsAffected() != 1 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}
