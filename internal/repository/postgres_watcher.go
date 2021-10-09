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
			&watcher.LastNotifiedAt,
			&watcher.DeviceID,
			&watcher.AccountID,
			&watcher.Type,
			&watcher.WatcheeID,
			&watcher.Upvotes,
			&watcher.Keyword,
			&watcher.Flair,
			&watcher.Domain,
			&watcher.Hits,
		); err != nil {
			return nil, err
		}
		watchers = append(watchers, watcher)
	}
	return watchers, nil
}

func (p *postgresWatcherRepository) GetByID(ctx context.Context, id int64) (domain.Watcher, error) {
	query := `
		SELECT id, created_at, last_notified_at, device_id, account_id, type, watchee_id, upvotes, keyword, flair, domain, hits
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

func (p *postgresWatcherRepository) GetByTypeAndWatcheeID(ctx context.Context, typ domain.WatcherType, id int64) ([]domain.Watcher, error) {
	query := `
		SELECT id, created_at, last_notified_at, device_id, account_id, type, watchee_id, upvotes, keyword, flair, domain, hits
		FROM watchers
		WHERE type = $1 AND watchee_id = $2`

	return p.fetch(ctx, query, typ, id)
}

func (p *postgresWatcherRepository) GetBySubredditID(ctx context.Context, id int64) ([]domain.Watcher, error) {
	return p.GetByTypeAndWatcheeID(ctx, domain.SubredditWatcher, id)
}

func (p *postgresWatcherRepository) GetByUserID(ctx context.Context, id int64) ([]domain.Watcher, error) {
	return p.GetByTypeAndWatcheeID(ctx, domain.UserWatcher, id)
}

func (p *postgresWatcherRepository) GetByDeviceAPNSTokenAndAccountRedditID(ctx context.Context, apns string, rid string) ([]domain.Watcher, error) {
	query := `
		SELECT
			watchers.id,
			watchers.created_at,
			watchers.last_notified_at
			watchers.device_id,
			watchers.account_id,
			watchers.type,
			watchers.watchee_id,
			watchers.upvotes,
			watchers.keyword,
			watchers.flair,
			watchers.domain,
			watchers.hits
		FROM watchers
		INNER JOIN accounts ON watchers.account_id = accounts.id
		INNER JOIN devices ON watchers.device_id = devices.id
		WHERE
			devices.apns_token = $1 AND
			accounts.account_id = $2`

	return p.fetch(ctx, query, apns, rid)
}

func (p *postgresWatcherRepository) Create(ctx context.Context, watcher *domain.Watcher) error {
	now := float64(time.Now().UTC().Unix())

	query := `
		INSERT INTO watchers
			(created_at, last_notified_at, device_id, account_id, type, watchee_id, upvotes, keyword, flair, domain)
		VALUES ($1, 0, $2, $3, $4, $5, $6, $7, $8, $9)
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		now,
		watcher.DeviceID,
		watcher.AccountID,
		watcher.Type,
		watcher.WatcheeID,
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

func (p *postgresWatcherRepository) IncrementHits(ctx context.Context, id int64) error {
	now := time.Now().Unix()
	query := `UPDATE watchers SET hits = hits + 1, last_notified_at = $2 WHERE id = $1`
	res, err := p.pool.Exec(ctx, query, id, now)

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

func (p *postgresWatcherRepository) DeleteByTypeAndWatcheeID(ctx context.Context, typ domain.WatcherType, id int64) error {
	query := `DELETE FROM watchers WHERE type = $1 AND watchee_id = $2`
	res, err := p.pool.Exec(ctx, query, typ, id)

	if res.RowsAffected() == 0 {
		return fmt.Errorf("weird behaviour, total rows affected: %d", res.RowsAffected())
	}
	return err
}
