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
		var subredditLabel, userLabel string

		if err := rows.Scan(
			&watcher.ID,
			&watcher.CreatedAt,
			&watcher.LastNotifiedAt,
			&watcher.Label,
			&watcher.DeviceID,
			&watcher.AccountID,
			&watcher.Type,
			&watcher.WatcheeID,
			&watcher.Author,
			&watcher.Subreddit,
			&watcher.Upvotes,
			&watcher.Keyword,
			&watcher.Flair,
			&watcher.Domain,
			&watcher.Hits,
			&watcher.Device.ID,
			&watcher.Device.APNSToken,
			&watcher.Device.Sandbox,
			&watcher.Account.ID,
			&watcher.Account.AccessToken,
			&watcher.Account.RefreshToken,
			&subredditLabel,
			&userLabel,
		); err != nil {
			return nil, err
		}

		switch watcher.Type {
		case domain.SubredditWatcher, domain.TrendingWatcher:
			watcher.WatcheeLabel = subredditLabel
		case domain.UserWatcher:
			watcher.WatcheeLabel = userLabel
		}

		watchers = append(watchers, watcher)
	}
	return watchers, nil
}

func (p *postgresWatcherRepository) GetByID(ctx context.Context, id int64) (domain.Watcher, error) {
	query := `
		SELECT
			watchers.id,
			watchers.created_at,
			watchers.last_notified_at,
			watchers.label,
			watchers.device_id,
			watchers.account_id,
			watchers.type,
			watchers.watchee_id,
			watchers.author,
			watchers.subreddit,
			watchers.upvotes,
			watchers.keyword,
			watchers.flair,
			watchers.domain,
			watchers.hits,
			devices.id,
			devices.apns_token,
			devices.sandbox,
			accounts.id,
			accounts.access_token,
			accounts.refresh_token,
			subreddits.name AS subreddit_label,
			users.name AS user_label
		FROM watchers
		INNER JOIN devices ON watchers.device_id = devices.id
		INNER JOIN accounts ON watchers.account_id = accounts.id
		LEFT JOIN subreddits ON watchers.type IN(0,2) AND watchers.watchee_id = subreddits.id
		LEFT JOIN users ON watchers.type = 1 AND watchers.watchee_id = users.id
		WHERE watchers.id = $1`

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
		SELECT
			watchers.id,
			watchers.created_at,
			watchers.last_notified_at,
			watchers.label,
			watchers.device_id,
			watchers.account_id,
			watchers.type,
			watchers.watchee_id,
			watchers.author,
			watchers.subreddit,
			watchers.upvotes,
			watchers.keyword,
			watchers.flair,
			watchers.domain,
			watchers.hits,
			devices.id,
			devices.apns_token,
			devices.sandbox,
			accounts.id,
			accounts.access_token,
			accounts.refresh_token
		FROM watchers
		INNER JOIN devices ON watchers.device_id = devices.id
		INNER JOIN accounts ON watchers.account_id = accounts.id
		LEFT JOIN subreddits ON watchers.type IN(0,2) AND watchers.watchee_id = subreddits.id
		LEFT JOIN users ON watchers.type = 1 AND watchers.watchee_id = users.id
		WHERE watchers.type = $1 AND watchers.watchee_id = $2`

	return p.fetch(ctx, query, typ, id)
}

func (p *postgresWatcherRepository) GetByTrendingSubredditID(ctx context.Context, id int64) ([]domain.Watcher, error) {
	return p.GetByTypeAndWatcheeID(ctx, domain.TrendingWatcher, id)
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
			watchers.last_notified_at,
			watchers.label,
			watchers.device_id,
			watchers.account_id,
			watchers.type,
			watchers.watchee_id,
			watchers.author,
			watchers.subreddit,
			watchers.upvotes,
			watchers.keyword,
			watchers.flair,
			watchers.domain,
			watchers.hits,
			devices.id,
			devices.apns_token,
			devices.sandbox,
			accounts.id,
			accounts.access_token,
			accounts.refresh_token
		FROM watchers
		INNER JOIN accounts ON watchers.account_id = accounts.id
		INNER JOIN devices ON watchers.device_id = devices.id
		LEFT JOIN subreddits ON watchers.type IN(0,2) AND watchers.watchee_id = subreddits.id
		LEFT JOIN users ON watchers.type = 1 AND watchers.watchee_id = users.id
		WHERE
			devices.apns_token = $1 AND
			accounts.account_id = $2`

	return p.fetch(ctx, query, apns, rid)
}

func (p *postgresWatcherRepository) Create(ctx context.Context, watcher *domain.Watcher) error {
	if err := watcher.Validate(); err != nil {
		return err
	}

	now := float64(time.Now().UTC().Unix())

	query := `
		INSERT INTO watchers
			(created_at, last_notified_at, label, device_id, account_id, type, watchee_id, author, subreddit, upvotes, keyword, flair, domain)
		VALUES ($1, 0, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		now,
		watcher.Label,
		watcher.DeviceID,
		watcher.AccountID,
		watcher.Type,
		watcher.WatcheeID,
		watcher.Author,
		watcher.Subreddit,
		watcher.Upvotes,
		watcher.Keyword,
		watcher.Flair,
		watcher.Domain,
	).Scan(&watcher.ID)
}

func (p *postgresWatcherRepository) Update(ctx context.Context, watcher *domain.Watcher) error {
	if err := watcher.Validate(); err != nil {
		return err
	}

	query := `
		UPDATE watchers
		SET author = $2,
			subreddit = $3,
			upvotes = $4,
			keyword = $5,
			flair = $6,
			domain = $7,
			label = $8
		WHERE id = $1`

	res, err := p.pool.Exec(
		ctx,
		query,
		watcher.ID,
		watcher.Author,
		watcher.Subreddit,
		watcher.Upvotes,
		watcher.Keyword,
		watcher.Flair,
		watcher.Domain,
		watcher.Label,
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
