package repository

import (
	"context"
	"strings"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/jackc/pgx/v4/pgxpool"
)

type postgresSubredditRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresSubreddit(pool *pgxpool.Pool) domain.SubredditRepository {
	return &postgresSubredditRepository{pool: pool}
}

func (p *postgresSubredditRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.Subreddit, error) {
	rows, err := p.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var srs []domain.Subreddit
	for rows.Next() {
		var sr domain.Subreddit
		if err := rows.Scan(
			&sr.ID,
			&sr.SubredditID,
			&sr.Name,
			&sr.LastCheckedAt,
		); err != nil {
			return nil, err
		}
		srs = append(srs, sr)
	}
	return srs, nil
}

func (p *postgresSubredditRepository) GetByID(ctx context.Context, id int64) (domain.Subreddit, error) {
	query := `
		SELECT id, subreddit_id, name, last_checked_at
		FROM subreddits
		WHERE id = $1`

	srs, err := p.fetch(ctx, query, id)

	if err != nil {
		return domain.Subreddit{}, err
	}
	if len(srs) == 0 {
		return domain.Subreddit{}, domain.ErrNotFound
	}
	return srs[0], nil
}

func (p *postgresSubredditRepository) GetByName(ctx context.Context, name string) (domain.Subreddit, error) {
	query := `
		SELECT id, subreddit_id, name, last_checked_at
		FROM subreddits
		WHERE name = $1`

	name = strings.ToLower(name)

	srs, err := p.fetch(ctx, query, name)

	if err != nil {
		return domain.Subreddit{}, err
	}
	if len(srs) == 0 {
		return domain.Subreddit{}, domain.ErrNotFound
	}
	return srs[0], nil
}

func (p *postgresSubredditRepository) CreateOrUpdate(ctx context.Context, sr *domain.Subreddit) error {
	query := `
		INSERT INTO subreddits (subreddit_id, name)
		VALUES ($1, $2)
		ON CONFLICT(subreddit_id) DO NOTHING
		RETURNING id`

	return p.pool.QueryRow(
		ctx,
		query,
		sr.SubredditID,
		sr.NormalizedName(),
	).Scan(&sr.ID)
}
