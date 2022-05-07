package repository

import (
	"context"
	"strings"

	"github.com/christianselig/apollo-backend/internal/domain"
)

type postgresSubredditRepository struct {
	conn Connection
}

func NewPostgresSubreddit(conn Connection) domain.SubredditRepository {
	return &postgresSubredditRepository{conn: conn}
}

func (p *postgresSubredditRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.Subreddit, error) {
	rows, err := p.conn.Query(ctx, query, args...)
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
			&sr.NextCheckAt,
		); err != nil {
			return nil, err
		}
		srs = append(srs, sr)
	}
	return srs, nil
}

func (p *postgresSubredditRepository) GetByID(ctx context.Context, id int64) (domain.Subreddit, error) {
	query := `
		SELECT id, subreddit_id, name, next_check_at
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
		SELECT id, subreddit_id, name, next_check_at
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
	if err := sr.Validate(); err != nil {
		return err
	}

	query := `
		INSERT INTO subreddits (subreddit_id, name, next_check_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT(subreddit_id) DO NOTHING
		RETURNING id`

	return p.conn.QueryRow(
		ctx,
		query,
		sr.SubredditID,
		sr.NormalizedName(),
	).Scan(&sr.ID)
}
