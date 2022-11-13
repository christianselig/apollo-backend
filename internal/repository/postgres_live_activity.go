package repository

import (
	"context"
	"time"

	"github.com/christianselig/apollo-backend/internal/domain"
)

type postgresLiveActivityRepository struct {
	conn Connection
}

func NewPostgresLiveActivity(conn Connection) domain.LiveActivityRepository {
	return &postgresLiveActivityRepository{conn: conn}
}

func (p *postgresLiveActivityRepository) fetch(ctx context.Context, query string, args ...interface{}) ([]domain.LiveActivity, error) {
	rows, err := p.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var las []domain.LiveActivity
	for rows.Next() {
		var la domain.LiveActivity
		if err := rows.Scan(
			&la.ID,
			&la.APNSToken,
			&la.RedditAccountID,
			&la.AccessToken,
			&la.RefreshToken,
			&la.TokenExpiresAt,
			&la.ThreadID,
			&la.Subreddit,
			&la.NextCheckAt,
			&la.ExpiresAt,
			&la.Development,
		); err != nil {
			return nil, err
		}
		las = append(las, la)
	}
	return las, nil
}

func (p *postgresLiveActivityRepository) Get(ctx context.Context, apnsToken string) (domain.LiveActivity, error) {
	query := `
		SELECT id, apns_token, reddit_account_id, access_token, refresh_token, token_expires_at, thread_id, subreddit, next_check_at, expires_at, development
		FROM live_activities
		WHERE apns_token = $1`

	las, err := p.fetch(ctx, query, apnsToken)

	if err != nil {
		return domain.LiveActivity{}, err
	}
	if len(las) == 0 {
		return domain.LiveActivity{}, domain.ErrNotFound
	}
	return las[0], nil
}

func (p *postgresLiveActivityRepository) List(ctx context.Context) ([]domain.LiveActivity, error) {
	query := `
		SELECT id, apns_token, reddit_account_id, access_token, refresh_token, token_expires_at, thread_id, subreddit, next_check_at, expires_at, development
		FROM live_activities
		WHERE expires_at > NOW()`

	return p.fetch(ctx, query)
}

func (p *postgresLiveActivityRepository) Create(ctx context.Context, la *domain.LiveActivity) error {
	query := `
		INSERT INTO live_activities (apns_token, reddit_account_id, access_token, refresh_token, token_expires_at, thread_id, subreddit, next_check_at, expires_at, development)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (apns_token) DO UPDATE SET expires_at = $10
		RETURNING id`

	return p.conn.QueryRow(ctx, query,
		la.APNSToken,
		la.RedditAccountID,
		la.AccessToken,
		la.RefreshToken,
		la.TokenExpiresAt,
		la.ThreadID,
		la.Subreddit,
		time.Now(),
		time.Now().Add(domain.LiveActivityDuration),
		la.Development,
	).Scan(&la.ID)
}

func (p *postgresLiveActivityRepository) Update(ctx context.Context, la *domain.LiveActivity) error {
	query := `
		UPDATE live_activities
		SET access_token = $1, refresh_token = $2, token_expires_at = $3, next_check_at = $4
		WHERE id = $5`

	_, err := p.conn.Exec(ctx, query,
		la.AccessToken,
		la.RefreshToken,
		la.TokenExpiresAt,
		la.NextCheckAt,
		la.ID,
	)
	return err
}

func (p *postgresLiveActivityRepository) RemoveStale(ctx context.Context) error {
	query := `DELETE FROM live_activities WHERE expires_at < NOW()`

	_, err := p.conn.Exec(ctx, query)
	return err
}

func (p *postgresLiveActivityRepository) Delete(ctx context.Context, apns_token string) error {
	query := `DELETE FROM live_activities WHERE apns_token = $1`

	_, err := p.conn.Exec(ctx, query, apns_token)
	return err
}
