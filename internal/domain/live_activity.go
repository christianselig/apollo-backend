package domain

import (
	"context"
	"time"
)

const (
	LiveActivityDuration      = 75 * time.Minute
	LiveActivityCheckInterval = 30 * time.Second
)

type LiveActivity struct {
	ID          int64
	APNSToken   string `json:"apns_token"`
	Development bool   `json:"development"`

	RedditAccountID string `json:"reddit_account_id"`
	AccessToken     string `json:"access_token"`
	RefreshToken    string `json:"refresh_token"`
	TokenExpiresAt  time.Time

	ThreadID    string `json:"thread_id"`
	Subreddit   string `json:"subreddit"`
	NextCheckAt time.Time
	ExpiresAt   time.Time
}

type LiveActivityRepository interface {
	Get(ctx context.Context, apnsToken string) (LiveActivity, error)
	List(ctx context.Context) ([]LiveActivity, error)

	Create(ctx context.Context, la *LiveActivity) error
	Update(ctx context.Context, la *LiveActivity) error

	RemoveStale(ctx context.Context) error
	Delete(ctx context.Context, apns_token string) error
}
