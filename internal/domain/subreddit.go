package domain

import (
	"context"
	"strings"
)

type Subreddit struct {
	ID            int64
	LastCheckedAt float64

	// Reddit information
	SubredditID string
	Name        string
}

func (sr *Subreddit) NormalizedName() string {
	return strings.ToLower(sr.Name)
}

type SubredditRepository interface {
	GetByID(ctx context.Context, id int64) (Subreddit, error)
	GetByName(ctx context.Context, name string) (Subreddit, error)

	CreateOrUpdate(ctx context.Context, sr *Subreddit) error
}
