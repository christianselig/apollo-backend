package domain

import (
	"context"
	"strings"

	validation "github.com/go-ozzo/ozzo-validation/v4"
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

func (sr *Subreddit) Validate() error {
	return validation.ValidateStruct(sr,
		validation.Field(&sr.Name, validation.Required, validation.Length(3, 32)),
		validation.Field(&sr.SubredditID, validation.Required, validation.Length(4, 9)),
	)
}

type SubredditRepository interface {
	GetByID(ctx context.Context, id int64) (Subreddit, error)
	GetByName(ctx context.Context, name string) (Subreddit, error)

	CreateOrUpdate(ctx context.Context, sr *Subreddit) error
}
