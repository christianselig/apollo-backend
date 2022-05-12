package domain

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const SubredditCheckInterval = 2 * time.Minute

type Subreddit struct {
	ID          int64
	NextCheckAt time.Time

	// Reddit information
	SubredditID string
	Name        string
}

func (sr *Subreddit) NormalizedName() string {
	return strings.ToLower(sr.Name)
}

func validPrefix(value interface{}) error {
	s, _ := value.(string)
	if len(s) < 2 {
		return nil
	}
	if s[1] != '_' || s[0] != 'u' {
		return nil
	}

	return errors.New("invalid subreddit format")
}

func (sr *Subreddit) Validate() error {
	return validation.ValidateStruct(sr,
		validation.Field(&sr.Name, validation.Required, validation.Length(2, 32), validation.By(validPrefix), validation.Match(regexp.MustCompile(`^[a-zA-Z0-9]\w{1,19}$`))),
		validation.Field(&sr.SubredditID, validation.Required, validation.Length(4, 9)),
	)
}

type SubredditRepository interface {
	GetByID(ctx context.Context, id int64) (Subreddit, error)
	GetByName(ctx context.Context, name string) (Subreddit, error)

	CreateOrUpdate(ctx context.Context, sr *Subreddit) error
}
