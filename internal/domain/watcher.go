package domain

import (
	"context"
	"strings"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

type WatcherType int64

const (
	SubredditWatcher WatcherType = iota
	UserWatcher
	TrendingWatcher
)

func (wt WatcherType) String() string {
	switch wt {
	case SubredditWatcher:
		return "subreddit"
	case UserWatcher:
		return "user"
	case TrendingWatcher:
		return "trending"
	}

	return "unknown"
}

type Watcher struct {
	ID             int64
	CreatedAt      time.Time
	LastNotifiedAt time.Time
	Label          string

	DeviceID     int64
	AccountID    int64
	Type         WatcherType
	WatcheeID    int64
	WatcheeLabel string

	Author    string
	Subreddit string
	Upvotes   int64
	Keyword   string
	Flair     string
	Domain    string
	Hits      int64

	// Related models
	Device  Device
	Account Account
}

func (w *Watcher) KeywordMatches(haystack string) bool {
	if w.Keyword == "" {
		return true
	}

	keywords := strings.FieldsFunc(w.Keyword, func(r rune) bool {
		return r == '+' || r == ','
	})

	haystack = strings.ToLower(haystack)

	for _, keyword := range keywords {
		if !strings.Contains(haystack, keyword) {
			return false
		}
	}

	return true
}

func (w *Watcher) Validate() error {
	return validation.ValidateStruct(w,
		validation.Field(&w.Label, validation.Required, validation.Length(1, 64)),
		validation.Field(&w.Type, validation.In(SubredditWatcher, UserWatcher, TrendingWatcher)),
		validation.Field(&w.WatcheeID, validation.Required),
	)
}

type WatcherRepository interface {
	GetByID(ctx context.Context, id int64) (Watcher, error)
	GetBySubredditID(ctx context.Context, id int64) ([]Watcher, error)
	GetByUserID(ctx context.Context, id int64) ([]Watcher, error)
	GetByTrendingSubredditID(ctx context.Context, id int64) ([]Watcher, error)
	GetByDeviceAPNSTokenAndAccountRedditID(ctx context.Context, apns string, rid string) ([]Watcher, error)

	Create(ctx context.Context, watcher *Watcher) error
	Update(ctx context.Context, watcher *Watcher) error
	IncrementHits(ctx context.Context, id int64) error
	Delete(ctx context.Context, id int64) error
	DeleteByTypeAndWatcheeID(context.Context, WatcherType, int64) error
}
