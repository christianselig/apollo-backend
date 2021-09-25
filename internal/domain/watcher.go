package domain

import "context"

type Watcher struct {
	ID int64

	DeviceID    int64
	AccountID   int64
	SubredditID int64

	Upvotes int64
	Keyword string
	Flair   string
	Domain  string
}

type WatcherRepository interface {
	GetByID(ctx context.Context, id int64) (Watcher, error)
	GetBySubredditID(ctx context.Context, id int64) ([]Watcher, error)

	Create(ctx context.Context, watcher *Watcher) error
	Update(ctx context.Context, watcher *Watcher) error
	Delete(ctx context.Context, id int64) error
}
