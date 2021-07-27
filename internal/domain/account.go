package domain

import "context"

// Account represents an account we need to periodically check in the notifications worker.
type Account struct {
	ID int64

	// Reddit information
	Username     string
	AccountID    string
	AccessToken  string
	RefreshToken string
	ExpiresAt    int64

	// Tracking how far behind we are
	LastMessageID string
	LastCheckedAt float64
}

// AccountRepository represents the account's repository contract
type AccountRepository interface {
	GetByID(ctx context.Context, id int64) (Account, error)
	GetByRedditID(ctx context.Context, id string) (Account, error)

	CreateOrUpdate(ctx context.Context, acc *Account) error
	Update(ctx context.Context, acc *Account) error
	Create(ctx context.Context, acc *Account) error
	Delete(ctx context.Context, id int64) error
	Associate(ctx context.Context, acc *Account, dev *Device) error
}

// AccountUsecase represents the account's usecases
type AccountUsecase interface {
	GetByID(ctx context.Context, id int64) (Account, error)
	GetByRedditID(ctx context.Context, id string) (Account, error)
	CreateOrUpdate(ctx context.Context, acc *Account) error
	Delete(ctx context.Context, id int64) error
}
