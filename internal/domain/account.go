package domain

import (
	"context"
	"strings"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const (
	NotificationCheckInterval      = 10 * time.Second // time between notification checks
	NotificationCheckTimeout       = 5 * time.Minute  // time before we give up an account check lock
	StuckNotificationCheckInterval = 2 * time.Minute  // time between stuck notification checks
	StaleTokenThreshold            = 2 * time.Hour    // time an oauth token has to be expired for to be stale
)

// Account represents an account we need to periodically check in the notifications worker.
type Account struct {
	ID int64

	// Reddit information
	Username       string
	AccountID      string
	AccessToken    string
	RefreshToken   string
	TokenExpiresAt time.Time
	Development    bool

	// Tracking how far behind we are
	LastMessageID                string
	NextNotificationCheckAt      time.Time
	NextStuckNotificationCheckAt time.Time
	CheckCount                   int64
}

func (acct *Account) NormalizedUsername() string {
	return strings.ToLower(acct.Username)
}

func (acct *Account) Validate() error {
	return validation.ValidateStruct(acct,
		validation.Field(&acct.Username, validation.Required, validation.Length(3, 32)),
		validation.Field(&acct.AccountID, validation.Required, validation.Length(4, 9)),
	)
}

// AccountRepository represents the account's repository contract
type AccountRepository interface {
	GetByID(ctx context.Context, id int64) (Account, error)
	GetByRedditID(ctx context.Context, id string) (Account, error)
	GetByAPNSToken(ctx context.Context, token string) ([]Account, error)

	CreateOrUpdate(ctx context.Context, acc *Account) error
	Update(ctx context.Context, acc *Account) error
	Create(ctx context.Context, acc *Account) error
	Delete(ctx context.Context, id int64) error
	Associate(ctx context.Context, acc *Account, dev *Device) error
	Disassociate(ctx context.Context, acc *Account, dev *Device) error

	PruneOrphaned(ctx context.Context) (int64, error)
	PruneStale(ctx context.Context, expiry time.Time) (int64, error)
}
