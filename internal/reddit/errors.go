package reddit

import (
	"errors"
	"fmt"
)

type ServerError struct {
	Body       string
	StatusCode int
}

func (se ServerError) Error() string {
	return fmt.Sprintf("error from reddit: %d (%s)", se.StatusCode, se.Body)
}

var (
	// ErrOauthRevoked .
	ErrOauthRevoked = errors.New("oauth revoked")
	// ErrTimeout .
	ErrTimeout = errors.New("timeout")
	// ErrRateLimited .
	ErrRateLimited = errors.New("rate limited")
	// ErrRequiresRedditId .
	ErrRequiresRedditId = errors.New("requires reddit id")
	// ErrInvalidBasicAuth .
	ErrInvalidBasicAuth = errors.New("invalid basic auth")
)
