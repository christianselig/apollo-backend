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
	// ErrSubredditIsPrivate .
	ErrSubredditIsPrivate = errors.New("subreddit is private")
	// ErrSubredditIsQuarantined .
	ErrSubredditIsQuarantined = errors.New("subreddit is quarantined")
	// ErrSubredditNotFound .
	ErrSubredditNotFound = errors.New("subreddit not found")
	// ErrTooManyRequests .
	ErrTooManyRequests = errors.New("too many requests")
)
