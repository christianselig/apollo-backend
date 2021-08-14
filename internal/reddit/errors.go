package reddit

import (
	"errors"
	"fmt"
)

type ServerError struct {
	StatusCode int
}

func (se ServerError) Error() string {
	return fmt.Sprintf("errror from reddit: %d", se.StatusCode)
}

var (
	// ErrOauthRevoked .
	ErrOauthRevoked = errors.New("oauth revoked")
	// ErrTimeout .
	ErrTimeout = errors.New("timeout")
)
