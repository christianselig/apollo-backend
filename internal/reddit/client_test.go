package reddit

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/go-redis/redismock/v8"
	"github.com/stretchr/testify/assert"
)

// RoundTripFunc .
type RoundTripFunc func(req *http.Request) *http.Response

// RoundTrip .
func (f RoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req), nil
}

//NewTestClient returns *http.Client with Transport replaced to avoid making real calls
func NewTestClient(fn RoundTripFunc) *http.Client {
	return &http.Client{
		Transport: RoundTripFunc(fn),
	}
}

func TestErrorResponse(t *testing.T) {
	db, _ := redismock.NewClientMock()

	rc := NewClient("", "", &statsd.NoOpClient{}, db, 1)
	rac := rc.NewAuthenticatedClient(SkipRateLimiting, "", "")

	errortests := []struct {
		name string
		call func() error

		status int
		body   string
		err    error
	}{
		{"/api/v1/me 500 returns ServerError", func() error { _, err := rac.Me(WithRetry(false)); return err }, 500, "", ServerError{500}},
		{"/api/v1/access_token 400 returns ErrOauthRevoked", func() error { _, err := rac.RefreshTokens(WithRetry(false)); return err }, 400, "", ErrOauthRevoked},
		{"/api/v1/message/inbox 403 returns ErrOauthRevoked", func() error { _, err := rac.MessageInbox(WithRetry(false)); return err }, 403, "", ErrOauthRevoked},
		{"/api/v1/message/unread 403 returns ErrOauthRevoked", func() error { _, err := rac.MessageUnread(WithRetry(false)); return err }, 403, "", ErrOauthRevoked},
		{"/api/v1/me 403 returns ErrOauthRevoked", func() error { _, err := rac.Me(WithRetry(false)); return err }, 403, "", ErrOauthRevoked},
	}

	for _, tt := range errortests {
		t.Run(tt.name, func(t *testing.T) {
			rac.client = NewTestClient(func(req *http.Request) *http.Response {
				return &http.Response{
					StatusCode: tt.status,
					Body:       ioutil.NopCloser(bytes.NewBufferString(tt.body)),
					Header:     make(http.Header),
				}
			})

			err := tt.call()

			assert.ErrorIs(t, err, tt.err)
		})
	}
}
