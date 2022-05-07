package reddit_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/christianselig/apollo-backend/internal/reddit"
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
	return &http.Client{Transport: fn}
}

func TestErrorResponse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	db, _ := redismock.NewClientMock()

	errortests := map[string]struct {
		call func(*reddit.AuthenticatedClient) error

		status int
		body   string
		err    error
	}{
		"/api/v1/me 500 returns ServerError":                 {func(rac *reddit.AuthenticatedClient) error { _, err := rac.Me(ctx); return err }, 500, "", reddit.ServerError{500}},
		"/api/v1/access_token 400 returns ErrOauthRevoked":   {func(rac *reddit.AuthenticatedClient) error { _, err := rac.RefreshTokens(ctx); return err }, 400, "", reddit.ErrOauthRevoked},
		"/api/v1/message/inbox 403 returns ErrOauthRevoked":  {func(rac *reddit.AuthenticatedClient) error { _, err := rac.MessageInbox(ctx); return err }, 403, "", reddit.ErrOauthRevoked},
		"/api/v1/message/unread 403 returns ErrOauthRevoked": {func(rac *reddit.AuthenticatedClient) error { _, err := rac.MessageUnread(ctx); return err }, 403, "", reddit.ErrOauthRevoked},
		"/api/v1/me 403 returns ErrOauthRevoked":             {func(rac *reddit.AuthenticatedClient) error { _, err := rac.Me(ctx); return err }, 403, "", reddit.ErrOauthRevoked},
	}

	for scenario, tt := range errortests {
		tt := tt

		t.Run(scenario, func(t *testing.T) {
			t.Parallel()

			tc := NewTestClient(func(req *http.Request) *http.Response {
				return &http.Response{
					StatusCode: tt.status,
					Body:       ioutil.NopCloser(bytes.NewBufferString(tt.body)),
					Header:     make(http.Header),
				}
			})

			rc := reddit.NewClient("", "", &statsd.NoOpClient{}, db, 1, reddit.WithRetry(false), reddit.WithClient(tc))
			rac := rc.NewAuthenticatedClient(reddit.SkipRateLimiting, "", "")

			err := tt.call(rac)

			assert.ErrorIs(t, err, tt.err)
		})
	}
}
