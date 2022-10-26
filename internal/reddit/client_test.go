package reddit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/christianselig/apollo-backend/internal/reddit"
)

func TestAuthenticatedClientObfuscatedToken(t *testing.T) {
	t.Parallel()

	rc := reddit.NewClient("<SECRET>", "<SECRET>", nil, nil, 1)

	type test struct {
		have string
		want string
	}

	tests := []test{
		{"abc", "<SHORT>"},
		{"abcdefghi", "abc...ghi"},
	}

	for _, tc := range tests {
		tc := tc
		rac := rc.NewAuthenticatedClient("<ID>", "<REFRESH>", tc.have)
		got := rac.ObfuscatedAccessToken()

		assert.Equal(t, tc.want, got)
	}
}
