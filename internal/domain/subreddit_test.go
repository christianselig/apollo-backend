package domain

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	tests := map[string]struct {
		subreddit Subreddit
		err       error
	}{
		"invalid subreddit prefix":        {Subreddit{Name: "u_iamthatis"}, errors.New("invalid subreddit format")},
		"valid subreddit":                 {Subreddit{Name: "pics", SubredditID: "abcd"}, nil},
		"valid subreddit starting with u": {Subreddit{Name: "urcool", SubredditID: "abcd"}, nil},
		"valid subreddit with _":          {Subreddit{Name: "p_i_x_a_r", SubredditID: "abcd"}, nil},
	}

	for scenario, tc := range tests {
		t.Run(scenario, func(t *testing.T) {
			err := tc.subreddit.Validate()

			if tc.err == nil {
				require.NoError(t, err)
				return
			}

			assert.Error(t, err)
		})
	}
}
