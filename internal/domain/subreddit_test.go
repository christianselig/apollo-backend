package domain_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/christianselig/apollo-backend/internal/domain"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	tt := map[string]struct {
		subreddit domain.Subreddit
		err       error
	}{
		"invalid subreddit prefix":        {domain.Subreddit{Name: "u_iamthatis"}, errors.New("invalid subreddit format")},
		"valid subreddit":                 {domain.Subreddit{Name: "pics", SubredditID: "abcd"}, nil},
		"valid subreddit starting with u": {domain.Subreddit{Name: "urcool", SubredditID: "abcd"}, nil},
		"valid subreddit with _":          {domain.Subreddit{Name: "p_i_x_a_r", SubredditID: "abcd"}, nil},
		"valid subreddit with 2 letters":  {domain.Subreddit{Name: "de", SubredditID: "abcd"}, nil},
	}

	for scenario, tc := range tt {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()

			err := tc.subreddit.Validate()

			if tc.err == nil {
				require.NoError(t, err)
				return
			}

			assert.Error(t, err)
		})
	}
}
