package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/christianselig/apollo-backend/internal/domain"
)

func TestWatcherKeywordMatches(t *testing.T) {
	t.Parallel()

	tt := map[string]struct {
		title   string
		keyword string

		want bool
	}{
		"match exact":               {"exact title", "exact title", true},
		"empty keyword matches all": {"exact title", "", true},
		"keywords with commas":      {"exact title", "exact,title", true},
		"keywords with plus":        {"exact title", "exact+title", true},
		"missing words":             {"exact title", "not title", false},
	}

	for scenario, tc := range tt {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()

			w := &domain.Watcher{Keyword: tc.keyword}

			assert.Equal(t, tc.want, w.KeywordMatches(tc.title))
		})
	}
}
