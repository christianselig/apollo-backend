package repository_test

import (
	"context"
	"testing"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/repository"
	"github.com/christianselig/apollo-backend/internal/testhelper"
	"github.com/stretchr/testify/require"
)

func NewTestPostgresWatcher(t *testing.T) domain.WatcherRepository {
	t.Helper()

	ctx := context.Background()
	conn := testhelper.NewTestPgxConn(t)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	repo := repository.NewPostgresWatcher(tx)

	t.Cleanup(func() {
		_ = tx.Rollback(ctx)
	})

	return repo
}

func TestPostgresWatcher_GetByID(t *testing.T) {
	t.Parallel()
}
