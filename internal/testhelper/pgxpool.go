package testhelper

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/require"
)

func NewTestPgxConn(t *testing.T) *pgx.Conn {
	t.Helper()

	ctx := context.Background()

	connString := os.Getenv("DATABASE_URL")

	if connString == "" {
		t.Skipf("skipping due to missing environment variable %v", "DATABASE_URL")
	}

	config, err := pgx.ParseConfig(connString)
	require.NoError(t, err)

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)

	t.Cleanup(func() {
		conn.Close(ctx)
	})

	return conn
}
