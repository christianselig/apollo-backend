package repository_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/christianselig/apollo-backend/internal/domain"
	"github.com/christianselig/apollo-backend/internal/repository"
	"github.com/christianselig/apollo-backend/internal/testhelper"
)

const testToken = "313a182b63224821f5595f42aa019de850a0e7b776253659a9aac8140bb8a3f2"

func NewTestPostgresDevice(t *testing.T) domain.DeviceRepository {
	t.Helper()

	ctx := context.Background()
	conn := testhelper.NewTestPgxConn(t)

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	repo := repository.NewPostgresDevice(tx)

	t.Cleanup(func() {
		_ = tx.Rollback(ctx)
	})

	return repo
}

func TestPostgresDevice_GetByID(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := NewTestPostgresDevice(t)

	dev := &domain.Device{APNSToken: testToken}
	require.NoError(t, repo.CreateOrUpdate(ctx, dev))

	testCases := map[string]struct {
		id   int64
		want *domain.Device
		err  error
	}{
		"valid ID":   {dev.ID, dev, nil},
		"invalid ID": {0, nil, domain.ErrNotFound},
	}

	for scenario, tc := range testCases { //nolint:paralleltest
		t.Run(scenario, func(t *testing.T) {
			dev, err := repo.GetByID(ctx, tc.id)
			if tc.err != nil {
				require.Error(t, err)
				assert.Equal(t, tc.err, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.want, &dev)
		})
	}
}

func TestPostgresDevice_Create(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := NewTestPostgresDevice(t)

	testCases := map[string]struct {
		have *domain.Device
		err  bool
	}{
		"valid":              {&domain.Device{APNSToken: testToken}, false},
		"invalid APNS token": {&domain.Device{APNSToken: "not valid"}, true},
	}

	for scenario, tc := range testCases { //nolint:paralleltest
		t.Run(scenario, func(t *testing.T) {
			err := repo.Create(ctx, tc.have)

			if tc.err {
				assert.Error(t, err)
				return
			}

			assert.NotEqual(t, 0, tc.have.ID)
		})
	}
}

func TestPostgresDevice_Update(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	repo := NewTestPostgresDevice(t)

	testCases := map[string]struct {
		fn  func(*domain.Device)
		err error
	}{
		"valid update":              {func(dev *domain.Device) { dev.Sandbox = true }, nil},
		"empty update":              {func(dev *domain.Device) {}, nil},
		"update on non existant id": {func(dev *domain.Device) { dev.ID = 0 }, errors.New("weird behaviour, total rows affected: 0")},
	}

	for scenario, tc := range testCases { //nolint:paralleltest
		t.Run(scenario, func(t *testing.T) {
			b := make([]byte, 32)
			_, err := rand.Read(b)
			require.NoError(t, err)

			dev := &domain.Device{APNSToken: hex.EncodeToString(b)}
			require.NoError(t, repo.Create(ctx, dev))

			tc.fn(dev)

			err = repo.Update(ctx, dev)
			if tc.err != nil {
				require.Error(t, err)
				assert.Equal(t, tc.err, err)
				return
			}

			require.NoError(t, err)
		})
	}
}
