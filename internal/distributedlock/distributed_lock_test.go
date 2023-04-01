package distributedlock_test

import (
	"context"
	"fmt"
	"github.com/christianselig/apollo-backend/internal/distributedlock"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func NewRedisClient(t *testing.T, ctx context.Context) (*redis.Client, func()) {
	t.Helper()

	opt, err := redis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		panic(err)
	}
	client := redis.NewClient(opt)
	if err := client.Ping(ctx).Err(); err != nil {
		panic(err)
	}

	return client, func() {
		_ = client.Close()
	}
}

func TestDistributedLock_AcquireLock(t *testing.T) {
	ctx := context.Background()
	key := fmt.Sprintf("%d", time.Now().UnixNano())

	client, closer := NewRedisClient(t, ctx)
	defer closer()

	d := distributedlock.New(client, 10*time.Second)
	lock, err := d.AcquireLock(ctx, key)
	assert.NoError(t, err)

	_, err = d.AcquireLock(ctx, key)
	assert.Equal(t, distributedlock.ErrLockAlreadyAcquired, err)

	err = lock.Release(ctx)
	assert.NoError(t, err)

	_, err = d.AcquireLock(ctx, key)
	assert.NoError(t, err)
}

func TestDistributedLock_WaitAcquireLock(t *testing.T) {
	ctx := context.Background()
	key := fmt.Sprintf("%d", time.Now().UnixNano())

	client, closer := NewRedisClient(t, ctx)
	defer closer()

	d := distributedlock.New(client, 10*time.Second)
	lock, err := d.AcquireLock(ctx, key)
	assert.NoError(t, err)

	go func(l *distributedlock.Lock) {
		select {
		case <-time.After(100 * time.Millisecond):
			_ = l.Release(ctx)
		}
	}(lock)

	lock, err = d.WaitAcquireLock(ctx, key, 5*time.Second)
	assert.NoError(t, err)
	assert.NotNil(t, lock)
}
