package distributedlock

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"math/rand"
	"time"
)

const lockTopicFormat = "pubsub:locks:%s"

type DistributedLock struct {
	client  *redis.Client
	timeout time.Duration
}

func New(client *redis.Client, timeout time.Duration) *DistributedLock {
	return &DistributedLock{
		client:  client,
		timeout: timeout,
	}
}

func (d *DistributedLock) setLock(ctx context.Context, key string, uid string) error {
	result, err := d.client.SetNX(ctx, key, uid, d.timeout).Result()
	if err != nil {
		return err
	}

	if !result {
		return ErrLockAlreadyAcquired
	}

	return nil
}

func (d *DistributedLock) AcquireLock(ctx context.Context, key string) (*Lock, error) {
	uid := generateUniqueID()
	if err := d.setLock(ctx, key, uid); err != nil {
		return nil, err
	}

	return NewLock(d, key, uid), nil
}

func (d *DistributedLock) WaitAcquireLock(ctx context.Context, key string, timeout time.Duration) (*Lock, error) {
	uid := generateUniqueID()
	if err := d.setLock(ctx, key, uid); err == nil {
		return NewLock(d, key, uid), nil
	}

	ch := fmt.Sprintf(lockTopicFormat, key)
	pubsub := d.client.Subscribe(ctx, ch)

	select {
	case <-time.After(timeout):
		return nil, ErrLockAcquisitionTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pubsub.Channel():
		err := d.setLock(ctx, key, uid)
		if err != nil {
			return nil, err
		}
		return NewLock(d, key, uid), nil
	}
}

func generateUniqueID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int63())
}
