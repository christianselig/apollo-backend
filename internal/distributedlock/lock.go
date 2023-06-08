package distributedlock

import (
	"context"
	"fmt"
)

type Lock struct {
	distributedLock *DistributedLock
	key             string
	uid             string
}

func NewLock(distributedLock *DistributedLock, key string, uid string) *Lock {
	return &Lock{
		distributedLock: distributedLock,
		key:             key,
		uid:             uid,
	}
}

func (l *Lock) Release(ctx context.Context) error {

	ch := fmt.Sprintf(lockTopicFormat, l.key)

	result, err := l.distributedLock.client.EvalSha(ctx, l.distributedLock.sha, []string{l.key, ch}, l.uid).Result()
	if err != nil {
		return err
	}

	if result == int64(0) {
		return ErrLockExpired
	}

	return nil
}
