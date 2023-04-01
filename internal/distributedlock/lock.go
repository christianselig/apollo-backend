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
	script := `
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			redis.call("DEL", KEYS[1])
			redis.call("PUBLISH", KEYS[2], KEYS[1])
			return 1
		else
			return 0
		end
	`

	ch := fmt.Sprintf(lockTopicFormat, l.key)

	result, err := l.distributedLock.client.Eval(ctx, script, []string{l.key, ch}, l.uid).Result()
	if err != nil {
		return err
	}

	if result == int64(0) {
		return ErrLockExpired
	}

	return nil
}
