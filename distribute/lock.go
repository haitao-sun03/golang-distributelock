package distribute

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type DistributedLock struct {
	client *redis.Client
	Key    string
	Value  string
	expiry time.Duration
}

func NewDistributedLock(client *redis.Client, key, value string, expiry time.Duration) *DistributedLock {
	return &DistributedLock{
		client: client,
		key:    key,
		value:  value,
		expiry: expiry,
	}
}

// TryLock attempts to acquire the lock. Returns true if lock was acquired.
func (lock *DistributedLock) TryLock(ctx context.Context) (bool, error) {
	// 使用SET命令，它在Redis 2.6.12+中是原子操作
	result := lock.client.SetNX(ctx, lock.key, lock.value, lock.expiry)
	if err := result.Err(); err != nil {
		return false, err
	}
	return result.Val(), nil
}

// Unlock releases the lock if it is held by this client.
func (lock *DistributedLock) Unlock(ctx context.Context) error {
	// 使用Lua脚本确保原子性
	unlockScript := redis.NewScript(`
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("DEL", KEYS[1])
        else
            return 0
        end
    `)

	result, err := unlockScript.Run(ctx, lock.client, []string{lock.key}, lock.value).Result()
	if err != nil {
		return err
	}

	if result == int64(0) {
		return fmt.Errorf("lock not held by current client")
	}
	return nil
}

// 锁续期
func (lock *DistributedLock) Renew(ctx context.Context) error {
	script := redis.NewScript(`
        if redis.call("GET", KEYS[1]) == ARGV[1] then
            return redis.call("EXPIRE", KEYS[1], ARGV[2])
        else
            return 0
        end
    `)
	result, err := script.Run(ctx, lock.client, []string{lock.key}, lock.value, int(lock.expiry.Seconds())).Result()
	if err != nil {
		return err
	}
	if result == int64(0) {
		return fmt.Errorf("lock not held by current client")
	}
	return nil
}
