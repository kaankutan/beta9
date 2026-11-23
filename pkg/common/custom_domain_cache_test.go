package common

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestCustomDomainCacheWriteIsRejectedAfterBindingChange(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	domain := "race.customer.io"
	key := CustomDomainHandlerCacheKey(domain)

	// No change in between: the write lands.
	rev := CustomDomainRevision(ctx, rdb, domain)
	require.True(t, SetCustomDomainCacheIfCurrent(ctx, rdb, domain, rev, key, "ws|/endpoint/a/latest", time.Minute))
	require.Equal(t, "ws|/endpoint/a/latest", rdb.Get(ctx, key).Val())

	// A slow request read the binding, then the domain was removed or
	// transferred (which invalidates the cache). Its late write must not bring
	// the old binding back.
	rev = CustomDomainRevision(ctx, rdb, domain)
	InvalidateCustomDomainCache(ctx, rdb, domain)
	require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())
	require.False(t, SetCustomDomainCacheIfCurrent(ctx, rdb, domain, rev, key, "oldws|/endpoint/old/latest", time.Minute))
	require.Equal(t, int64(0), rdb.Exists(ctx, key).Val())

	// A request that starts after the change sees the new revision and caches normally.
	rev = CustomDomainRevision(ctx, rdb, domain)
	require.True(t, SetCustomDomainCacheIfCurrent(ctx, rdb, domain, rev, key, "ws|/endpoint/new/latest", time.Minute))
	require.Equal(t, "ws|/endpoint/new/latest", rdb.Get(ctx, key).Val())
}

func TestCustomDomainCacheWriteWithoutRevisionHistory(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()
	// Domain never invalidated: revision is empty on both sides, so a miss can be cached.
	require.True(t, SetCustomDomainCacheIfCurrent(ctx, rdb, "fresh.customer.io", "", CustomDomainHandlerCacheKey("fresh.customer.io"), "-", time.Minute))
	// Without Redis nothing is written.
	require.False(t, SetCustomDomainCacheIfCurrent(ctx, nil, "fresh.customer.io", "", "k", "v", time.Minute))
}

// slotRecorder records the keys of every DEL sent through a pipeline.
type slotRecorder struct {
	dels [][]string
}

func (r *slotRecorder) DialHook(next redis.DialHook) redis.DialHook { return next }

func (r *slotRecorder) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }

func (r *slotRecorder) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			if cmd.Name() != "del" {
				continue
			}
			var keys []string
			for _, arg := range cmd.Args()[1:] {
				keys = append(keys, fmt.Sprint(arg))
			}
			r.dels = append(r.dels, keys)
		}
		return next(ctx, cmds)
	}
}

func hashTag(key string) string {
	start := strings.Index(key, "{")
	end := strings.Index(key[start+1:], "}")
	return key[start+1 : start+1+end]
}

// Redis Cluster rejects a multi-key command whose keys hash to different
// slots, so invalidating several domains must never mix their keys in one DEL.
func TestInvalidateCustomDomainCacheKeepsEachDeleteInOneSlot(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	rdb, err := NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rdb.Close() })
	recorder := &slotRecorder{}
	rdb.AddHook(recorder)

	ctx := context.Background()
	domains := []string{"a.customer.io", "b.customer.io"}
	for _, d := range domains {
		require.NoError(t, rdb.Set(ctx, CustomDomainHandlerCacheKey(d), "ws|/endpoint/x/latest", time.Minute).Err())
		require.NoError(t, rdb.Set(ctx, CustomDomainCheckCacheKey(d), "1", time.Minute).Err())
	}

	InvalidateCustomDomainCache(ctx, rdb, domains...)

	require.Len(t, recorder.dels, len(domains))
	for _, keys := range recorder.dels {
		for _, key := range keys {
			require.Equal(t, hashTag(keys[0]), hashTag(key), "DEL mixes hash slots: %v", keys)
		}
	}
	for _, d := range domains {
		require.Equal(t, int64(0), rdb.Exists(ctx, CustomDomainHandlerCacheKey(d), CustomDomainCheckCacheKey(d)).Val())
	}
}
