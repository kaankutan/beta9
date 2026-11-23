package common

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

type fakeActiveDomainLister struct {
	names []string
	err   error
	loads int
}

func (l *fakeActiveDomainLister) ListActiveCustomDomainNames(context.Context) ([]string, error) {
	l.loads++
	return l.names, l.err
}

type fakeDeploymentDomainLister struct {
	domains []types.CustomDomain
}

func (l *fakeDeploymentDomainLister) ListCustomDomains(context.Context, uint, string) ([]types.CustomDomain, error) {
	return l.domains, nil
}

func newTestActiveDomainSet(t *testing.T, lister ActiveCustomDomainLister, withRedis bool) (*ActiveCustomDomainSet, *RedisClient, *time.Time) {
	t.Helper()
	var rdb *RedisClient
	if withRedis {
		mr, err := miniredis.Run()
		require.NoError(t, err)
		t.Cleanup(mr.Close)
		rdb, err = NewRedisClient(types.RedisConfig{Addrs: []string{mr.Addr()}, Mode: types.RedisModeSingle})
		require.NoError(t, err)
		t.Cleanup(func() { _ = rdb.Close() })
	}
	clock := time.Unix(1_700_000_000, 0)
	set := NewActiveCustomDomainSet(lister, rdb)
	set.now = func() time.Time { return clock }
	return set, rdb, &clock
}

func TestActiveCustomDomainSetAnswersFromOneLoad(t *testing.T) {
	lister := &fakeActiveDomainLister{names: []string{"Live.Customer.io."}}
	set, _, _ := newTestActiveDomainSet(t, lister, true)
	ctx := context.Background()

	require.True(t, set.MayBeActive(ctx, "live.customer.io:443"))
	require.False(t, set.MayBeActive(ctx, "random.attacker.example"))
	require.False(t, set.MayBeActive(ctx, "other.attacker.example"))
	require.Equal(t, 1, lister.loads)
}

func TestActiveCustomDomainSetReloadsWhenAnotherGatewayChangesABinding(t *testing.T) {
	lister := &fakeActiveDomainLister{}
	set, rdb, clock := newTestActiveDomainSet(t, lister, true)
	ctx := context.Background()

	require.False(t, set.MayBeActive(ctx, "new.customer.io"))

	// Another gateway verifies the domain: it bumps the shared generation but
	// not this process's epoch.
	lister.names = []string{"new.customer.io"}
	require.NoError(t, rdb.Incr(ctx, customDomainGenerationKey).Err())

	// Until the next check the copy is not compared with the generation.
	require.False(t, set.MayBeActive(ctx, "new.customer.io"))
	*clock = clock.Add(customDomainSetCheckInterval)
	require.True(t, set.MayBeActive(ctx, "new.customer.io"))
	require.Equal(t, 2, lister.loads)

	// No further change: later checks do not reload.
	*clock = clock.Add(customDomainSetCheckInterval)
	require.True(t, set.MayBeActive(ctx, "new.customer.io"))
	require.Equal(t, 2, lister.loads)
}

func TestActiveCustomDomainSetReloadsAtOnceAfterALocalChange(t *testing.T) {
	lister := &fakeActiveDomainLister{}
	set, rdb, _ := newTestActiveDomainSet(t, lister, true)
	ctx := context.Background()

	require.False(t, set.MayBeActive(ctx, "verified.customer.io"))
	lister.names = []string{"verified.customer.io"}
	InvalidateCustomDomainCache(ctx, rdb, "verified.customer.io")
	require.True(t, set.MayBeActive(ctx, "verified.customer.io"))
	require.Equal(t, 2, lister.loads)
}

// Stopping, starting or redeploying a deployment changes which deployment a
// domain routes to, not which domains are active, so no gateway reloads.
func TestActiveCustomDomainSetIgnoresDeploymentChanges(t *testing.T) {
	lister := &fakeActiveDomainLister{names: []string{"a.customer.io"}}
	set, rdb, clock := newTestActiveDomainSet(t, lister, true)
	ctx := context.Background()

	require.True(t, set.MayBeActive(ctx, "a.customer.io"))
	InvalidateDeploymentCustomDomains(ctx, &fakeDeploymentDomainLister{domains: []types.CustomDomain{{Domain: "a.customer.io"}}}, rdb, 1, "my-api")
	*clock = clock.Add(customDomainSetCheckInterval)
	require.True(t, set.MayBeActive(ctx, "a.customer.io"))
	require.Equal(t, 1, lister.loads)
}

func TestActiveCustomDomainSetFallsBackToLookupsWhenItCannotLoad(t *testing.T) {
	lister := &fakeActiveDomainLister{err: errors.New("database unavailable")}
	set, _, clock := newTestActiveDomainSet(t, lister, true)
	ctx := context.Background()

	require.True(t, set.MayBeActive(ctx, "anything.example"), "a set that never loaded cannot rule anything out")
	require.True(t, set.MayBeActive(ctx, "else.example"))
	require.Equal(t, 1, lister.loads, "failed loads are retried on the next check, not on every request")

	lister.err = nil
	lister.names = []string{"live.customer.io"}
	*clock = clock.Add(customDomainSetCheckInterval)
	require.False(t, set.MayBeActive(ctx, "anything.example"))
	require.True(t, set.MayBeActive(ctx, "live.customer.io"))
}

func TestActiveCustomDomainSetWithoutRedisReloadsByAge(t *testing.T) {
	lister := &fakeActiveDomainLister{}
	set, _, clock := newTestActiveDomainSet(t, lister, false)
	ctx := context.Background()

	require.False(t, set.MayBeActive(ctx, "new.customer.io"))
	lister.names = []string{"new.customer.io"}

	// Checked just before the copy is too old: kept.
	*clock = clock.Add(customDomainSetMaxAgeWithoutRedis - time.Millisecond)
	require.False(t, set.MayBeActive(ctx, "new.customer.io"))
	// The next check, a check interval later, finds it too old and reloads.
	*clock = clock.Add(customDomainSetCheckInterval)
	require.True(t, set.MayBeActive(ctx, "new.customer.io"))
	require.Equal(t, 2, lister.loads)
}

func TestActiveCustomDomainSetWithoutListerCannotTell(t *testing.T) {
	require.True(t, NewActiveCustomDomainSet(nil, nil).MayBeActive(context.Background(), "x.example"))
	var set *ActiveCustomDomainSet
	require.True(t, set.MayBeActive(context.Background(), "x.example"))
}

type syncActiveDomainLister struct {
	mu    sync.Mutex
	names []string
}

func (l *syncActiveDomainLister) ListActiveCustomDomainNames(context.Context) ([]string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.names...), nil
}

func TestActiveCustomDomainSetConcurrentUse(t *testing.T) {
	lister := &syncActiveDomainLister{names: []string{"a.customer.io"}}
	set, rdb, _ := newTestActiveDomainSet(t, lister, true)
	set.now = time.Now
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				set.MayBeActive(ctx, "a.customer.io")
				set.MayBeActive(ctx, "b.customer.io")
			}
		}()
	}
	for i := 0; i < 20; i++ {
		lister.mu.Lock()
		lister.names = append(lister.names, fmt.Sprintf("n%d.customer.io", i))
		lister.mu.Unlock()
		InvalidateCustomDomainCache(ctx, rdb, fmt.Sprintf("n%d.customer.io", i))
	}
	wg.Wait()

	require.True(t, set.MayBeActive(ctx, "n19.customer.io"))
}
