package common

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// customDomainGenerationKey is bumped whenever any custom domain binding
// changes (see InvalidateCustomDomainCache), so every gateway knows when its
// copy of the active domain names is out of date.
const customDomainGenerationKey = "middleware:custom_domain:generation"

const (
	// How often a gateway compares its copy with the shared generation.
	customDomainSetCheckInterval = time.Second
	// Reload at least this often even if no change was announced.
	customDomainSetMaxAge = time.Minute
	// Without Redis no change is announced, so reload more often.
	customDomainSetMaxAgeWithoutRedis = 10 * time.Second
	customDomainSetLoadTimeout        = 5 * time.Second
)

var errNoCustomDomainGeneration = errors.New("custom domain generation unavailable")

// customDomainLocalEpoch is bumped by InvalidateCustomDomainCache, so the
// gateway that made a change reloads its copy on next use instead of waiting
// for the shared generation check.
var customDomainLocalEpoch atomic.Uint64

// ActiveCustomDomainLister lists the names of all active (verified) custom
// domains.
type ActiveCustomDomainLister interface {
	ListActiveCustomDomainNames(ctx context.Context) ([]string, error)
}

// ActiveCustomDomainSet is one gateway's copy of the names of all active custom
// domains. Host routing consults it before looking a host up in the database,
// so requests with arbitrary Host headers cannot cause a database query each:
// only hosts in the set are looked up. The copy is reloaded when the shared
// generation changes (checked at most once a second), when this gateway itself
// changed a binding, and at least once a minute.
type ActiveCustomDomainSet struct {
	lister      ActiveCustomDomainLister
	redisClient *RedisClient
	now         func() time.Time

	mu         sync.RWMutex
	domains    map[string]struct{}
	usable     bool
	generation string
	epoch      uint64
	loadedAt   time.Time
	checkedAt  time.Time

	refreshing atomic.Bool
}

// NewActiveCustomDomainSet returns a set backed by lister. With a nil lister
// the set cannot tell, and MayBeActive always answers true.
func NewActiveCustomDomainSet(lister ActiveCustomDomainLister, redisClient *RedisClient) *ActiveCustomDomainSet {
	return &ActiveCustomDomainSet{lister: lister, redisClient: redisClient, now: time.Now}
}

// MayBeActive reports whether domain may have an active claim. False is safe
// to act on without a database lookup; true means the caller must look the
// domain up. Whenever the set cannot tell (not loaded yet, or its last load
// failed) it answers true. Right after a domain is verified on another
// gateway, this one may answer false for up to about a second.
func (s *ActiveCustomDomainSet) MayBeActive(ctx context.Context, domain string) bool {
	if s == nil || s.lister == nil {
		return true
	}
	s.refresh(ctx)

	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.usable {
		return true
	}
	_, ok := s.domains[NormalizeDomain(domain)]
	return ok
}

func (s *ActiveCustomDomainSet) refresh(ctx context.Context) {
	now := s.now()
	epoch := customDomainLocalEpoch.Load()

	s.mu.RLock()
	due := s.checkedAt.IsZero() || epoch != s.epoch || now.Sub(s.checkedAt) >= customDomainSetCheckInterval
	s.mu.RUnlock()
	// One request refreshes; the others keep using the current copy.
	if !due || !s.refreshing.CompareAndSwap(false, true) {
		return
	}
	defer s.refreshing.Store(false)

	// The load must not fail because the request that triggered it ended.
	loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), customDomainSetLoadTimeout)
	defer cancel()

	// Read the generation before listing: a change that lands in between bumps
	// the generation again, so the next check reloads and nothing is missed.
	generation, genErr := s.currentGeneration(loadCtx)

	s.mu.RLock()
	maxAge := customDomainSetMaxAge
	if genErr != nil {
		maxAge = customDomainSetMaxAgeWithoutRedis
	}
	stale := !s.usable || epoch != s.epoch || now.Sub(s.loadedAt) >= maxAge ||
		(genErr == nil && generation != s.generation)
	s.mu.RUnlock()

	if !stale {
		s.mu.Lock()
		s.checkedAt = now
		s.mu.Unlock()
		return
	}

	names, err := s.lister.ListActiveCustomDomainNames(loadCtx)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkedAt = now
	s.epoch = epoch
	if err != nil {
		// Fall back to looking every host up until a load succeeds.
		s.usable = false
		return
	}
	domains := make(map[string]struct{}, len(names))
	for _, name := range names {
		if normalized := NormalizeDomain(name); normalized != "" {
			domains[normalized] = struct{}{}
		}
	}
	s.domains = domains
	s.usable = true
	s.generation = generation
	s.loadedAt = now
}

func (s *ActiveCustomDomainSet) currentGeneration(ctx context.Context) (string, error) {
	if s.redisClient == nil {
		return "", errNoCustomDomainGeneration
	}
	generation, err := s.redisClient.Get(ctx, customDomainGenerationKey).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return generation, err
}
