package cache

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Cache interface defines the cache operations
type Cache interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{})
	SetWithTTL(key string, value interface{}, ttl time.Duration)
	Delete(key string) bool
	Has(key string) bool
	Clear()
}

// MultiTierCache implements a multi-tier caching strategy
type MultiTierCache struct {
	tiers      []Cache
	mu         sync.RWMutex
	metrics    *MultiTierMetrics
	writeThrough bool // Write to all tiers on set
	promote      bool // Promote items to higher tiers on access
}

// MultiTierMetrics tracks multi-tier cache statistics
type MultiTierMetrics struct {
	mu       sync.RWMutex
	TierHits map[int]int64
	Misses   int64
	Sets     int64
	Deletes  int64
	Promotes int64
}

// MultiTierConfig configures the multi-tier cache
type MultiTierConfig struct {
	WriteThrough bool // Write to all tiers on set
	Promote      bool // Promote items to higher tiers on access
}

// DefaultMultiTierConfig returns a config with sensible defaults
func DefaultMultiTierConfig() MultiTierConfig {
	return MultiTierConfig{
		WriteThrough: true,
		Promote:      true,
	}
}

// NewMultiTierCache creates a new multi-tier cache
func NewMultiTierCache(config MultiTierConfig, tiers ...Cache) *MultiTierCache {
	metrics := &MultiTierMetrics{
		TierHits: make(map[int]int64),
	}

	return &MultiTierCache{
		tiers:        tiers,
		metrics:      metrics,
		writeThrough: config.WriteThrough,
		promote:      config.Promote,
	}
}

// Get retrieves a value from the cache, checking tiers in order
func (mtc *MultiTierCache) Get(key string) (interface{}, bool) {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()

	for i, tier := range mtc.tiers {
		if value, found := tier.Get(key); found {
			mtc.metrics.mu.Lock()
			mtc.metrics.TierHits[i]++
			mtc.metrics.mu.Unlock()

			// Promote to higher tiers
			if mtc.promote && i > 0 {
				go mtc.promoteToHigherTiers(key, value, i)
			}

			return value, true
		}
	}

	mtc.metrics.mu.Lock()
	mtc.metrics.Misses++
	mtc.metrics.mu.Unlock()

	return nil, false
}

// Set stores a value in the cache
func (mtc *MultiTierCache) Set(key string, value interface{}) {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()

	mtc.metrics.mu.Lock()
	mtc.metrics.Sets++
	mtc.metrics.mu.Unlock()

	if mtc.writeThrough {
		// Write to all tiers
		for _, tier := range mtc.tiers {
			tier.Set(key, value)
		}
	} else {
		// Write only to first tier
		if len(mtc.tiers) > 0 {
			mtc.tiers[0].Set(key, value)
		}
	}
}

// SetWithTTL stores a value with a specific TTL
func (mtc *MultiTierCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()

	mtc.metrics.mu.Lock()
	mtc.metrics.Sets++
	mtc.metrics.mu.Unlock()

	if mtc.writeThrough {
		for _, tier := range mtc.tiers {
			tier.SetWithTTL(key, value, ttl)
		}
	} else {
		if len(mtc.tiers) > 0 {
			mtc.tiers[0].SetWithTTL(key, value, ttl)
		}
	}
}

// Delete removes a value from all tiers
func (mtc *MultiTierCache) Delete(key string) bool {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()

	mtc.metrics.mu.Lock()
	mtc.metrics.Deletes++
	mtc.metrics.mu.Unlock()

	deleted := false
	for _, tier := range mtc.tiers {
		if tier.Delete(key) {
			deleted = true
		}
	}
	return deleted
}

// Has checks if a key exists in any tier
func (mtc *MultiTierCache) Has(key string) bool {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()

	for _, tier := range mtc.tiers {
		if tier.Has(key) {
			return true
		}
	}
	return false
}

// Clear removes all items from all tiers
func (mtc *MultiTierCache) Clear() {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()

	for _, tier := range mtc.tiers {
		tier.Clear()
	}
}

// promoteToHigherTiers copies a value to higher-level tiers
func (mtc *MultiTierCache) promoteToHigherTiers(key string, value interface{}, fromTier int) {
	mtc.mu.RLock()
	defer mtc.mu.RUnlock()

	mtc.metrics.mu.Lock()
	mtc.metrics.Promotes++
	mtc.metrics.mu.Unlock()

	for i := 0; i < fromTier; i++ {
		mtc.tiers[i].Set(key, value)
	}
}

// GetMetrics returns a copy of the metrics
func (mtc *MultiTierCache) GetMetrics() MultiTierMetrics {
	mtc.metrics.mu.RLock()
	defer mtc.metrics.mu.RUnlock()

	tierHits := make(map[int]int64)
	for k, v := range mtc.metrics.TierHits {
		tierHits[k] = v
	}

	return MultiTierMetrics{
		TierHits: tierHits,
		Misses:   mtc.metrics.Misses,
		Sets:     mtc.metrics.Sets,
		Deletes:  mtc.metrics.Deletes,
		Promotes: mtc.metrics.Promotes,
	}
}

// HitRate returns the overall hit rate
func (mtc *MultiTierCache) HitRate() float64 {
	mtc.metrics.mu.RLock()
	defer mtc.metrics.mu.RUnlock()

	var totalHits int64
	for _, hits := range mtc.metrics.TierHits {
		totalHits += hits
	}

	total := totalHits + mtc.metrics.Misses
	if total == 0 {
		return 0
	}
	return float64(totalHits) / float64(total)
}

// CacheAside implements the cache-aside pattern
type CacheAside struct {
	cache  Cache
	loader func(key string) (interface{}, error)
	ttl    time.Duration
}

// NewCacheAside creates a new cache-aside helper
func NewCacheAside(cache Cache, loader func(key string) (interface{}, error), ttl time.Duration) *CacheAside {
	return &CacheAside{
		cache:  cache,
		loader: loader,
		ttl:    ttl,
	}
}

// Get retrieves a value from cache or loads it
func (ca *CacheAside) Get(key string) (interface{}, error) {
	// Try cache first
	if value, found := ca.cache.Get(key); found {
		return value, nil
	}

	// Load from source
	value, err := ca.loader(key)
	if err != nil {
		return nil, err
	}

	// Store in cache
	if ca.ttl > 0 {
		ca.cache.SetWithTTL(key, value, ca.ttl)
	} else {
		ca.cache.Set(key, value)
	}

	return value, nil
}

// Set updates both cache and source
func (ca *CacheAside) Set(key string, value interface{}) {
	if ca.ttl > 0 {
		ca.cache.SetWithTTL(key, value, ca.ttl)
	} else {
		ca.cache.Set(key, value)
	}
}

// Delete removes from cache
func (ca *CacheAside) Delete(key string) {
	ca.cache.Delete(key)
}

// CacheWarmer preloads cache with data
type CacheWarmer struct {
	cache   Cache
	mu      sync.Mutex
	warming bool
}

// NewCacheWarmer creates a new cache warmer
func NewCacheWarmer(cache Cache) *CacheWarmer {
	return &CacheWarmer{
		cache: cache,
	}
}

// Warm preloads the cache with data
func (cw *CacheWarmer) Warm(ctx context.Context, loader func(ctx context.Context) (map[string]interface{}, error)) error {
	cw.mu.Lock()
	if cw.warming {
		cw.mu.Unlock()
		return fmt.Errorf("cache warming already in progress")
	}
	cw.warming = true
	cw.mu.Unlock()

	defer func() {
		cw.mu.Lock()
		cw.warming = false
		cw.mu.Unlock()
	}()

	data, err := loader(ctx)
	if err != nil {
		return fmt.Errorf("failed to load data: %w", err)
	}

	for key, value := range data {
		cw.cache.Set(key, value)
	}

	return nil
}

// WarmWithTTL preloads the cache with data and TTL
func (cw *CacheWarmer) WarmWithTTL(ctx context.Context, loader func(ctx context.Context) (map[string]interface{}, error), ttl time.Duration) error {
	cw.mu.Lock()
	if cw.warming {
		cw.mu.Unlock()
		return fmt.Errorf("cache warming already in progress")
	}
	cw.warming = true
	cw.mu.Unlock()

	defer func() {
		cw.mu.Lock()
		cw.warming = false
		cw.mu.Unlock()
	}()

	data, err := loader(ctx)
	if err != nil {
		return fmt.Errorf("failed to load data: %w", err)
	}

	for key, value := range data {
		cw.cache.SetWithTTL(key, value, ttl)
	}

	return nil
}

// IsWarming returns whether warming is in progress
func (cw *CacheWarmer) IsWarming() bool {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.warming
}
