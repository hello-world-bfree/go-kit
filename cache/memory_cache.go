package cache

import (
	"container/list"
	"sync"
	"time"
)

// EvictionPolicy defines the cache eviction strategy
type EvictionPolicy int

const (
	// LRU - Least Recently Used
	LRU EvictionPolicy = iota
	// LFU - Least Frequently Used
	LFU
	// FIFO - First In First Out
	FIFO
)

// CacheItem represents a cached item with metadata
type CacheItem struct {
	Key        string
	Value      interface{}
	ExpiresAt  time.Time
	AccessedAt time.Time
	CreatedAt  time.Time
	Frequency  int64
	Size       int64
}

// IsExpired checks if the item has expired
func (ci *CacheItem) IsExpired() bool {
	if ci.ExpiresAt.IsZero() {
		return false
	}
	return time.Now().After(ci.ExpiresAt)
}

// MemoryCache is an in-memory cache with configurable eviction policies
type MemoryCache struct {
	mu             sync.RWMutex
	items          map[string]*list.Element
	evictionList   *list.List
	maxSize        int64
	currentSize    int64
	evictionPolicy EvictionPolicy
	defaultTTL     time.Duration
	metrics        *CacheMetrics
	onEvict        func(key string, value interface{})
}

// CacheMetrics tracks cache statistics
type CacheMetrics struct {
	mu         sync.RWMutex
	Hits       int64
	Misses     int64
	Sets       int64
	Deletes    int64
	Evictions  int64
	Expirations int64
}

// MemoryCacheConfig configures the memory cache
type MemoryCacheConfig struct {
	MaxSize        int64          // Maximum cache size (number of items, 0 = unlimited)
	EvictionPolicy EvictionPolicy // Eviction policy to use
	DefaultTTL     time.Duration  // Default TTL for items (0 = no expiration)
	CleanupInterval time.Duration // How often to clean up expired items
	OnEvict        func(key string, value interface{}) // Callback on eviction
}

// DefaultMemoryCacheConfig returns a config with sensible defaults
func DefaultMemoryCacheConfig() MemoryCacheConfig {
	return MemoryCacheConfig{
		MaxSize:         1000,
		EvictionPolicy:  LRU,
		DefaultTTL:      5 * time.Minute,
		CleanupInterval: 1 * time.Minute,
	}
}

// NewMemoryCache creates a new in-memory cache
func NewMemoryCache(config MemoryCacheConfig) *MemoryCache {
	cache := &MemoryCache{
		items:          make(map[string]*list.Element),
		evictionList:   list.New(),
		maxSize:        config.MaxSize,
		evictionPolicy: config.EvictionPolicy,
		defaultTTL:     config.DefaultTTL,
		metrics:        &CacheMetrics{},
		onEvict:        config.OnEvict,
	}

	// Start cleanup routine if configured
	if config.CleanupInterval > 0 {
		go cache.cleanupRoutine(config.CleanupInterval)
	}

	return cache
}

// Set stores a value in the cache with default TTL
func (mc *MemoryCache) Set(key string, value interface{}) {
	mc.SetWithTTL(key, value, mc.defaultTTL)
}

// SetWithTTL stores a value with a specific TTL
func (mc *MemoryCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.metrics.mu.Lock()
	mc.metrics.Sets++
	mc.metrics.mu.Unlock()

	now := time.Now()
	item := &CacheItem{
		Key:        key,
		Value:      value,
		CreatedAt:  now,
		AccessedAt: now,
		Frequency:  1,
	}

	if ttl > 0 {
		item.ExpiresAt = now.Add(ttl)
	}

	// Update existing item
	if elem, exists := mc.items[key]; exists {
		mc.evictionList.Remove(elem)
		mc.currentSize--
	}

	// Add new item
	elem := mc.evictionList.PushFront(item)
	mc.items[key] = elem
	mc.currentSize++

	// Evict if necessary
	mc.evictIfNeeded()
}

// Get retrieves a value from the cache
func (mc *MemoryCache) Get(key string) (interface{}, bool) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	elem, exists := mc.items[key]
	if !exists {
		mc.metrics.mu.Lock()
		mc.metrics.Misses++
		mc.metrics.mu.Unlock()
		return nil, false
	}

	item := elem.Value.(*CacheItem)

	// Check expiration
	if item.IsExpired() {
		mc.removeElement(elem)
		mc.metrics.mu.Lock()
		mc.metrics.Misses++
		mc.metrics.Expirations++
		mc.metrics.mu.Unlock()
		return nil, false
	}

	// Update access metadata
	item.AccessedAt = time.Now()
	item.Frequency++

	// Move to front for LRU
	if mc.evictionPolicy == LRU {
		mc.evictionList.MoveToFront(elem)
	}

	mc.metrics.mu.Lock()
	mc.metrics.Hits++
	mc.metrics.mu.Unlock()

	return item.Value, true
}

// GetOrSet retrieves a value or sets it if not found
func (mc *MemoryCache) GetOrSet(key string, defaultValue interface{}) interface{} {
	if value, found := mc.Get(key); found {
		return value
	}
	mc.Set(key, defaultValue)
	return defaultValue
}

// GetOrCompute retrieves a value or computes it if not found
func (mc *MemoryCache) GetOrCompute(key string, compute func() (interface{}, error)) (interface{}, error) {
	if value, found := mc.Get(key); found {
		return value, nil
	}

	value, err := compute()
	if err != nil {
		return nil, err
	}

	mc.Set(key, value)
	return value, nil
}

// Delete removes a value from the cache
func (mc *MemoryCache) Delete(key string) bool {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	elem, exists := mc.items[key]
	if !exists {
		return false
	}

	mc.removeElement(elem)

	mc.metrics.mu.Lock()
	mc.metrics.Deletes++
	mc.metrics.mu.Unlock()

	return true
}

// Has checks if a key exists in the cache
func (mc *MemoryCache) Has(key string) bool {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	elem, exists := mc.items[key]
	if !exists {
		return false
	}

	item := elem.Value.(*CacheItem)
	return !item.IsExpired()
}

// Clear removes all items from the cache
func (mc *MemoryCache) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.items = make(map[string]*list.Element)
	mc.evictionList.Init()
	mc.currentSize = 0
}

// Size returns the current number of items in the cache
func (mc *MemoryCache) Size() int64 {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.currentSize
}

// Keys returns all keys in the cache
func (mc *MemoryCache) Keys() []string {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	keys := make([]string, 0, len(mc.items))
	for key := range mc.items {
		keys = append(keys, key)
	}
	return keys
}

// GetMetrics returns a copy of the cache metrics
func (mc *MemoryCache) GetMetrics() CacheMetrics {
	mc.metrics.mu.RLock()
	defer mc.metrics.mu.RUnlock()

	return CacheMetrics{
		Hits:        mc.metrics.Hits,
		Misses:      mc.metrics.Misses,
		Sets:        mc.metrics.Sets,
		Deletes:     mc.metrics.Deletes,
		Evictions:   mc.metrics.Evictions,
		Expirations: mc.metrics.Expirations,
	}
}

// HitRate returns the cache hit rate
func (mc *MemoryCache) HitRate() float64 {
	mc.metrics.mu.RLock()
	defer mc.metrics.mu.RUnlock()

	total := mc.metrics.Hits + mc.metrics.Misses
	if total == 0 {
		return 0
	}
	return float64(mc.metrics.Hits) / float64(total)
}

// evictIfNeeded evicts items if cache is over capacity (must be called with lock held)
func (mc *MemoryCache) evictIfNeeded() {
	if mc.maxSize <= 0 {
		return
	}

	for mc.currentSize > mc.maxSize {
		mc.evictOne()
	}
}

// evictOne evicts a single item based on the eviction policy (must be called with lock held)
func (mc *MemoryCache) evictOne() {
	var elem *list.Element

	switch mc.evictionPolicy {
	case LRU, FIFO:
		// Remove from back (least recently used / oldest)
		elem = mc.evictionList.Back()

	case LFU:
		// Find least frequently used
		elem = mc.findLFU()
	}

	if elem != nil {
		mc.removeElement(elem)

		mc.metrics.mu.Lock()
		mc.metrics.Evictions++
		mc.metrics.mu.Unlock()
	}
}

// findLFU finds the least frequently used item (must be called with lock held)
func (mc *MemoryCache) findLFU() *list.Element {
	var minElem *list.Element
	var minFreq int64 = -1

	for e := mc.evictionList.Front(); e != nil; e = e.Next() {
		item := e.Value.(*CacheItem)
		if minFreq == -1 || item.Frequency < minFreq {
			minFreq = item.Frequency
			minElem = e
		}
	}

	return minElem
}

// removeElement removes an element from the cache (must be called with lock held)
func (mc *MemoryCache) removeElement(elem *list.Element) {
	item := elem.Value.(*CacheItem)

	if mc.onEvict != nil {
		mc.onEvict(item.Key, item.Value)
	}

	delete(mc.items, item.Key)
	mc.evictionList.Remove(elem)
	mc.currentSize--
}

// cleanupRoutine periodically removes expired items
func (mc *MemoryCache) cleanupRoutine(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		mc.cleanupExpired()
	}
}

// cleanupExpired removes all expired items
func (mc *MemoryCache) cleanupExpired() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	var toRemove []*list.Element

	for e := mc.evictionList.Front(); e != nil; e = e.Next() {
		item := e.Value.(*CacheItem)
		if item.IsExpired() {
			toRemove = append(toRemove, e)
		}
	}

	for _, elem := range toRemove {
		mc.removeElement(elem)

		mc.metrics.mu.Lock()
		mc.metrics.Expirations++
		mc.metrics.mu.Unlock()
	}
}
