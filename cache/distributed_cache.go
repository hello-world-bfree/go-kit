package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// DistributedCache provides a generic interface for distributed caching
type DistributedCache interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
	Clear(ctx context.Context) error
}

// TypedCache provides type-safe caching operations
type TypedCache[T any] struct {
	cache      Cache
	serializer Serializer
}

// Serializer handles object serialization
type Serializer interface {
	Serialize(v interface{}) ([]byte, error)
	Deserialize(data []byte, v interface{}) error
}

// JSONSerializer implements JSON serialization
type JSONSerializer struct{}

// Serialize converts a value to JSON
func (js *JSONSerializer) Serialize(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Deserialize converts JSON to a value
func (js *JSONSerializer) Deserialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// NewTypedCache creates a new typed cache
func NewTypedCache[T any](cache Cache) *TypedCache[T] {
	return &TypedCache[T]{
		cache:      cache,
		serializer: &JSONSerializer{},
	}
}

// NewTypedCacheWithSerializer creates a typed cache with custom serializer
func NewTypedCacheWithSerializer[T any](cache Cache, serializer Serializer) *TypedCache[T] {
	return &TypedCache[T]{
		cache:      cache,
		serializer: serializer,
	}
}

// Get retrieves a typed value from the cache
func (tc *TypedCache[T]) Get(key string) (*T, bool) {
	rawValue, found := tc.cache.Get(key)
	if !found {
		return nil, false
	}

	// If already the right type, return it
	if value, ok := rawValue.(*T); ok {
		return value, true
	}

	// Try to deserialize if it's bytes
	if bytes, ok := rawValue.([]byte); ok {
		var value T
		if err := tc.serializer.Deserialize(bytes, &value); err == nil {
			return &value, true
		}
	}

	return nil, false
}

// Set stores a typed value in the cache
func (tc *TypedCache[T]) Set(key string, value *T) error {
	tc.cache.Set(key, value)
	return nil
}

// SetWithTTL stores a typed value with TTL
func (tc *TypedCache[T]) SetWithTTL(key string, value *T, ttl time.Duration) error {
	tc.cache.SetWithTTL(key, value, ttl)
	return nil
}

// GetOrCompute retrieves or computes a typed value
func (tc *TypedCache[T]) GetOrCompute(key string, compute func() (*T, error)) (*T, error) {
	if value, found := tc.Get(key); found {
		return value, nil
	}

	value, err := compute()
	if err != nil {
		return nil, err
	}

	tc.Set(key, value)
	return value, nil
}

// Delete removes a value from the cache
func (tc *TypedCache[T]) Delete(key string) bool {
	return tc.cache.Delete(key)
}

// Has checks if a key exists
func (tc *TypedCache[T]) Has(key string) bool {
	return tc.cache.Has(key)
}

// CacheGroup manages a group of related cache keys
type CacheGroup struct {
	cache  Cache
	prefix string
}

// NewCacheGroup creates a new cache group
func NewCacheGroup(cache Cache, prefix string) *CacheGroup {
	return &CacheGroup{
		cache:  cache,
		prefix: prefix,
	}
}

// Key generates a prefixed key
func (cg *CacheGroup) Key(key string) string {
	return fmt.Sprintf("%s:%s", cg.prefix, key)
}

// Get retrieves a value from the group
func (cg *CacheGroup) Get(key string) (interface{}, bool) {
	return cg.cache.Get(cg.Key(key))
}

// Set stores a value in the group
func (cg *CacheGroup) Set(key string, value interface{}) {
	cg.cache.Set(cg.Key(key), value)
}

// SetWithTTL stores a value with TTL
func (cg *CacheGroup) SetWithTTL(key string, value interface{}, ttl time.Duration) {
	cg.cache.SetWithTTL(cg.Key(key), value, ttl)
}

// Delete removes a value from the group
func (cg *CacheGroup) Delete(key string) bool {
	return cg.cache.Delete(cg.Key(key))
}

// Has checks if a key exists in the group
func (cg *CacheGroup) Has(key string) bool {
	return cg.cache.Has(cg.Key(key))
}

// NullCache is a no-op cache implementation
type NullCache struct{}

// NewNullCache creates a new null cache
func NewNullCache() *NullCache {
	return &NullCache{}
}

// Get always returns not found
func (nc *NullCache) Get(key string) (interface{}, bool) {
	return nil, false
}

// Set does nothing
func (nc *NullCache) Set(key string, value interface{}) {}

// SetWithTTL does nothing
func (nc *NullCache) SetWithTTL(key string, value interface{}, ttl time.Duration) {}

// Delete does nothing
func (nc *NullCache) Delete(key string) bool {
	return false
}

// Has always returns false
func (nc *NullCache) Has(key string) bool {
	return false
}

// Clear does nothing
func (nc *NullCache) Clear() {}
