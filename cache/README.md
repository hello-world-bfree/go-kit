# Cache Package

The `cache` package provides in-memory and multi-tier caching solutions with various eviction policies and utilities.

## Features

### Memory Cache

In-memory cache with configurable eviction policies (LRU, LFU, FIFO).

```go
import "github.com/flarco/g/cache"

config := cache.DefaultMemoryCacheConfig()
config.MaxSize = 1000
config.EvictionPolicy = cache.LRU
config.DefaultTTL = 5 * time.Minute

cache := cache.NewMemoryCache(config)

// Set and get
cache.Set("key", "value")
value, found := cache.Get("key")

// With TTL
cache.SetWithTTL("key", "value", 1*time.Minute)

// Get or compute
value, err := cache.GetOrCompute("key", func() (interface{}, error) {
    return expensiveOperation(), nil
})

// Metrics
metrics := cache.GetMetrics()
hitRate := cache.HitRate()
```

### Eviction Policies

- **LRU (Least Recently Used)**: Evicts least recently accessed items
- **LFU (Least Frequently Used)**: Evicts least frequently accessed items
- **FIFO (First In First Out)**: Evicts oldest items first

```go
config := cache.DefaultMemoryCacheConfig()
config.EvictionPolicy = cache.LRU // or cache.LFU, cache.FIFO
```

### Multi-Tier Cache

Combine multiple cache tiers (e.g., memory + Redis) for optimal performance.

```go
// Create tiers
tier1 := cache.NewMemoryCache(fastConfig)   // L1: Fast, small
tier2 := cache.NewMemoryCache(largeConfig)  // L2: Slower, larger

// Create multi-tier cache
mtConfig := cache.DefaultMultiTierConfig()
mtConfig.WriteThrough = true  // Write to all tiers
mtConfig.Promote = true       // Promote to higher tiers on access

mtCache := cache.NewMultiTierCache(mtConfig, tier1, tier2)

// Use like a regular cache
mtCache.Set("key", "value")
value, found := mtCache.Get("key")

// Metrics
metrics := mtCache.GetMetrics()
hitRate := mtCache.HitRate()
```

### Cache-Aside Pattern

Implements the cache-aside (lazy loading) pattern.

```go
loader := func(key string) (interface{}, error) {
    // Load from database or external source
    return database.Get(key)
}

ca := cache.NewCacheAside(memCache, loader, 5*time.Minute)

// Automatically loads and caches if not found
value, err := ca.Get("key")
```

### Cache Warming

Preload cache with data for better performance.

```go
warmer := cache.NewCacheWarmer(memCache)

loader := func(ctx context.Context) (map[string]interface{}, error) {
    // Load data from database
    return loadAllData()
}

err := warmer.Warm(ctx, loader)
```

### Typed Cache

Type-safe cache operations with generics.

```go
type User struct {
    ID   int
    Name string
}

userCache := cache.NewTypedCache[User](memCache)

user := &User{ID: 1, Name: "Alice"}
userCache.Set("user:1", user)

retrieved, found := userCache.Get("user:1")
if found {
    fmt.Printf("User: %s\n", retrieved.Name)
}

// Get or compute with type safety
user, err := userCache.GetOrCompute("user:2", func() (*User, error) {
    return loadUserFromDB(2)
})
```

### Cache Groups

Organize cache keys into logical groups with prefixes.

```go
userCache := cache.NewCacheGroup(memCache, "users")
sessionCache := cache.NewCacheGroup(memCache, "sessions")

// Keys are automatically prefixed
userCache.Set("123", userData)      // Stored as "users:123"
sessionCache.Set("abc", sessionData) // Stored as "sessions:abc"
```

### Null Cache

No-op cache implementation for testing or disabling caching.

```go
nullCache := cache.NewNullCache()

// All operations are no-ops
nullCache.Set("key", "value")
_, found := nullCache.Get("key") // Always returns false
```

## Configuration

### Memory Cache

```go
config := cache.MemoryCacheConfig{
    MaxSize:         1000,                // Maximum items (0 = unlimited)
    EvictionPolicy:  cache.LRU,          // Eviction policy
    DefaultTTL:      5 * time.Minute,    // Default TTL
    CleanupInterval: 1 * time.Minute,    // Cleanup frequency
    OnEvict: func(key string, value interface{}) {
        // Eviction callback
    },
}
```

### Multi-Tier Cache

```go
config := cache.MultiTierConfig{
    WriteThrough: true, // Write to all tiers on set
    Promote:      true, // Promote items to higher tiers on access
}
```

## Metrics

Track cache performance:

- `Hits`: Cache hits
- `Misses`: Cache misses
- `Sets`: Number of set operations
- `Deletes`: Number of delete operations
- `Evictions`: Number of evictions
- `Expirations`: Number of expired items
- `HitRate()`: Cache hit rate (hits / (hits + misses))

## Use Cases

### API Response Caching

```go
apiCache := cache.NewMemoryCache(config)

func getUser(id string) (*User, error) {
    // Check cache
    if cached, found := apiCache.Get(id); found {
        return cached.(*User), nil
    }

    // Load from database
    user, err := database.GetUser(id)
    if err != nil {
        return nil, err
    }

    // Cache for 5 minutes
    apiCache.SetWithTTL(id, user, 5*time.Minute)
    return user, nil
}
```

### Multi-Tier Caching

```go
// L1: Fast in-memory (100 items, 1 minute TTL)
l1Config := cache.DefaultMemoryCacheConfig()
l1Config.MaxSize = 100
l1Config.DefaultTTL = 1 * time.Minute
l1 := cache.NewMemoryCache(l1Config)

// L2: Larger in-memory (10000 items, 10 minute TTL)
l2Config := cache.DefaultMemoryCacheConfig()
l2Config.MaxSize = 10000
l2Config.DefaultTTL = 10 * time.Minute
l2 := cache.NewMemoryCache(l2Config)

mtCache := cache.NewMultiTierCache(cache.DefaultMultiTierConfig(), l1, l2)
```

### Rate-Limited Cache Warming

```go
warmer := cache.NewCacheWarmer(cache)
limiter := concurrency.NewRateLimiter(10, 10) // 10 ops/sec

loader := func(ctx context.Context) (map[string]interface{}, error) {
    data := make(map[string]interface{})
    for _, key := range keysToWarm {
        limiter.Wait(ctx)
        value, err := loadFromDB(key)
        if err != nil {
            return nil, err
        }
        data[key] = value
    }
    return data, nil
}

warmer.Warm(context.Background(), loader)
```

## Best Practices

1. **Choose appropriate eviction policy**: LRU for general use, LFU for hot keys
2. **Set reasonable TTLs**: Balance freshness vs. performance
3. **Monitor metrics**: Track hit rates and adjust configuration
4. **Use multi-tier**: Combine fast L1 with larger L2 for optimal performance
5. **Implement cache warming**: Preload critical data on startup
6. **Use typed caches**: Benefit from compile-time type safety
7. **Group related keys**: Use cache groups for logical organization
