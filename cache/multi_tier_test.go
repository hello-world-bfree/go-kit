package cache

import (
	"testing"
	"time"
)

func TestMultiTierCache(t *testing.T) {
	t.Run("basic multi-tier", func(t *testing.T) {
		config1 := DefaultMemoryCacheConfig()
		config1.MaxSize = 10
		tier1 := NewMemoryCache(config1)

		config2 := DefaultMemoryCacheConfig()
		config2.MaxSize = 100
		tier2 := NewMemoryCache(config2)

		mtConfig := DefaultMultiTierConfig()
		mtConfig.WriteThrough = true
		mtConfig.Promote = false

		cache := NewMultiTierCache(mtConfig, tier1, tier2)

		cache.Set("key1", "value1")

		// Should be in both tiers
		val, found := tier1.Get("key1")
		if !found || val != "value1" {
			t.Error("value should be in tier1")
		}

		val, found = tier2.Get("key1")
		if !found || val != "value1" {
			t.Error("value should be in tier2")
		}
	})

	t.Run("promotion on access", func(t *testing.T) {
		tier1 := NewMemoryCache(DefaultMemoryCacheConfig())
		tier2 := NewMemoryCache(DefaultMemoryCacheConfig())

		mtConfig := DefaultMultiTierConfig()
		mtConfig.WriteThrough = false
		mtConfig.Promote = true

		cache := NewMultiTierCache(mtConfig, tier1, tier2)

		// Set only in tier2
		tier2.Set("key1", "value1")

		// Access through multi-tier cache
		val, found := cache.Get("key1")
		if !found || val != "value1" {
			t.Error("value should be found in tier2")
		}

		// Give promotion time to complete
		time.Sleep(50 * time.Millisecond)

		// Should now be in tier1
		if !tier1.Has("key1") {
			t.Error("value should be promoted to tier1")
		}
	})

	t.Run("metrics tracking", func(t *testing.T) {
		tier1 := NewMemoryCache(DefaultMemoryCacheConfig())
		tier2 := NewMemoryCache(DefaultMemoryCacheConfig())

		cache := NewMultiTierCache(DefaultMultiTierConfig(), tier1, tier2)

		tier1.Set("key1", "value1")
		tier2.Set("key2", "value2")

		cache.Get("key1") // tier1 hit
		cache.Get("key2") // tier2 hit
		cache.Get("key3") // miss

		metrics := cache.GetMetrics()
		if metrics.TierHits[0] != 1 {
			t.Errorf("expected 1 tier1 hit, got %d", metrics.TierHits[0])
		}
		if metrics.TierHits[1] != 1 {
			t.Errorf("expected 1 tier2 hit, got %d", metrics.TierHits[1])
		}
		if metrics.Misses != 1 {
			t.Errorf("expected 1 miss, got %d", metrics.Misses)
		}
	})
}

func TestCacheAside(t *testing.T) {
	t.Run("basic cache-aside", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())
		loadCount := 0

		loader := func(key string) (interface{}, error) {
			loadCount++
			return "loaded-" + key, nil
		}

		ca := NewCacheAside(cache, loader, 1*time.Minute)

		// First get should load
		val, err := ca.Get("key1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "loaded-key1" {
			t.Errorf("expected 'loaded-key1', got %v", val)
		}
		if loadCount != 1 {
			t.Errorf("expected 1 load, got %d", loadCount)
		}

		// Second get should use cache
		val, err = ca.Get("key1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "loaded-key1" {
			t.Errorf("expected 'loaded-key1', got %v", val)
		}
		if loadCount != 1 {
			t.Errorf("expected 1 load (cached), got %d", loadCount)
		}
	})
}

func TestTypedCache(t *testing.T) {
	t.Run("typed operations", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())
		typed := NewTypedCache[string](cache)

		value := "test-value"
		err := typed.Set("key1", &value)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		retrieved, found := typed.Get("key1")
		if !found {
			t.Error("value should be found")
		}
		if *retrieved != value {
			t.Errorf("expected %s, got %s", value, *retrieved)
		}
	})

	t.Run("get or compute", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())
		typed := NewTypedCache[int](cache)

		computeCount := 0
		compute := func() (*int, error) {
			computeCount++
			val := 42
			return &val, nil
		}

		val, err := typed.GetOrCompute("key1", compute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if *val != 42 {
			t.Errorf("expected 42, got %d", *val)
		}
		if computeCount != 1 {
			t.Errorf("expected 1 compute, got %d", computeCount)
		}

		// Second call should use cache
		val, err = typed.GetOrCompute("key1", compute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if computeCount != 1 {
			t.Errorf("expected 1 compute (cached), got %d", computeCount)
		}
	})
}

func TestCacheGroup(t *testing.T) {
	t.Run("prefixed keys", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())
		group1 := NewCacheGroup(cache, "group1")
		group2 := NewCacheGroup(cache, "group2")

		group1.Set("key", "value1")
		group2.Set("key", "value2")

		val1, found := group1.Get("key")
		if !found || val1 != "value1" {
			t.Errorf("expected 'value1', got %v", val1)
		}

		val2, found := group2.Get("key")
		if !found || val2 != "value2" {
			t.Errorf("expected 'value2', got %v", val2)
		}
	})
}

func TestNullCache(t *testing.T) {
	cache := NewNullCache()

	cache.Set("key", "value")

	val, found := cache.Get("key")
	if found {
		t.Error("null cache should always return not found")
	}
	if val != nil {
		t.Error("null cache should always return nil")
	}

	if cache.Has("key") {
		t.Error("null cache should never have keys")
	}

	if cache.Delete("key") {
		t.Error("null cache delete should always return false")
	}

	cache.Clear() // Should not panic
}
