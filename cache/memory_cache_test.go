package cache

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryCache(t *testing.T) {
	t.Run("basic set and get", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())

		cache.Set("key1", "value1")
		cache.Set("key2", 42)

		val, found := cache.Get("key1")
		if !found || val != "value1" {
			t.Errorf("expected 'value1', got %v", val)
		}

		val, found = cache.Get("key2")
		if !found || val != 42 {
			t.Errorf("expected 42, got %v", val)
		}
	})

	t.Run("TTL expiration", func(t *testing.T) {
		config := DefaultMemoryCacheConfig()
		config.DefaultTTL = 0
		cache := NewMemoryCache(config)

		cache.SetWithTTL("key", "value", 100*time.Millisecond)

		val, found := cache.Get("key")
		if !found || val != "value" {
			t.Error("value should be present")
		}

		time.Sleep(150 * time.Millisecond)

		_, found = cache.Get("key")
		if found {
			t.Error("value should be expired")
		}
	})

	t.Run("LRU eviction", func(t *testing.T) {
		config := DefaultMemoryCacheConfig()
		config.MaxSize = 3
		config.EvictionPolicy = LRU
		cache := NewMemoryCache(config)

		cache.Set("key1", "value1")
		cache.Set("key2", "value2")
		cache.Set("key3", "value3")

		// Access key1 to make it recently used
		cache.Get("key1")

		// Add key4, should evict key2 (least recently used)
		cache.Set("key4", "value4")

		if cache.Has("key2") {
			t.Error("key2 should have been evicted")
		}
		if !cache.Has("key1") {
			t.Error("key1 should still be present")
		}
	})

	t.Run("concurrent access", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())
		var wg sync.WaitGroup

		// Concurrent writes
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				cache.Set(string(rune(n)), n)
			}(i)
		}

		// Concurrent reads
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				cache.Get(string(rune(n)))
			}(i)
		}

		wg.Wait()
	})

	t.Run("metrics tracking", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())

		cache.Set("key1", "value1")
		cache.Set("key2", "value2")

		cache.Get("key1") // hit
		cache.Get("key1") // hit
		cache.Get("key3") // miss

		metrics := cache.GetMetrics()
		if metrics.Hits != 2 {
			t.Errorf("expected 2 hits, got %d", metrics.Hits)
		}
		if metrics.Misses != 1 {
			t.Errorf("expected 1 miss, got %d", metrics.Misses)
		}
		if metrics.Sets != 2 {
			t.Errorf("expected 2 sets, got %d", metrics.Sets)
		}

		hitRate := cache.HitRate()
		expectedRate := 2.0 / 3.0
		if hitRate < expectedRate-0.01 || hitRate > expectedRate+0.01 {
			t.Errorf("expected hit rate ~0.67, got %f", hitRate)
		}
	})

	t.Run("get or compute", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())

		computeCalls := 0
		compute := func() (interface{}, error) {
			computeCalls++
			return "computed", nil
		}

		// First call should compute
		val, err := cache.GetOrCompute("key", compute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "computed" {
			t.Errorf("expected 'computed', got %v", val)
		}
		if computeCalls != 1 {
			t.Errorf("expected 1 compute call, got %d", computeCalls)
		}

		// Second call should use cache
		val, err = cache.GetOrCompute("key", compute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if val != "computed" {
			t.Errorf("expected 'computed', got %v", val)
		}
		if computeCalls != 1 {
			t.Errorf("expected 1 compute call, got %d", computeCalls)
		}
	})

	t.Run("clear cache", func(t *testing.T) {
		cache := NewMemoryCache(DefaultMemoryCacheConfig())

		cache.Set("key1", "value1")
		cache.Set("key2", "value2")

		if cache.Size() != 2 {
			t.Errorf("expected size 2, got %d", cache.Size())
		}

		cache.Clear()

		if cache.Size() != 0 {
			t.Errorf("expected size 0 after clear, got %d", cache.Size())
		}
	})

	t.Run("eviction callback", func(t *testing.T) {
		evicted := make(map[string]interface{})
		config := DefaultMemoryCacheConfig()
		config.MaxSize = 2
		config.OnEvict = func(key string, value interface{}) {
			evicted[key] = value
		}

		cache := NewMemoryCache(config)

		cache.Set("key1", "value1")
		cache.Set("key2", "value2")
		cache.Set("key3", "value3")

		if len(evicted) != 1 {
			t.Errorf("expected 1 eviction, got %d", len(evicted))
		}
	})
}

func BenchmarkMemoryCache(b *testing.B) {
	cache := NewMemoryCache(DefaultMemoryCacheConfig())

	b.Run("Set", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cache.Set(string(rune(i)), i)
		}
	})

	b.Run("Get", func(b *testing.B) {
		for i := 0; i < 1000; i++ {
			cache.Set(string(rune(i)), i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cache.Get(string(rune(i % 1000)))
		}
	})

	b.Run("Concurrent", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				if i%2 == 0 {
					cache.Set(string(rune(i)), i)
				} else {
					cache.Get(string(rune(i)))
				}
				i++
			}
		})
	})
}
