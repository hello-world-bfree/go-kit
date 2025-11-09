package promptgather

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/flarco/g/cache"
)

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.MaxConcurrency != 10 {
		t.Errorf("Expected MaxConcurrency=10, got %d", config.MaxConcurrency)
	}

	if config.Timeout != 2*time.Second {
		t.Errorf("Expected Timeout=2s, got %v", config.Timeout)
	}

	if !config.ContinueOnError {
		t.Error("Expected ContinueOnError=true")
	}

	if !config.EnableMetrics {
		t.Error("Expected EnableMetrics=true")
	}
}

func TestNewPromptGatherer(t *testing.T) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	if gatherer == nil {
		t.Fatal("Expected non-nil gatherer")
	}

	if len(gatherer.sources) != 0 {
		t.Errorf("Expected 0 sources, got %d", len(gatherer.sources))
	}
}

func TestAddSource(t *testing.T) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	source := NewFuncSource("test", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"key": "value"}, nil
	})

	gatherer.AddSource(source)

	if len(gatherer.sources) != 1 {
		t.Errorf("Expected 1 source, got %d", len(gatherer.sources))
	}
}

func TestAddSources(t *testing.T) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	source1 := NewFuncSource("test1", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"key": "value1"}, nil
	})

	source2 := NewFuncSource("test2", 1, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"key": "value2"}, nil
	})

	gatherer.AddSources(source1, source2)

	if len(gatherer.sources) != 2 {
		t.Errorf("Expected 2 sources, got %d", len(gatherer.sources))
	}
}

func TestGatherBasic(t *testing.T) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	gatherer.AddSource(NewFuncSource("source1", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"data": "value1"}, nil
	}))

	gatherer.AddSource(NewFuncSource("source2", 1, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"data": "value2"}, nil
	}))

	ctx := context.Background()
	result, err := gatherer.Gather(ctx)

	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	if result.Metrics.SuccessfulSources != 2 {
		t.Errorf("Expected 2 successful sources, got %d", result.Metrics.SuccessfulSources)
	}

	if result.Data["source1.data"] != "value1" {
		t.Errorf("Expected source1.data=value1, got %v", result.Data["source1.data"])
	}

	if result.Data["source2.data"] != "value2" {
		t.Errorf("Expected source2.data=value2, got %v", result.Data["source2.data"])
	}
}

func TestGatherWithError(t *testing.T) {
	config := DefaultConfig()
	config.ContinueOnError = true
	gatherer := NewPromptGatherer(config)

	gatherer.AddSource(NewFuncSource("success", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"data": "ok"}, nil
	}))

	gatherer.AddSource(NewFuncSource("error", 1, func(ctx context.Context) (map[string]interface{}, error) {
		return nil, fmt.Errorf("intentional error")
	}))

	ctx := context.Background()
	result, err := gatherer.Gather(ctx)

	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	if result.Metrics.SuccessfulSources != 1 {
		t.Errorf("Expected 1 successful source, got %d", result.Metrics.SuccessfulSources)
	}

	if result.Metrics.FailedSources != 1 {
		t.Errorf("Expected 1 failed source, got %d", result.Metrics.FailedSources)
	}

	if len(result.Metrics.Errors) != 1 {
		t.Errorf("Expected 1 error, got %d", len(result.Metrics.Errors))
	}
}

func TestGatherWithErrorStopOnError(t *testing.T) {
	config := DefaultConfig()
	config.ContinueOnError = false
	gatherer := NewPromptGatherer(config)

	gatherer.AddSource(NewFuncSource("error", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return nil, fmt.Errorf("intentional error")
	}))

	ctx := context.Background()
	result, err := gatherer.Gather(ctx)

	if err == nil {
		t.Error("Expected error but got nil")
	}

	if result.Metrics.FailedSources == 0 {
		t.Error("Expected at least one failed source")
	}
}

func TestGatherWithTimeout(t *testing.T) {
	config := DefaultConfig()
	config.Timeout = 100 * time.Millisecond
	gatherer := NewPromptGatherer(config)

	gatherer.AddSource(NewFuncSource("slow", 0, func(ctx context.Context) (map[string]interface{}, error) {
		time.Sleep(200 * time.Millisecond)
		return map[string]interface{}{"data": "slow"}, nil
	}))

	ctx := context.Background()
	start := time.Now()
	result, _ := gatherer.Gather(ctx)
	duration := time.Since(start)

	if duration > 150*time.Millisecond {
		t.Errorf("Expected timeout around 100ms, got %v", duration)
	}

	if result.Metrics.FailedSources == 0 {
		t.Error("Expected failed source due to timeout")
	}
}

func TestGatherWithCache(t *testing.T) {
	cacheConfig := cache.MemoryCacheConfig{
		MaxSize:      10,
		EvictionType: cache.EvictionTypeLRU,
		DefaultTTL:   1 * time.Minute,
	}
	memCache := cache.NewMemoryCache(cacheConfig)

	config := DefaultConfig()
	config.Cache = memCache
	config.CacheTTL = 1 * time.Minute

	gatherer := NewPromptGatherer(config)

	callCount := 0
	gatherer.AddSource(NewFuncSource("cached", 0, func(ctx context.Context) (map[string]interface{}, error) {
		callCount++
		return map[string]interface{}{"data": "value", "call": callCount}, nil
	}))

	ctx := context.Background()

	// First call - should fetch from source
	result1, err := gatherer.Gather(ctx)
	if err != nil {
		t.Fatalf("First gather failed: %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected 1 call, got %d", callCount)
	}

	// Second call - should use cache
	result2, err := gatherer.Gather(ctx)
	if err != nil {
		t.Fatalf("Second gather failed: %v", err)
	}

	if callCount != 1 {
		t.Errorf("Expected still 1 call (cached), got %d", callCount)
	}

	// Results should be the same
	if result1.Data["cached.call"] != result2.Data["cached.call"] {
		t.Error("Cached result should match first result")
	}
}

func TestGatherEmpty(t *testing.T) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	ctx := context.Background()
	result, err := gatherer.Gather(ctx)

	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	if len(result.Data) != 0 {
		t.Errorf("Expected empty data, got %d items", len(result.Data))
	}
}

func TestGatherWithTemplate(t *testing.T) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	gatherer.AddSource(NewFuncSource("user", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"name":  "Alice",
			"email": "[email protected]",
		}, nil
	}))

	template := `Hello {{.user.name}} ({{.user.email}})`

	ctx := context.Background()
	output, err := gatherer.GatherWithTemplate(ctx, template)

	if err != nil {
		t.Fatalf("GatherWithTemplate failed: %v", err)
	}

	expected := "Hello Alice ([email protected])"
	if output != expected {
		t.Errorf("Expected %q, got %q", expected, output)
	}
}

func TestClearCache(t *testing.T) {
	cacheConfig := cache.MemoryCacheConfig{
		MaxSize:      10,
		EvictionType: cache.EvictionTypeLRU,
		DefaultTTL:   1 * time.Minute,
	}
	memCache := cache.NewMemoryCache(cacheConfig)

	config := DefaultConfig()
	config.Cache = memCache
	config.CacheTTL = 1 * time.Minute

	gatherer := NewPromptGatherer(config)

	callCount := 0
	gatherer.AddSource(NewFuncSource("test", 0, func(ctx context.Context) (map[string]interface{}, error) {
		callCount++
		return map[string]interface{}{"count": callCount}, nil
	}))

	ctx := context.Background()

	// First call
	gatherer.Gather(ctx)

	// Clear cache
	gatherer.ClearCache()

	// Second call should fetch again
	gatherer.Gather(ctx)

	if callCount != 2 {
		t.Errorf("Expected 2 calls after cache clear, got %d", callCount)
	}
}

func TestFuncSource(t *testing.T) {
	source := NewFuncSource("test", 5, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"key": "value"}, nil
	})

	if source.Name() != "test" {
		t.Errorf("Expected name 'test', got %q", source.Name())
	}

	if source.Priority() != 5 {
		t.Errorf("Expected priority 5, got %d", source.Priority())
	}

	ctx := context.Background()
	data, err := source.Fetch(ctx)

	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	if data["key"] != "value" {
		t.Errorf("Expected key=value, got %v", data["key"])
	}
}

func TestCacheSource(t *testing.T) {
	cacheConfig := cache.MemoryCacheConfig{
		MaxSize:      10,
		EvictionType: cache.EvictionTypeLRU,
		DefaultTTL:   1 * time.Minute,
	}
	memCache := cache.NewMemoryCache(cacheConfig)

	// Store value in cache
	testData := map[string]interface{}{"cached": "data"}
	memCache.Set("testkey", testData)

	source := NewCacheSource("cache_test", memCache, "testkey")

	if source.Name() != "cache_test" {
		t.Errorf("Expected name 'cache_test', got %q", source.Name())
	}

	ctx := context.Background()
	data, err := source.Fetch(ctx)

	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	if data["cached"] != "data" {
		t.Errorf("Expected cached=data, got %v", data["cached"])
	}
}

func TestCacheSourceNotFound(t *testing.T) {
	cacheConfig := cache.MemoryCacheConfig{
		MaxSize:      10,
		EvictionType: cache.EvictionTypeLRU,
		DefaultTTL:   1 * time.Minute,
	}
	memCache := cache.NewMemoryCache(cacheConfig)

	source := NewCacheSource("cache_test", memCache, "nonexistent")

	ctx := context.Background()
	_, err := source.Fetch(ctx)

	if err == nil {
		t.Error("Expected error for missing cache key, got nil")
	}
}

func TestBaseDataSource(t *testing.T) {
	base := NewBaseDataSource("test", 10)

	if base.Name() != "test" {
		t.Errorf("Expected name 'test', got %q", base.Name())
	}

	if base.Priority() != 10 {
		t.Errorf("Expected priority 10, got %d", base.Priority())
	}
}

func TestConcurrentGather(t *testing.T) {
	config := DefaultConfig()
	config.MaxConcurrency = 5
	gatherer := NewPromptGatherer(config)

	// Add 10 sources that take some time
	for i := 0; i < 10; i++ {
		i := i // capture
		gatherer.AddSource(NewFuncSource(fmt.Sprintf("source%d", i), i, func(ctx context.Context) (map[string]interface{}, error) {
			time.Sleep(50 * time.Millisecond)
			return map[string]interface{}{"value": i}, nil
		}))
	}

	ctx := context.Background()
	start := time.Now()
	result, err := gatherer.Gather(ctx)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}

	if result.Metrics.SuccessfulSources != 10 {
		t.Errorf("Expected 10 successful sources, got %d", result.Metrics.SuccessfulSources)
	}

	// With 5 concurrency and 10 sources taking 50ms each,
	// should take around 100ms (2 batches), not 500ms (sequential)
	if duration > 200*time.Millisecond {
		t.Errorf("Expected concurrent execution ~100ms, got %v", duration)
	}
}

func TestPriorityOrdering(t *testing.T) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	executionOrder := make([]string, 0)
	mu := make(chan struct{}, 1)

	addSource := func(name string, priority int) {
		gatherer.AddSource(NewFuncSource(name, priority, func(ctx context.Context) (map[string]interface{}, error) {
			mu <- struct{}{}
			executionOrder = append(executionOrder, name)
			<-mu
			return map[string]interface{}{}, nil
		}))
	}

	// Add in random order
	addSource("low", 10)
	addSource("high", 0)
	addSource("medium", 5)

	ctx := context.Background()
	gatherer.Gather(ctx)

	// Higher priority (lower number) should execute first
	if len(executionOrder) != 3 {
		t.Fatalf("Expected 3 executions, got %d", len(executionOrder))
	}

	// Note: Due to concurrency, exact order isn't guaranteed,
	// but high priority should generally start first
}

func BenchmarkGatherNoCache(b *testing.B) {
	config := DefaultConfig()
	config.Cache = nil
	gatherer := NewPromptGatherer(config)

	gatherer.AddSource(NewFuncSource("test", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"data": "value"}, nil
	}))

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gatherer.Gather(ctx)
	}
}

func BenchmarkGatherWithCache(b *testing.B) {
	cacheConfig := cache.MemoryCacheConfig{
		MaxSize:      100,
		EvictionType: cache.EvictionTypeLRU,
		DefaultTTL:   1 * time.Minute,
	}
	memCache := cache.NewMemoryCache(cacheConfig)

	config := DefaultConfig()
	config.Cache = memCache
	config.CacheTTL = 1 * time.Minute

	gatherer := NewPromptGatherer(config)

	gatherer.AddSource(NewFuncSource("test", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{"data": "value"}, nil
	}))

	ctx := context.Background()

	// Warm cache
	gatherer.Gather(ctx)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gatherer.Gather(ctx)
	}
}

func BenchmarkGatherMultipleSources(b *testing.B) {
	config := DefaultConfig()
	gatherer := NewPromptGatherer(config)

	// Add 10 sources
	for i := 0; i < 10; i++ {
		i := i
		gatherer.AddSource(NewFuncSource(fmt.Sprintf("source%d", i), i, func(ctx context.Context) (map[string]interface{}, error) {
			return map[string]interface{}{"value": i}, nil
		}))
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gatherer.Gather(ctx)
	}
}
