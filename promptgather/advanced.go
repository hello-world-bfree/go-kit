package promptgather

import (
	"context"
	"fmt"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/cache"
	"github.com/flarco/g/cacheredis"
	"github.com/flarco/g/concurrency"
)

// BatchGatherer processes multiple prompt gathering requests in batches
// Useful for processing multiple user requests efficiently
type BatchGatherer struct {
	gatherer  *PromptGatherer
	processor *concurrency.BatchProcessor[*BatchRequest]
}

// BatchRequest represents a single gathering request in a batch
type BatchRequest struct {
	ID       string
	Template string
	Context  context.Context
	Result   chan BatchResponse
}

// BatchResponse contains the result of a batch request
type BatchResponse struct {
	ID     string
	Output string
	Error  error
}

// NewBatchGatherer creates a batch processor for gathering requests
func NewBatchGatherer(gatherer *PromptGatherer, batchSize int, flushInterval time.Duration) *BatchGatherer {
	bg := &BatchGatherer{
		gatherer: gatherer,
	}

	config := concurrency.BatchProcessorConfig[*BatchRequest]{
		MaxBatchSize:  batchSize,
		FlushInterval: flushInterval,
		Workers:       4, // Process up to 4 batches concurrently
	}

	bg.processor = concurrency.NewBatchProcessor(config, bg.processBatch)

	return bg
}

// processBatch handles a batch of gathering requests
func (bg *BatchGatherer) processBatch(items []*BatchRequest) error {
	// Process each request in the batch concurrently
	var wg g.WaitGroup

	for _, req := range items {
		wg.Add()
		go func(r *BatchRequest) {
			defer wg.Done()

			output, err := bg.gatherer.GatherWithTemplate(r.Context, r.Template)

			r.Result <- BatchResponse{
				ID:     r.ID,
				Output: output,
				Error:  err,
			}
			close(r.Result)
		}(req)
	}

	wg.Wait()
	return nil
}

// Submit submits a gathering request to the batch
func (bg *BatchGatherer) Submit(ctx context.Context, id, template string) <-chan BatchResponse {
	resultChan := make(chan BatchResponse, 1)

	req := &BatchRequest{
		ID:       id,
		Template: template,
		Context:  ctx,
		Result:   resultChan,
	}

	bg.processor.Add(req)

	return resultChan
}

// Flush forces processing of pending requests
func (bg *BatchGatherer) Flush() error {
	return bg.processor.Flush()
}

// Close shuts down the batch gatherer
func (bg *BatchGatherer) Close() error {
	return bg.processor.Close()
}

// StreamingGatherer provides streaming results as data becomes available
// Optimizes TTFT by returning partial results immediately
type StreamingGatherer struct {
	gatherer *PromptGatherer
}

// NewStreamingGatherer creates a streaming gatherer
func NewStreamingGatherer(gatherer *PromptGatherer) *StreamingGatherer {
	return &StreamingGatherer{
		gatherer: gatherer,
	}
}

// PartialResult contains data from a single source
type PartialResult struct {
	Source   string
	Data     map[string]interface{}
	Duration time.Duration
	Error    error
}

// GatherStream returns a channel that streams results as they become available
// This minimizes TTFT by allowing consumers to process data as soon as the first source responds
func (sg *StreamingGatherer) GatherStream(ctx context.Context) (<-chan PartialResult, error) {
	sg.gatherer.mu.RLock()
	sources := make([]DataSource, len(sg.gatherer.sources))
	copy(sources, sg.gatherer.sources)
	sg.gatherer.mu.RUnlock()

	if len(sources) == 0 {
		ch := make(chan PartialResult)
		close(ch)
		return ch, nil
	}

	// Create context with timeout
	gatherCtx := ctx
	if sg.gatherer.config.Timeout > 0 {
		var cancel context.CancelFunc
		gatherCtx, cancel = context.WithTimeout(ctx, sg.gatherer.config.Timeout)
		_ = cancel // Will be called when sources complete
	}

	// Sort sources by priority
	g.SortBy(sources, func(s DataSource) int { return s.Priority() })

	// Create result channel
	resultChan := make(chan PartialResult, len(sources))

	// Launch concurrent fetchers
	go func() {
		defer close(resultChan)

		var wg g.WaitGroup
		sem := make(chan struct{}, sg.gatherer.config.MaxConcurrency)

		for _, source := range sources {
			wg.Add()
			go func(src DataSource) {
				defer wg.Done()

				// Acquire semaphore
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-gatherCtx.Done():
					resultChan <- PartialResult{
						Source: src.Name(),
						Error:  gatherCtx.Err(),
					}
					return
				}

				// Fetch from source
				startTime := time.Now()
				data, err := src.Fetch(gatherCtx)
				duration := time.Since(startTime)

				resultChan <- PartialResult{
					Source:   src.Name(),
					Data:     data,
					Duration: duration,
					Error:    err,
				}
			}(source)
		}

		wg.Wait()
	}()

	return resultChan, nil
}

// RateLimitedHTTPSource wraps HTTPSource with rate limiting
// Prevents API throttling by limiting request rate
type RateLimitedHTTPSource struct {
	*HTTPSource
	limiter *concurrency.RateLimiter
}

// NewRateLimitedHTTPSource creates an HTTP source with rate limiting
// Example: ratePerSecond=10, burst=5 allows 10 requests/sec with burst of 5
func NewRateLimitedHTTPSource(name, url string, priority int, ratePerSecond float64, burst int) *RateLimitedHTTPSource {
	return &RateLimitedHTTPSource{
		HTTPSource: NewHTTPSource(name, url, priority),
		limiter:    concurrency.NewRateLimiter(ratePerSecond, burst),
	}
}

// Fetch retrieves data with rate limiting applied
func (r *RateLimitedHTTPSource) Fetch(ctx context.Context) (map[string]interface{}, error) {
	// Wait for rate limiter
	if err := r.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limit wait failed: %w", err)
	}

	// Proceed with normal fetch
	return r.HTTPSource.Fetch(ctx)
}

// RedisBackedGatherer uses Redis for distributed caching
// Enables sharing cached results across multiple instances
type RedisBackedGatherer struct {
	*PromptGatherer
	redisCache *cacheredis.Cache
}

// NewRedisBackedGatherer creates a gatherer with Redis caching
func NewRedisBackedGatherer(config GatherConfig, redisURL string) (*RedisBackedGatherer, error) {
	// Create Redis cache
	redisCache, err := cacheredis.NewCache(cacheredis.CacheConfig{
		RedisURL: redisURL,
		Prefix:   "promptgather:",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis cache: %w", err)
	}

	// Use Redis cache in config
	config.Cache = redisCache

	gatherer := NewPromptGatherer(config)

	return &RedisBackedGatherer{
		PromptGatherer: gatherer,
		redisCache:     redisCache,
	}, nil
}

// PublishResult publishes a gathering result to Redis pub/sub
func (r *RedisBackedGatherer) PublishResult(channel string, result *Result) error {
	data, err := g.Marshal(result)
	if err != nil {
		return err
	}

	return r.redisCache.Publish(channel, string(data))
}

// SubscribeResults subscribes to gathering results from Redis pub/sub
func (r *RedisBackedGatherer) SubscribeResults(channel string) (<-chan string, error) {
	return r.redisCache.Subscribe(channel)
}

// Close closes the Redis connection
func (r *RedisBackedGatherer) Close() error {
	return r.redisCache.Close()
}

// MultiTierGatherer uses multi-tier caching for optimal performance
// L1: Fast in-memory cache, L2: Larger Redis cache
type MultiTierGatherer struct {
	*PromptGatherer
	l1Cache cache.Cache
	l2Cache cache.Cache
}

// NewMultiTierGatherer creates a gatherer with multi-tier caching
func NewMultiTierGatherer(config GatherConfig, l1Size, l2Size int) *MultiTierGatherer {
	// Create L1 cache (small, fast)
	l1Config := cache.MemoryCacheConfig{
		MaxSize:       l1Size,
		EvictionType:  cache.EvictionTypeLRU,
		DefaultTTL:    config.CacheTTL,
		EnableMetrics: true,
	}
	l1Cache := cache.NewMemoryCache(l1Config)

	// Create L2 cache (larger)
	l2Config := cache.MemoryCacheConfig{
		MaxSize:       l2Size,
		EvictionType:  cache.EvictionTypeLRU,
		DefaultTTL:    config.CacheTTL,
		EnableMetrics: true,
	}
	l2Cache := cache.NewMemoryCache(l2Config)

	// Create multi-tier cache
	multiCache := cache.NewMultiTierCache(l1Cache, l2Cache)

	config.Cache = multiCache

	gatherer := NewPromptGatherer(config)

	return &MultiTierGatherer{
		PromptGatherer: gatherer,
		l1Cache:        l1Cache,
		l2Cache:        l2Cache,
	}
}

// GetCacheMetrics returns metrics for both cache tiers
func (m *MultiTierGatherer) GetCacheMetrics() map[string]interface{} {
	return map[string]interface{}{
		"l1_metrics": m.l1Cache.GetMetrics(),
		"l2_metrics": m.l2Cache.GetMetrics(),
	}
}

// PipelineGatherer uses pipeline pattern for complex transformations
// Useful for multi-stage data processing before prompt injection
type PipelineGatherer struct {
	gatherer *PromptGatherer
	pipeline *concurrency.Pipeline[map[string]interface{}]
}

// TransformStage represents a transformation stage in the pipeline
type TransformStage struct {
	Name    string
	Workers int
	Process func(context.Context, map[string]interface{}) (map[string]interface{}, error)
}

// NewPipelineGatherer creates a gatherer with transformation pipeline
func NewPipelineGatherer(gatherer *PromptGatherer, stages ...TransformStage) *PipelineGatherer {
	// Convert to concurrency.Stage
	concStages := make([]concurrency.Stage[map[string]interface{}], len(stages))
	for i, stage := range stages {
		concStages[i] = concurrency.Stage[map[string]interface{}]{
			Name:    stage.Name,
			Workers: stage.Workers,
			Process: stage.Process,
		}
	}

	pipeline := concurrency.NewPipeline(concStages...)

	return &PipelineGatherer{
		gatherer: gatherer,
		pipeline: pipeline,
	}
}

// GatherAndTransform gathers data and runs it through the transformation pipeline
func (pg *PipelineGatherer) GatherAndTransform(ctx context.Context) ([]map[string]interface{}, error) {
	// Gather data
	result, err := pg.gatherer.Gather(ctx)
	if err != nil {
		return nil, err
	}

	// Create input channel
	inputChan := make(chan map[string]interface{}, 1)
	inputChan <- result.Data
	close(inputChan)

	// Execute pipeline
	outputChan, errorChan := pg.pipeline.Execute(inputChan)

	// Collect results
	var results []map[string]interface{}
	var errors []error

	for output := range outputChan {
		results = append(results, output)
	}

	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return results, fmt.Errorf("pipeline errors: %v", errors)
	}

	return results, nil
}

// WarmCache pre-loads data into the cache for faster first access
type CacheWarmer struct {
	gatherer *PromptGatherer
}

// NewCacheWarmer creates a cache warming utility
func NewCacheWarmer(gatherer *PromptGatherer) *CacheWarmer {
	return &CacheWarmer{
		gatherer: gatherer,
	}
}

// WarmAll fetches all data sources and populates the cache
func (cw *CacheWarmer) WarmAll(ctx context.Context) error {
	_, err := cw.gatherer.Gather(ctx)
	return err
}

// WarmPeriodically warms the cache at regular intervals
func (cw *CacheWarmer) WarmPeriodically(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial warm
	_ = cw.WarmAll(ctx)

	for {
		select {
		case <-ticker.C:
			_ = cw.WarmAll(ctx)
		case <-ctx.Done():
			return
		}
	}
}
