package promptgather

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/cache"
)

// DataSource represents a source of information for prompt augmentation
type DataSource interface {
	// Fetch retrieves data from the source. Returns key-value pairs.
	Fetch(ctx context.Context) (map[string]interface{}, error)
	// Name returns a unique identifier for this data source
	Name() string
	// Priority returns the priority (lower = higher priority, 0 = highest)
	// Higher priority sources are fetched first in case of timeout
	Priority() int
}

// GatherConfig configures the information gatherer
type GatherConfig struct {
	// MaxConcurrency limits how many data sources to fetch concurrently
	MaxConcurrency int
	// Timeout is the maximum time to wait for all sources
	Timeout time.Duration
	// ContinueOnError determines if we continue gathering if a source fails
	ContinueOnError bool
	// Cache enables caching of gathered data
	Cache cache.Cache
	// CacheTTL is the time-to-live for cached results
	CacheTTL time.Duration
	// EnableMetrics enables performance metrics collection
	EnableMetrics bool
}

// DefaultConfig returns sensible defaults for TTFT optimization
func DefaultConfig() GatherConfig {
	return GatherConfig{
		MaxConcurrency:  10, // Fetch up to 10 sources concurrently
		Timeout:         2 * time.Second,
		ContinueOnError: true,
		CacheTTL:        5 * time.Minute,
		EnableMetrics:   true,
	}
}

// GatherMetrics tracks performance metrics
type GatherMetrics struct {
	TotalDuration     time.Duration
	SourceDurations   map[string]time.Duration
	CacheHits         int
	CacheMisses       int
	SuccessfulSources int
	FailedSources     int
	Errors            []error
}

// PromptGatherer orchestrates concurrent data gathering from multiple sources
type PromptGatherer struct {
	config  GatherConfig
	sources []DataSource
	mu      sync.RWMutex
}

// NewPromptGatherer creates a new prompt information gatherer
func NewPromptGatherer(config GatherConfig) *PromptGatherer {
	return &PromptGatherer{
		config:  config,
		sources: make([]DataSource, 0),
	}
}

// AddSource registers a data source with the gatherer
func (pg *PromptGatherer) AddSource(source DataSource) {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	pg.sources = append(pg.sources, source)
}

// AddSources registers multiple data sources
func (pg *PromptGatherer) AddSources(sources ...DataSource) {
	for _, source := range sources {
		pg.AddSource(source)
	}
}

// Result represents the gathered data and metrics
type Result struct {
	Data    map[string]interface{}
	Metrics GatherMetrics
}

// Gather fetches data from all registered sources concurrently
// Optimized for shortest time to first token by:
// 1. Concurrent fetching with worker pool
// 2. Priority-based execution
// 3. Timeout handling
// 4. Cache layer for repeated queries
func (pg *PromptGatherer) Gather(ctx context.Context) (*Result, error) {
	startTime := time.Now()

	pg.mu.RLock()
	sources := make([]DataSource, len(pg.sources))
	copy(sources, pg.sources)
	pg.mu.RUnlock()

	if len(sources) == 0 {
		return &Result{Data: make(map[string]interface{})}, nil
	}

	// Create context with timeout
	gatherCtx := ctx
	var cancel context.CancelFunc
	if pg.config.Timeout > 0 {
		gatherCtx, cancel = context.WithTimeout(ctx, pg.config.Timeout)
		defer cancel()
	}

	// Sort sources by priority (lower number = higher priority)
	g.SortBy(sources, func(s DataSource) int { return s.Priority() })

	// Prepare result collection
	result := &Result{
		Data: make(map[string]interface{}),
		Metrics: GatherMetrics{
			SourceDurations: make(map[string]time.Duration),
			Errors:          make([]error, 0),
		},
	}

	// Use fan-out pattern for concurrent fetching
	type fetchResult struct {
		source   string
		data     map[string]interface{}
		duration time.Duration
		err      error
	}

	resultChan := make(chan fetchResult, len(sources))
	var wg sync.WaitGroup

	// Limit concurrency using semaphore pattern
	sem := make(chan struct{}, pg.config.MaxConcurrency)

	// Launch concurrent fetchers
	for _, source := range sources {
		wg.Add(1)
		go func(src DataSource) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-gatherCtx.Done():
				resultChan <- fetchResult{
					source: src.Name(),
					err:    gatherCtx.Err(),
				}
				return
			}

			// Check cache first
			var data map[string]interface{}
			var err error
			cacheKey := fmt.Sprintf("promptgather:%s", src.Name())

			fetchStart := time.Now()

			if pg.config.Cache != nil {
				if cached, found := pg.config.Cache.Get(cacheKey); found {
					if cachedData, ok := cached.(map[string]interface{}); ok {
						data = cachedData
						resultChan <- fetchResult{
							source:   src.Name(),
							data:     data,
							duration: time.Since(fetchStart),
							err:      nil,
						}
						return
					}
				}
			}

			// Fetch from source
			data, err = src.Fetch(gatherCtx)
			duration := time.Since(fetchStart)

			// Cache successful results
			if err == nil && pg.config.Cache != nil && data != nil {
				pg.config.Cache.SetWithTTL(cacheKey, data, pg.config.CacheTTL)
			}

			resultChan <- fetchResult{
				source:   src.Name(),
				data:     data,
				duration: duration,
				err:      err,
			}
		}(source)
	}

	// Close result channel when all fetches complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for fr := range resultChan {
		result.Metrics.SourceDurations[fr.source] = fr.duration

		if fr.err != nil {
			result.Metrics.FailedSources++
			result.Metrics.Errors = append(result.Metrics.Errors,
				fmt.Errorf("source %s: %w", fr.source, fr.err))

			if !pg.config.ContinueOnError {
				result.Metrics.TotalDuration = time.Since(startTime)
				return result, fmt.Errorf("source %s failed: %w", fr.source, fr.err)
			}
			continue
		}

		result.Metrics.SuccessfulSources++

		// Merge data into result
		for key, value := range fr.data {
			// Prefix with source name to avoid conflicts
			prefixedKey := fmt.Sprintf("%s.%s", fr.source, key)
			result.Data[prefixedKey] = value
		}
	}

	result.Metrics.TotalDuration = time.Since(startTime)

	if result.Metrics.FailedSources > 0 && !pg.config.ContinueOnError {
		return result, fmt.Errorf("some sources failed: %v", result.Metrics.Errors)
	}

	return result, nil
}

// GatherWithTemplate fetches data and injects it into a template
// Template uses Go template syntax with all gathered data available
func (pg *PromptGatherer) GatherWithTemplate(ctx context.Context, template string) (string, error) {
	result, err := pg.Gather(ctx)
	if err != nil {
		return "", err
	}

	// Execute template with gathered data
	output, err := g.ExecuteTemplate(template, result.Data)
	if err != nil {
		return "", fmt.Errorf("template execution failed: %w", err)
	}

	return output, nil
}

// ClearCache clears all cached data for this gatherer
func (pg *PromptGatherer) ClearCache() {
	if pg.config.Cache == nil {
		return
	}

	pg.mu.RLock()
	defer pg.mu.RUnlock()

	for _, source := range pg.sources {
		cacheKey := fmt.Sprintf("promptgather:%s", source.Name())
		pg.config.Cache.Delete(cacheKey)
	}
}

// BaseDataSource provides common functionality for data sources
type BaseDataSource struct {
	name     string
	priority int
}

// NewBaseDataSource creates a base data source
func NewBaseDataSource(name string, priority int) BaseDataSource {
	return BaseDataSource{
		name:     name,
		priority: priority,
	}
}

// Name returns the data source name
func (b BaseDataSource) Name() string {
	return b.name
}

// Priority returns the data source priority
func (b BaseDataSource) Priority() int {
	return b.priority
}

// HTTPSource fetches data from HTTP APIs
type HTTPSource struct {
	BaseDataSource
	url     string
	headers map[string]string
	timeout time.Duration
	parser  func([]byte) (map[string]interface{}, error)
}

// NewHTTPSource creates an HTTP API data source
func NewHTTPSource(name, url string, priority int) *HTTPSource {
	return &HTTPSource{
		BaseDataSource: NewBaseDataSource(name, priority),
		url:            url,
		headers:        make(map[string]string),
		timeout:        10 * time.Second,
		parser:         defaultJSONParser,
	}
}

// WithHeaders sets custom headers for the HTTP request
func (h *HTTPSource) WithHeaders(headers map[string]string) *HTTPSource {
	h.headers = headers
	return h
}

// WithTimeout sets the HTTP request timeout
func (h *HTTPSource) WithTimeout(timeout time.Duration) *HTTPSource {
	h.timeout = timeout
	return h
}

// WithParser sets a custom response parser
func (h *HTTPSource) WithParser(parser func([]byte) (map[string]interface{}, error)) *HTTPSource {
	h.parser = parser
	return h
}

// Fetch retrieves data from the HTTP endpoint
func (h *HTTPSource) Fetch(ctx context.Context) (map[string]interface{}, error) {
	// Use g.ClientDo for HTTP request
	resp, respBytes, err := g.ClientDo("GET", h.url, nil, h.headers, int(h.timeout.Seconds()))
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// Parse response
	data, err := h.parser(respBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return data, nil
}

// defaultJSONParser parses JSON responses
func defaultJSONParser(data []byte) (map[string]interface{}, error) {
	var result map[string]interface{}
	if err := g.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// DBSource fetches data from database queries
type DBSource struct {
	BaseDataSource
	db    *sql.DB
	query string
	args  []interface{}
}

// NewDBSource creates a database query data source
func NewDBSource(name string, db *sql.DB, query string, args ...interface{}) *DBSource {
	return &DBSource{
		BaseDataSource: NewBaseDataSource(name, 0),
		db:             db,
		query:          query,
		args:           args,
	}
}

// WithPriority sets the priority for this source
func (d *DBSource) WithPriority(priority int) *DBSource {
	d.priority = priority
	return d
}

// Fetch executes the database query and returns results
func (d *DBSource) Fetch(ctx context.Context) (map[string]interface{}, error) {
	rows, err := d.db.QueryContext(ctx, d.query, d.args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Prepare result
	result := make(map[string]interface{})
	resultRows := make([]map[string]interface{}, 0)

	// Scan rows
	for rows.Next() {
		// Create a slice of interface{} to hold each column value
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create row map
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			rowMap[col] = values[i]
		}
		resultRows = append(resultRows, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// Store results
	result["rows"] = resultRows
	result["count"] = len(resultRows)

	// If there's only one row, also flatten the first row into top-level keys
	if len(resultRows) == 1 {
		for key, value := range resultRows[0] {
			result[key] = value
		}
	}

	return result, nil
}

// CacheSource fetches data from a cache
type CacheSource struct {
	BaseDataSource
	cache cache.Cache
	key   string
}

// NewCacheSource creates a cache data source
func NewCacheSource(name string, cache cache.Cache, key string) *CacheSource {
	return &CacheSource{
		BaseDataSource: NewBaseDataSource(name, 0),
		cache:          cache,
		key:            key,
	}
}

// WithPriority sets the priority for this source
func (c *CacheSource) WithPriority(priority int) *CacheSource {
	c.priority = priority
	return c
}

// Fetch retrieves data from the cache
func (c *CacheSource) Fetch(ctx context.Context) (map[string]interface{}, error) {
	value, found := c.cache.Get(c.key)
	if !found {
		return nil, fmt.Errorf("key not found in cache: %s", c.key)
	}

	// If value is already a map, return it
	if mapValue, ok := value.(map[string]interface{}); ok {
		return mapValue, nil
	}

	// Otherwise, wrap it
	return map[string]interface{}{
		"value": value,
	}, nil
}

// FuncSource executes a custom function to fetch data
type FuncSource struct {
	BaseDataSource
	fetchFunc func(context.Context) (map[string]interface{}, error)
}

// NewFuncSource creates a custom function-based data source
func NewFuncSource(name string, priority int, fetchFunc func(context.Context) (map[string]interface{}, error)) *FuncSource {
	return &FuncSource{
		BaseDataSource: NewBaseDataSource(name, priority),
		fetchFunc:      fetchFunc,
	}
}

// Fetch executes the custom function
func (f *FuncSource) Fetch(ctx context.Context) (map[string]interface{}, error) {
	return f.fetchFunc(ctx)
}
