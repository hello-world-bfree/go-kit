package promptgather_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/flarco/g"
	"github.com/flarco/g/cache"
	"github.com/flarco/g/promptgather"
)

// Example_basic demonstrates basic usage of the prompt gatherer
func Example_basic() {
	// Create gatherer with default config optimized for TTFT
	config := promptgather.DefaultConfig()
	config.Timeout = 2 * time.Second

	gatherer := promptgather.NewPromptGatherer(config)

	// Add a custom function data source
	gatherer.AddSource(promptgather.NewFuncSource("user_context", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"user_id":   "12345",
			"user_name": "Alice",
			"role":      "admin",
		}, nil
	}))

	// Add another data source with lower priority
	gatherer.AddSource(promptgather.NewFuncSource("app_config", 1, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"app_version": "1.0.0",
			"environment": "production",
		}, nil
	}))

	// Gather all data concurrently
	ctx := context.Background()
	result, err := gatherer.Gather(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Gathered data in %v\n", result.Metrics.TotalDuration)
	fmt.Printf("Successful sources: %d\n", result.Metrics.SuccessfulSources)
	fmt.Printf("User name: %v\n", result.Data["user_context.user_name"])
}

// Example_withTemplate demonstrates using templates for prompt injection
func Example_withTemplate() {
	config := promptgather.DefaultConfig()
	gatherer := promptgather.NewPromptGatherer(config)

	// Add data sources
	gatherer.AddSource(promptgather.NewFuncSource("user", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"name":  "Alice",
			"email": "[email protected]",
		}, nil
	}))

	// Template for prompt injection
	template := `
You are helping user {{.user.name}} ({{.user.email}}).

Please assist them with their query.
`

	ctx := context.Background()
	prompt, err := gatherer.GatherWithTemplate(ctx, template)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(prompt)
}

// Example_httpAPI demonstrates fetching from HTTP APIs
func Example_httpAPI() {
	config := promptgather.DefaultConfig()
	gatherer := promptgather.NewPromptGatherer(config)

	// Add HTTP API source
	apiSource := promptgather.NewHTTPSource(
		"weather",
		"https://api.weather.example.com/current",
		0,
	).WithHeaders(map[string]string{
		"Authorization": "Bearer TOKEN",
	}).WithTimeout(5 * time.Second)

	gatherer.AddSource(apiSource)

	// Add rate-limited API source
	rateLimitedAPI := promptgather.NewRateLimitedHTTPSource(
		"stock_prices",
		"https://api.stocks.example.com/quotes",
		1,
		10.0, // 10 requests per second
		5,    // burst of 5
	)
	gatherer.AddSource(rateLimitedAPI)

	ctx := context.Background()
	result, err := gatherer.Gather(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Gathered from %d APIs in %v\n",
		result.Metrics.SuccessfulSources,
		result.Metrics.TotalDuration)
}

// Example_database demonstrates database queries
func Example_database() {
	// Open database connection
	db, err := sql.Open("postgres", "postgresql://user:pass@localhost/dbname")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	config := promptgather.DefaultConfig()
	gatherer := promptgather.NewPromptGatherer(config)

	// Add database query source
	userQuery := promptgather.NewDBSource(
		"recent_users",
		db,
		"SELECT id, name, email FROM users WHERE created_at > $1 ORDER BY created_at DESC LIMIT 10",
		time.Now().AddDate(0, 0, -7), // Last 7 days
	).WithPriority(0)

	gatherer.AddSource(userQuery)

	// Add aggregation query
	statsQuery := promptgather.NewDBSource(
		"user_stats",
		db,
		"SELECT COUNT(*) as total_users, COUNT(DISTINCT country) as countries FROM users",
	).WithPriority(1)

	gatherer.AddSource(statsQuery)

	ctx := context.Background()
	result, err := gatherer.Gather(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Access the data
	stats := result.Data["user_stats"]
	fmt.Printf("Database stats: %v\n", stats)
}

// Example_duckdb demonstrates DuckDB analytical queries
func Example_duckdb() {
	// Create DuckDB helper
	duckdb, err := promptgather.NewDuckDBHelper(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	defer duckdb.Close()

	// Load data from CSV
	ctx := context.Background()
	err = duckdb.CreateTableFromCSV(ctx, "sales", "/path/to/sales.csv")
	if err != nil {
		log.Fatal(err)
	}

	config := promptgather.DefaultConfig()
	gatherer := promptgather.NewPromptGatherer(config)

	// Add analytical queries
	gatherer.AddSources(
		// Top products by revenue
		duckdb.TopN("top_products", "sales", "revenue", 10),

		// Sales by category
		duckdb.GroupByCount("category_sales", "sales", "category", 20),

		// Time series aggregation
		duckdb.TimeSeriesAggregation(
			"daily_sales",
			"sales",
			"sale_date",
			"revenue",
			"day",
		),

		// Custom analytical query
		promptgather.NewDuckDBSource(
			"sales_summary",
			duckdb.GetDB(),
			`SELECT
				COUNT(*) as total_sales,
				SUM(revenue) as total_revenue,
				AVG(revenue) as avg_sale,
				MAX(revenue) as max_sale
			FROM sales
			WHERE sale_date >= CURRENT_DATE - INTERVAL '30 days'`,
		),
	)

	result, err := gatherer.Gather(ctx)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Gathered analytics in %v\n", result.Metrics.TotalDuration)
}

// Example_caching demonstrates caching for faster responses
func Example_caching() {
	// Create memory cache
	cacheConfig := cache.MemoryCacheConfig{
		MaxSize:       100,
		EvictionType:  cache.EvictionTypeLRU,
		DefaultTTL:    5 * time.Minute,
		EnableMetrics: true,
	}
	memCache := cache.NewMemoryCache(cacheConfig)

	config := promptgather.DefaultConfig()
	config.Cache = memCache
	config.CacheTTL = 5 * time.Minute

	gatherer := promptgather.NewPromptGatherer(config)

	// Add slow data source
	gatherer.AddSource(promptgather.NewFuncSource("slow_api", 0, func(ctx context.Context) (map[string]interface{}, error) {
		time.Sleep(500 * time.Millisecond) // Simulate slow API
		return map[string]interface{}{
			"result": "expensive computation",
		}, nil
	}))

	ctx := context.Background()

	// First call - will be slow
	start := time.Now()
	result1, _ := gatherer.Gather(ctx)
	fmt.Printf("First call: %v\n", time.Since(start))

	// Second call - will be fast (cached)
	start = time.Now()
	result2, _ := gatherer.Gather(ctx)
	fmt.Printf("Second call (cached): %v\n", time.Since(start))

	// Check cache metrics
	metrics := memCache.GetMetrics()
	fmt.Printf("Cache hit rate: %.2f%%\n", metrics.HitRate*100)

	_, _ = result1, result2
}

// Example_streaming demonstrates streaming results for minimum TTFT
func Example_streaming() {
	config := promptgather.DefaultConfig()
	gatherer := promptgather.NewPromptGatherer(config)

	// Add multiple sources with different response times
	gatherer.AddSource(promptgather.NewFuncSource("fast", 0, func(ctx context.Context) (map[string]interface{}, error) {
		time.Sleep(100 * time.Millisecond)
		return map[string]interface{}{"data": "fast response"}, nil
	}))

	gatherer.AddSource(promptgather.NewFuncSource("slow", 1, func(ctx context.Context) (map[string]interface{}, error) {
		time.Sleep(1 * time.Second)
		return map[string]interface{}{"data": "slow response"}, nil
	}))

	// Use streaming gatherer for minimum TTFT
	streaming := promptgather.NewStreamingGatherer(gatherer)

	ctx := context.Background()
	resultChan, err := streaming.GatherStream(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Process results as they arrive
	for partial := range resultChan {
		if partial.Error != nil {
			fmt.Printf("Source %s failed: %v\n", partial.Source, partial.Error)
			continue
		}
		fmt.Printf("Source %s completed in %v: %v\n",
			partial.Source, partial.Duration, partial.Data)
		// Can start processing/using data immediately!
	}
}

// Example_batch demonstrates batch processing for multiple requests
func Example_batch() {
	config := promptgather.DefaultConfig()
	gatherer := promptgather.NewPromptGatherer(config)

	gatherer.AddSource(promptgather.NewFuncSource("context", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"timestamp": time.Now().Unix(),
		}, nil
	}))

	// Create batch gatherer
	batchGatherer := promptgather.NewBatchGatherer(
		gatherer,
		10,               // batch size
		100*time.Millisecond, // flush interval
	)
	defer batchGatherer.Close()

	// Submit multiple requests
	ctx := context.Background()
	requests := []struct {
		id       string
		template string
	}{
		{"req1", "Context: {{.context.timestamp}}"},
		{"req2", "Time: {{.context.timestamp}}"},
		{"req3", "Timestamp: {{.context.timestamp}}"},
	}

	// Submit all requests
	var channels []<-chan promptgather.BatchResponse
	for _, req := range requests {
		ch := batchGatherer.Submit(ctx, req.id, req.template)
		channels = append(channels, ch)
	}

	// Wait for results
	for i, ch := range channels {
		response := <-ch
		if response.Error != nil {
			fmt.Printf("Request %s failed: %v\n", response.ID, response.Error)
		} else {
			fmt.Printf("Request %s completed: %s\n", requests[i].id, response.Output)
		}
	}
}

// Example_multiTier demonstrates multi-tier caching
func Example_multiTier() {
	config := promptgather.DefaultConfig()

	// Create multi-tier gatherer
	// L1: 100 items (fast, small)
	// L2: 1000 items (larger)
	gatherer := promptgather.NewMultiTierGatherer(config, 100, 1000)

	gatherer.AddSource(promptgather.NewFuncSource("data", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"value": g.RandString(g.AlphaRunesLower, 10),
		}, nil
	}))

	ctx := context.Background()

	// Make multiple requests
	for i := 0; i < 5; i++ {
		result, _ := gatherer.Gather(ctx)
		fmt.Printf("Request %d: %v\n", i+1, result.Data["data.value"])
	}

	// Check cache metrics
	metrics := gatherer.GetCacheMetrics()
	fmt.Printf("Cache metrics: %v\n", metrics)
}

// Example_complete demonstrates a complete real-world scenario
func Example_complete() {
	// Setup
	ctx := context.Background()

	// Create cache
	cacheConfig := cache.MemoryCacheConfig{
		MaxSize:       1000,
		EvictionType:  cache.EvictionTypeLRU,
		DefaultTTL:    10 * time.Minute,
		EnableMetrics: true,
	}
	memCache := cache.NewMemoryCache(cacheConfig)

	// Create gatherer
	config := promptgather.GatherConfig{
		MaxConcurrency:  8,
		Timeout:         3 * time.Second,
		ContinueOnError: true,
		Cache:           memCache,
		CacheTTL:        5 * time.Minute,
		EnableMetrics:   true,
	}

	gatherer := promptgather.NewPromptGatherer(config)

	// Add user context
	gatherer.AddSource(promptgather.NewFuncSource("user", 0, func(ctx context.Context) (map[string]interface{}, error) {
		return map[string]interface{}{
			"id":    "user_123",
			"name":  "Alice Johnson",
			"email": "[email protected]",
			"plan":  "premium",
		}, nil
	}))

	// Add recent activity (from cache)
	gatherer.AddSource(promptgather.NewCacheSource("recent_activity", memCache, "user:user_123:activity"))

	// Add system stats (custom function)
	gatherer.AddSource(promptgather.NewFuncSource("system", 2, func(ctx context.Context) (map[string]interface{}, error) {
		stats := g.GetMachineProcStats()
		return map[string]interface{}{
			"cpu_percent":    stats["cpu_perc"],
			"memory_percent": stats["ram_perc"],
		}, nil
	}))

	// Define prompt template
	template := `You are an AI assistant helping {{.user.name}} ({{.user.email}}).

User Context:
- User ID: {{.user.id}}
- Plan: {{.user.plan}}
- Email: {{.user.email}}

System Status:
- CPU: {{.system.cpu_percent}}%
- Memory: {{.system.memory_percent}}%

Please provide helpful, accurate, and personalized assistance.
`

	// Gather and inject into template
	prompt, err := gatherer.GatherWithTemplate(ctx, template)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Generated prompt:")
	fmt.Println(prompt)

	// Get metrics
	result, _ := gatherer.Gather(ctx)
	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Total duration: %v\n", result.Metrics.TotalDuration)
	fmt.Printf("Successful sources: %d\n", result.Metrics.SuccessfulSources)
	fmt.Printf("Failed sources: %d\n", result.Metrics.FailedSources)
	for source, duration := range result.Metrics.SourceDurations {
		fmt.Printf("  %s: %v\n", source, duration)
	}
}
