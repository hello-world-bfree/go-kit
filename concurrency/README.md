# Concurrency Package

The `concurrency` package provides utilities for managing concurrent operations, especially for IO-bound workloads.

## Features

### Worker Pool

Manages a pool of workers for processing IO-bound tasks with configurable concurrency limits.

```go
import "github.com/flarco/g/concurrency"

// Create a worker pool with 10 workers and queue size of 100
pool := concurrency.NewWorkerPool(10, 100)
defer pool.Stop()

// Submit tasks
err := pool.SubmitFunc("task-1", func(ctx context.Context) error {
    // Perform IO-bound work
    return nil
})

// Get metrics
metrics := pool.GetMetrics()
fmt.Printf("Completed: %d, Failed: %d\n", metrics.TasksCompleted, metrics.TasksFailed)
```

### Semaphore

Controls concurrent access to resources using a weighted semaphore.

```go
// Create a semaphore with 5 permits
sem := concurrency.NewSemaphore(5)

// Acquire and release
sem.Acquire()
defer sem.Release()

// Or use helper
err := sem.WithSemaphore(func() error {
    // Critical section
    return nil
})

// With context support
err := sem.AcquireWithContext(ctx)
```

### Rate Limiter

Controls operation rate using token bucket algorithm.

```go
// Create rate limiter: 100 ops/sec, burst of 10
limiter := concurrency.NewRateLimiter(100, 10)

// Check if operation is allowed
if limiter.Allow() {
    // Proceed with operation
}

// Wait for permission
err := limiter.Wait(ctx)

// Adaptive rate limiter
adaptive := concurrency.NewAdaptiveRateLimiter(100, 10, 1000, 50)
adaptive.RecordSuccess() // Increase rate
adaptive.RecordFailure() // Decrease rate
```

### Batch Processor

Collects items and processes them in batches for efficient bulk operations.

```go
config := concurrency.BatchProcessorConfig{
    MaxBatchSize:  100,
    FlushInterval: 5 * time.Second,
    BufferSize:    1000,
}

processor := concurrency.NewBatchProcessor(config, func(items []string) error {
    // Process batch
    return nil
})

// Add items
processor.Add("item1")
processor.Add("item2")

// Manual flush
processor.Flush()
```

### Pipeline

Implements concurrent pipeline pattern for multi-stage processing.

```go
stage1 := concurrency.Stage[Data]{
    Name:    "parse",
    Workers: 5,
    Process: func(ctx context.Context, d Data) (Data, error) {
        // Parse data
        return d, nil
    },
}

stage2 := concurrency.Stage[Data]{
    Name:    "validate",
    Workers: 3,
    Process: func(ctx context.Context, d Data) (Data, error) {
        // Validate data
        return d, nil
    },
}

pipeline := concurrency.NewPipeline(stage1, stage2)
output, errors := pipeline.Execute(inputChan)
```

### FanOut/FanIn

Utilities for distributing and merging concurrent operations.

```go
// Fan out to multiple workers
errors := concurrency.FanOut(ctx, input, 10, func(ctx context.Context, item Item) error {
    // Process item
    return nil
})

// Fan in from multiple channels
merged := concurrency.FanIn(ctx, chan1, chan2, chan3)
```

## Use Cases

- **IO-bound operations**: Database queries, API calls, file operations
- **Batch processing**: Aggregating writes, bulk API requests
- **Rate limiting**: API throttling, resource protection
- **Pipeline processing**: ETL workflows, data transformation
- **Concurrent workflows**: Parallel task execution with proper resource management
