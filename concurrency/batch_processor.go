package concurrency

import (
	"context"
	"sync"
	"time"
)

// BatchProcessor collects items and processes them in batches
type BatchProcessor[T any] struct {
	maxBatchSize int
	flushInterval time.Duration
	processor    func([]T) error

	mu       sync.Mutex
	buffer   []T
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	errorCh  chan error
	itemCh   chan T
}

// BatchProcessorConfig configures the batch processor
type BatchProcessorConfig struct {
	MaxBatchSize  int           // Maximum items per batch
	FlushInterval time.Duration // Maximum time to wait before flushing
	BufferSize    int           // Channel buffer size
}

// NewBatchProcessor creates a new batch processor
func NewBatchProcessor[T any](config BatchProcessorConfig, processor func([]T) error) *BatchProcessor[T] {
	ctx, cancel := context.WithCancel(context.Background())

	bp := &BatchProcessor[T]{
		maxBatchSize:  config.MaxBatchSize,
		flushInterval: config.FlushInterval,
		processor:     processor,
		buffer:        make([]T, 0, config.MaxBatchSize),
		ctx:           ctx,
		cancel:        cancel,
		errorCh:       make(chan error, 10),
		itemCh:        make(chan T, config.BufferSize),
	}

	bp.wg.Add(1)
	go bp.run()

	return bp
}

// NewBatchProcessorWithContext creates a batch processor with custom context
func NewBatchProcessorWithContext[T any](ctx context.Context, config BatchProcessorConfig, processor func([]T) error) *BatchProcessor[T] {
	ctx, cancel := context.WithCancel(ctx)

	bp := &BatchProcessor[T]{
		maxBatchSize:  config.MaxBatchSize,
		flushInterval: config.FlushInterval,
		processor:     processor,
		buffer:        make([]T, 0, config.MaxBatchSize),
		ctx:           ctx,
		cancel:        cancel,
		errorCh:       make(chan error, 10),
		itemCh:        make(chan T, config.BufferSize),
	}

	bp.wg.Add(1)
	go bp.run()

	return bp
}

// Add adds an item to the batch processor
func (bp *BatchProcessor[T]) Add(item T) error {
	select {
	case <-bp.ctx.Done():
		return bp.ctx.Err()
	case bp.itemCh <- item:
		return nil
	}
}

// AddWithTimeout adds an item with a timeout
func (bp *BatchProcessor[T]) AddWithTimeout(item T, timeout time.Duration) error {
	select {
	case <-bp.ctx.Done():
		return bp.ctx.Err()
	case bp.itemCh <- item:
		return nil
	case <-time.After(timeout):
		return context.DeadlineExceeded
	}
}

// run is the main processing loop
func (bp *BatchProcessor[T]) run() {
	defer bp.wg.Done()

	ticker := time.NewTicker(bp.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bp.ctx.Done():
			// Flush remaining items before exiting
			bp.flush()
			return

		case item := <-bp.itemCh:
			bp.mu.Lock()
			bp.buffer = append(bp.buffer, item)
			shouldFlush := len(bp.buffer) >= bp.maxBatchSize
			bp.mu.Unlock()

			if shouldFlush {
				bp.flush()
				ticker.Reset(bp.flushInterval)
			}

		case <-ticker.C:
			bp.flush()
		}
	}
}

// flush processes the current batch
func (bp *BatchProcessor[T]) flush() {
	bp.mu.Lock()
	if len(bp.buffer) == 0 {
		bp.mu.Unlock()
		return
	}

	batch := bp.buffer
	bp.buffer = make([]T, 0, bp.maxBatchSize)
	bp.mu.Unlock()

	if err := bp.processor(batch); err != nil {
		select {
		case bp.errorCh <- err:
		default:
			// Error channel is full, drop error
		}
	}
}

// Flush manually triggers a batch flush
func (bp *BatchProcessor[T]) Flush() error {
	bp.flush()
	return nil
}

// Stop gracefully stops the batch processor, flushing remaining items
func (bp *BatchProcessor[T]) Stop() error {
	close(bp.itemCh)
	bp.wg.Wait()
	close(bp.errorCh)
	return nil
}

// StopWithTimeout stops the processor with a timeout
func (bp *BatchProcessor[T]) StopWithTimeout(timeout time.Duration) error {
	close(bp.itemCh)

	done := make(chan struct{})
	go func() {
		bp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		close(bp.errorCh)
		return nil
	case <-time.After(timeout):
		bp.cancel()
		return context.DeadlineExceeded
	}
}

// Cancel immediately cancels the processor without flushing
func (bp *BatchProcessor[T]) Cancel() {
	bp.cancel()
	bp.wg.Wait()
	close(bp.errorCh)
}

// Errors returns a channel for receiving processing errors
func (bp *BatchProcessor[T]) Errors() <-chan error {
	return bp.errorCh
}

// BufferSize returns the current number of items in the buffer
func (bp *BatchProcessor[T]) BufferSize() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.buffer)
}
