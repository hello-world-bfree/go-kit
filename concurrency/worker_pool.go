package concurrency

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// WorkerPool manages a pool of workers for processing IO-bound tasks
type WorkerPool struct {
	workers      int
	taskQueue    chan Task
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	metrics      *PoolMetrics
	errorHandler func(error)
	stopOnce     sync.Once
	stopped      bool
	mu           sync.RWMutex
}

// Task represents a unit of work to be processed
type Task struct {
	ID      string
	Execute func(context.Context) error
	OnError func(error)
}

// PoolMetrics tracks worker pool statistics
type PoolMetrics struct {
	mu             sync.RWMutex
	TasksSubmitted int64
	TasksCompleted int64
	TasksFailed    int64
	TotalDuration  time.Duration
	AvgDuration    time.Duration
}

// NewWorkerPool creates a new worker pool with the specified number of workers
func NewWorkerPool(workers int, queueSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		workers:   workers,
		taskQueue: make(chan Task, queueSize),
		ctx:       ctx,
		cancel:    cancel,
		metrics:   &PoolMetrics{},
	}

	pool.start()
	return pool
}

// NewWorkerPoolWithContext creates a worker pool with a custom context
func NewWorkerPoolWithContext(ctx context.Context, workers int, queueSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(ctx)

	pool := &WorkerPool{
		workers:   workers,
		taskQueue: make(chan Task, queueSize),
		ctx:       ctx,
		cancel:    cancel,
		metrics:   &PoolMetrics{},
	}

	pool.start()
	return pool
}

// SetErrorHandler sets a global error handler for the pool
func (wp *WorkerPool) SetErrorHandler(handler func(error)) {
	wp.errorHandler = handler
}

// start initializes all workers
func (wp *WorkerPool) start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

// worker processes tasks from the queue
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case task, ok := <-wp.taskQueue:
			if !ok {
				return
			}
			wp.executeTask(task)
		}
	}
}

// executeTask runs a single task and tracks metrics
func (wp *WorkerPool) executeTask(task Task) {
	start := time.Now()
	err := task.Execute(wp.ctx)
	duration := time.Since(start)

	wp.metrics.mu.Lock()
	wp.metrics.TasksCompleted++
	wp.metrics.TotalDuration += duration
	wp.metrics.AvgDuration = time.Duration(int64(wp.metrics.TotalDuration) / wp.metrics.TasksCompleted)

	if err != nil {
		wp.metrics.TasksFailed++
		wp.metrics.mu.Unlock()

		// Handle error with task-specific handler first
		if task.OnError != nil {
			task.OnError(err)
		} else if wp.errorHandler != nil {
			wp.errorHandler(fmt.Errorf("task %s failed: %w", task.ID, err))
		}
	} else {
		wp.metrics.mu.Unlock()
	}
}

// Submit submits a task to the worker pool
func (wp *WorkerPool) Submit(task Task) error {
	wp.mu.RLock()
	if wp.stopped {
		wp.mu.RUnlock()
		return fmt.Errorf("worker pool is stopped")
	}
	wp.mu.RUnlock()

	wp.metrics.mu.Lock()
	wp.metrics.TasksSubmitted++
	wp.metrics.mu.Unlock()

	select {
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	case wp.taskQueue <- task:
		return nil
	}
}

// SubmitFunc is a convenience method to submit a function as a task
func (wp *WorkerPool) SubmitFunc(id string, fn func(context.Context) error) error {
	return wp.Submit(Task{
		ID:      id,
		Execute: fn,
	})
}

// SubmitWithTimeout submits a task with a timeout
func (wp *WorkerPool) SubmitWithTimeout(task Task, timeout time.Duration) error {
	select {
	case <-wp.ctx.Done():
		return wp.ctx.Err()
	case wp.taskQueue <- task:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout submitting task %s", task.ID)
	}
}

// Stop gracefully shuts down the worker pool
func (wp *WorkerPool) Stop() {
	wp.stopOnce.Do(func() {
		wp.mu.Lock()
		wp.stopped = true
		wp.mu.Unlock()
		close(wp.taskQueue)
		wp.wg.Wait()
	})
}

// StopWithTimeout attempts to stop the pool within the specified duration
func (wp *WorkerPool) StopWithTimeout(timeout time.Duration) error {
	var err error
	wp.stopOnce.Do(func() {
		wp.mu.Lock()
		wp.stopped = true
		wp.mu.Unlock()
		close(wp.taskQueue)

		done := make(chan struct{})
		go func() {
			wp.wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			err = nil
		case <-time.After(timeout):
			wp.cancel()
			err = fmt.Errorf("worker pool shutdown timeout exceeded")
		}
	})
	return err
}

// Cancel immediately cancels all pending work
func (wp *WorkerPool) Cancel() {
	wp.cancel()
	wp.wg.Wait()
}

// GetMetrics returns a copy of the current metrics
func (wp *WorkerPool) GetMetrics() PoolMetrics {
	wp.metrics.mu.RLock()
	defer wp.metrics.mu.RUnlock()

	return PoolMetrics{
		TasksSubmitted: wp.metrics.TasksSubmitted,
		TasksCompleted: wp.metrics.TasksCompleted,
		TasksFailed:    wp.metrics.TasksFailed,
		TotalDuration:  wp.metrics.TotalDuration,
		AvgDuration:    wp.metrics.AvgDuration,
	}
}

// QueueSize returns the current number of tasks in the queue
func (wp *WorkerPool) QueueSize() int {
	return len(wp.taskQueue)
}

// QueueCapacity returns the maximum queue capacity
func (wp *WorkerPool) QueueCapacity() int {
	return cap(wp.taskQueue)
}
