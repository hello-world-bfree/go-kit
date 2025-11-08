package concurrency

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	t.Run("basic execution", func(t *testing.T) {
		pool := NewWorkerPool(3, 10)
		defer pool.Stop()

		var counter int64
		for i := 0; i < 10; i++ {
			err := pool.SubmitFunc("task", func(ctx context.Context) error {
				atomic.AddInt64(&counter, 1)
				return nil
			})
			if err != nil {
				t.Fatalf("failed to submit task: %v", err)
			}
		}

		pool.Stop()

		if counter != 10 {
			t.Errorf("expected 10 tasks executed, got %d", counter)
		}
	})

	t.Run("error handling", func(t *testing.T) {
		pool := NewWorkerPool(2, 5)
		defer pool.Stop()

		expectedErr := errors.New("task error")
		var errorCount int64

		pool.SetErrorHandler(func(err error) {
			atomic.AddInt64(&errorCount, 1)
		})

		for i := 0; i < 5; i++ {
			pool.SubmitFunc("task", func(ctx context.Context) error {
				return expectedErr
			})
		}

		pool.Stop()

		if errorCount != 5 {
			t.Errorf("expected 5 errors, got %d", errorCount)
		}
	})

	t.Run("metrics tracking", func(t *testing.T) {
		pool := NewWorkerPool(2, 10)
		defer pool.Stop()

		for i := 0; i < 10; i++ {
			pool.SubmitFunc("task", func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			})
		}

		pool.Stop()

		metrics := pool.GetMetrics()
		if metrics.TasksSubmitted != 10 {
			t.Errorf("expected 10 submitted, got %d", metrics.TasksSubmitted)
		}
		if metrics.TasksCompleted != 10 {
			t.Errorf("expected 10 completed, got %d", metrics.TasksCompleted)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pool := NewWorkerPoolWithContext(ctx, 2, 10)

		var started int64
		var completed int64

		for i := 0; i < 10; i++ {
			pool.SubmitFunc("task", func(ctx context.Context) error {
				atomic.AddInt64(&started, 1)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(100 * time.Millisecond):
					atomic.AddInt64(&completed, 1)
					return nil
				}
			})
		}

		time.Sleep(50 * time.Millisecond)
		cancel()
		pool.Stop()

		if completed == started {
			t.Error("expected some tasks to be cancelled")
		}
	})
}

func TestWorkerPoolTimeout(t *testing.T) {
	pool := NewWorkerPool(1, 0) // No queue buffer
	defer pool.Stop()

	// Fill the worker with a long-running task
	pool.SubmitFunc("task1", func(ctx context.Context) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	})

	// Give time for task1 to start
	time.Sleep(10 * time.Millisecond)

	// This should timeout because worker is busy and queue is full
	err := pool.SubmitWithTimeout(Task{
		ID: "task2",
		Execute: func(ctx context.Context) error {
			return nil
		},
	}, 10*time.Millisecond)

	if err == nil {
		t.Error("expected timeout error")
	}
}

func BenchmarkWorkerPool(b *testing.B) {
	pool := NewWorkerPool(10, 1000)
	defer pool.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.SubmitFunc("task", func(ctx context.Context) error {
			time.Sleep(1 * time.Millisecond)
			return nil
		})
	}
	pool.Stop()
}
