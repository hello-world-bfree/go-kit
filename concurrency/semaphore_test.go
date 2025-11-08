package concurrency

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestSemaphore(t *testing.T) {
	t.Run("basic acquire and release", func(t *testing.T) {
		sem := NewSemaphore(3)

		// Acquire all permits
		sem.Acquire()
		sem.Acquire()
		sem.Acquire()

		if sem.Available() != 0 {
			t.Errorf("expected 0 available, got %d", sem.Available())
		}

		// Release permits
		sem.Release()
		sem.Release()
		sem.Release()

		if sem.Available() != 3 {
			t.Errorf("expected 3 available, got %d", sem.Available())
		}
	})

	t.Run("try acquire", func(t *testing.T) {
		sem := NewSemaphore(2)

		if !sem.TryAcquire() {
			t.Error("first TryAcquire should succeed")
		}
		if !sem.TryAcquire() {
			t.Error("second TryAcquire should succeed")
		}
		if sem.TryAcquire() {
			t.Error("third TryAcquire should fail")
		}

		sem.Release()
		if !sem.TryAcquire() {
			t.Error("TryAcquire should succeed after release")
		}
	})

	t.Run("concurrent access", func(t *testing.T) {
		sem := NewSemaphore(5)
		var concurrent int64
		var maxConcurrent int64

		done := make(chan bool)
		workers := 20

		for i := 0; i < workers; i++ {
			go func() {
				sem.Acquire()
				defer sem.Release()

				current := atomic.AddInt64(&concurrent, 1)
				for {
					max := atomic.LoadInt64(&maxConcurrent)
					if current <= max {
						break
					}
					if atomic.CompareAndSwapInt64(&maxConcurrent, max, current) {
						break
					}
				}

				time.Sleep(10 * time.Millisecond)
				atomic.AddInt64(&concurrent, -1)
				done <- true
			}()
		}

		for i := 0; i < workers; i++ {
			<-done
		}

		if maxConcurrent > 5 {
			t.Errorf("max concurrent should be 5, got %d", maxConcurrent)
		}
	})

	t.Run("with context", func(t *testing.T) {
		sem := NewSemaphore(1)
		sem.Acquire()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := sem.AcquireWithContext(ctx)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}

		sem.Release()
		err = sem.AcquireWithContext(context.Background())
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("with semaphore helper", func(t *testing.T) {
		sem := NewSemaphore(1)
		executed := false

		err := sem.WithSemaphore(func() error {
			executed = true
			return nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !executed {
			t.Error("function was not executed")
		}
		if sem.Available() != 1 {
			t.Error("semaphore was not released")
		}
	})
}

func BenchmarkSemaphore(b *testing.B) {
	sem := NewSemaphore(100)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			sem.Acquire()
			sem.Release()
		}
	})
}
