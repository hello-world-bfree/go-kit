package concurrency

import (
	"context"
	"fmt"
	"time"
)

// Semaphore provides a weighted semaphore for controlling concurrent access to resources
type Semaphore struct {
	permits chan struct{}
}

// NewSemaphore creates a new semaphore with the specified number of permits
func NewSemaphore(permits int) *Semaphore {
	if permits <= 0 {
		panic("semaphore permits must be positive")
	}

	return &Semaphore{
		permits: make(chan struct{}, permits),
	}
}

// Acquire acquires one permit from the semaphore, blocking if necessary
func (s *Semaphore) Acquire() {
	s.permits <- struct{}{}
}

// AcquireN acquires n permits from the semaphore, blocking if necessary
func (s *Semaphore) AcquireN(n int) {
	for i := 0; i < n; i++ {
		s.permits <- struct{}{}
	}
}

// TryAcquire attempts to acquire one permit without blocking
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.permits <- struct{}{}:
		return true
	default:
		return false
	}
}

// TryAcquireN attempts to acquire n permits without blocking
func (s *Semaphore) TryAcquireN(n int) bool {
	acquired := 0
	for i := 0; i < n; i++ {
		if s.TryAcquire() {
			acquired++
		} else {
			// Release acquired permits and return false
			for j := 0; j < acquired; j++ {
				s.Release()
			}
			return false
		}
	}
	return true
}

// AcquireWithTimeout attempts to acquire one permit within the timeout
func (s *Semaphore) AcquireWithTimeout(timeout time.Duration) error {
	select {
	case s.permits <- struct{}{}:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("timeout acquiring semaphore")
	}
}

// AcquireWithContext attempts to acquire one permit, respecting context cancellation
func (s *Semaphore) AcquireWithContext(ctx context.Context) error {
	select {
	case s.permits <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release releases one permit back to the semaphore
func (s *Semaphore) Release() {
	select {
	case <-s.permits:
	default:
		panic("release of unacquired semaphore")
	}
}

// ReleaseN releases n permits back to the semaphore
func (s *Semaphore) ReleaseN(n int) {
	for i := 0; i < n; i++ {
		s.Release()
	}
}

// Available returns the number of available permits
func (s *Semaphore) Available() int {
	return cap(s.permits) - len(s.permits)
}

// Capacity returns the total capacity of the semaphore
func (s *Semaphore) Capacity() int {
	return cap(s.permits)
}

// WithSemaphore executes a function while holding a semaphore permit
func (s *Semaphore) WithSemaphore(fn func() error) error {
	s.Acquire()
	defer s.Release()
	return fn()
}

// WithSemaphoreContext executes a function while holding a semaphore permit, with context support
func (s *Semaphore) WithSemaphoreContext(ctx context.Context, fn func(context.Context) error) error {
	if err := s.AcquireWithContext(ctx); err != nil {
		return err
	}
	defer s.Release()
	return fn(ctx)
}
