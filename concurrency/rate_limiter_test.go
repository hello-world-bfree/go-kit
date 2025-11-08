package concurrency

import (
	"context"
	"testing"
	"time"
)

func TestRateLimiter(t *testing.T) {
	t.Run("basic rate limiting", func(t *testing.T) {
		rl := NewRateLimiter(10, 10) // 10 ops/sec, burst of 10

		// Should allow burst
		for i := 0; i < 10; i++ {
			if !rl.Allow() {
				t.Errorf("operation %d should be allowed", i)
			}
		}

		// Should block after burst
		if rl.Allow() {
			t.Error("operation should be blocked after burst")
		}
	})

	t.Run("token refill", func(t *testing.T) {
		rl := NewRateLimiter(10, 5) // 10 ops/sec, burst of 5

		// Use all tokens
		for i := 0; i < 5; i++ {
			rl.Allow()
		}

		// Wait for refill
		time.Sleep(500 * time.Millisecond)

		// Should have ~5 tokens now
		allowed := 0
		for i := 0; i < 10; i++ {
			if rl.Allow() {
				allowed++
			}
		}

		if allowed < 4 || allowed > 6 {
			t.Errorf("expected ~5 operations allowed, got %d", allowed)
		}
	})

	t.Run("wait for token", func(t *testing.T) {
		rl := NewRateLimiter(100, 1) // 100 ops/sec, burst of 1

		// Use the token
		rl.Allow()

		start := time.Now()
		ctx := context.Background()
		err := rl.Wait(ctx)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Should wait ~10ms (1/100 second)
		if elapsed < 5*time.Millisecond || elapsed > 20*time.Millisecond {
			t.Errorf("expected ~10ms wait, got %v", elapsed)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		rl := NewRateLimiter(1, 1) // 1 op/sec

		// Use the token
		rl.Allow()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		err := rl.Wait(ctx)
		if err != context.DeadlineExceeded {
			t.Errorf("expected DeadlineExceeded, got %v", err)
		}
	})

	t.Run("dynamic rate adjustment", func(t *testing.T) {
		rl := NewRateLimiter(10, 10)

		rl.SetRate(100)

		// Should allow more operations now
		time.Sleep(100 * time.Millisecond)

		allowed := 0
		for i := 0; i < 20; i++ {
			if rl.Allow() {
				allowed++
			}
		}

		if allowed < 10 {
			t.Errorf("expected at least 10 operations, got %d", allowed)
		}
	})
}

func TestAdaptiveRateLimiter(t *testing.T) {
	t.Run("adapt on feedback", func(t *testing.T) {
		arl := NewAdaptiveRateLimiter(10, 1, 100, 10)

		initialRate := arl.GetCurrentRate()

		// Record successes - rate should increase
		for i := 0; i < 10; i++ {
			arl.RecordSuccess()
		}

		if arl.GetCurrentRate() <= initialRate {
			t.Error("rate should increase after successes")
		}

		// Record failures - rate should decrease
		for i := 0; i < 20; i++ {
			arl.RecordFailure()
		}

		if arl.GetCurrentRate() >= initialRate {
			t.Error("rate should decrease after failures")
		}
	})

	t.Run("respect min and max rates", func(t *testing.T) {
		arl := NewAdaptiveRateLimiter(10, 5, 20, 10)

		// Try to increase beyond max
		for i := 0; i < 100; i++ {
			arl.RecordSuccess()
		}

		if arl.GetCurrentRate() > 20 {
			t.Errorf("rate should not exceed max (20), got %f", arl.GetCurrentRate())
		}

		// Try to decrease below min
		for i := 0; i < 100; i++ {
			arl.RecordFailure()
		}

		if arl.GetCurrentRate() < 5 {
			t.Errorf("rate should not go below min (5), got %f", arl.GetCurrentRate())
		}
	})
}

func BenchmarkRateLimiter(b *testing.B) {
	rl := NewRateLimiter(1000000, 100)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rl.Allow()
		}
	})
}
