package concurrency

import (
	"context"
	"sync"
	"time"
)

// RateLimiter controls the rate of operations using the token bucket algorithm
type RateLimiter struct {
	rate       float64       // tokens per second
	burst      int           // maximum burst size
	tokens     float64       // current tokens
	lastUpdate time.Time     // last token update time
	mu         sync.Mutex
}

// NewRateLimiter creates a new rate limiter with specified rate and burst size
// rate: number of operations per second
// burst: maximum burst size (tokens that can accumulate)
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

// Allow checks if an operation is allowed under the rate limit
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillTokens()

	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}

// AllowN checks if n operations are allowed under the rate limit
func (rl *RateLimiter) AllowN(n int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillTokens()

	if rl.tokens >= float64(n) {
		rl.tokens -= float64(n)
		return true
	}
	return false
}

// Wait blocks until the rate limiter allows an operation
func (rl *RateLimiter) Wait(ctx context.Context) error {
	for {
		if rl.Allow() {
			return nil
		}

		// Calculate wait time
		waitTime := rl.calculateWaitTime(1)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// Continue loop to check again
		}
	}
}

// WaitN blocks until the rate limiter allows n operations
func (rl *RateLimiter) WaitN(ctx context.Context, n int) error {
	for {
		if rl.AllowN(n) {
			return nil
		}

		// Calculate wait time
		waitTime := rl.calculateWaitTime(n)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// Continue loop to check again
		}
	}
}

// Reserve reserves n tokens and returns the time to wait before proceeding
func (rl *RateLimiter) Reserve(n int) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillTokens()

	if rl.tokens >= float64(n) {
		rl.tokens -= float64(n)
		return 0
	}

	needed := float64(n) - rl.tokens
	waitTime := time.Duration(needed/rl.rate) * time.Second
	rl.tokens = 0

	return waitTime
}

// refillTokens adds tokens based on elapsed time (must be called with lock held)
func (rl *RateLimiter) refillTokens() {
	now := time.Now()
	elapsed := now.Sub(rl.lastUpdate).Seconds()
	rl.lastUpdate = now

	rl.tokens += elapsed * rl.rate
	if rl.tokens > float64(rl.burst) {
		rl.tokens = float64(rl.burst)
	}
}

// calculateWaitTime calculates how long to wait for n tokens (must be called with lock held)
func (rl *RateLimiter) calculateWaitTime(n int) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillTokens()

	if rl.tokens >= float64(n) {
		return 0
	}

	needed := float64(n) - rl.tokens
	return time.Duration(needed/rl.rate*1000) * time.Millisecond
}

// Tokens returns the current number of available tokens
func (rl *RateLimiter) Tokens() float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillTokens()
	return rl.tokens
}

// SetRate updates the rate limit
func (rl *RateLimiter) SetRate(rate float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refillTokens()
	rl.rate = rate
}

// SetBurst updates the burst size
func (rl *RateLimiter) SetBurst(burst int) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.burst = burst
	if rl.tokens > float64(burst) {
		rl.tokens = float64(burst)
	}
}

// AdaptiveRateLimiter adjusts rate based on success/failure feedback
type AdaptiveRateLimiter struct {
	*RateLimiter
	minRate     float64
	maxRate     float64
	increaseBy  float64
	decreaseBy  float64
}

// NewAdaptiveRateLimiter creates a rate limiter that adjusts based on feedback
func NewAdaptiveRateLimiter(initialRate, minRate, maxRate float64, burst int) *AdaptiveRateLimiter {
	return &AdaptiveRateLimiter{
		RateLimiter: NewRateLimiter(initialRate, burst),
		minRate:     minRate,
		maxRate:     maxRate,
		increaseBy:  initialRate * 0.1, // 10% increase
		decreaseBy:  initialRate * 0.1, // 10% decrease
	}
}

// RecordSuccess increases the rate (operation succeeded, can go faster)
func (arl *AdaptiveRateLimiter) RecordSuccess() {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	newRate := arl.rate + arl.increaseBy
	if newRate <= arl.maxRate {
		arl.rate = newRate
	}
}

// RecordFailure decreases the rate (operation failed, need to slow down)
func (arl *AdaptiveRateLimiter) RecordFailure() {
	arl.mu.Lock()
	defer arl.mu.Unlock()

	newRate := arl.rate - arl.decreaseBy
	if newRate >= arl.minRate {
		arl.rate = newRate
	}
}

// GetCurrentRate returns the current rate
func (arl *AdaptiveRateLimiter) GetCurrentRate() float64 {
	arl.mu.Lock()
	defer arl.mu.Unlock()
	return arl.rate
}
