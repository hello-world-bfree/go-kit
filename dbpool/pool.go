package dbpool

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// DBPool manages a database connection pool with health checks and metrics
type DBPool struct {
	db      *sql.DB
	config  PoolConfig
	metrics *PoolMetrics
	mu      sync.RWMutex
	healthy bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// PoolConfig configures the database connection pool
type PoolConfig struct {
	// Connection settings
	MaxOpenConns    int           // Maximum open connections
	MaxIdleConns    int           // Maximum idle connections
	ConnMaxLifetime time.Duration // Maximum lifetime of a connection
	ConnMaxIdleTime time.Duration // Maximum idle time for a connection

	// Health check settings
	HealthCheckInterval time.Duration // How often to run health checks
	HealthCheckTimeout  time.Duration // Timeout for health checks
	PingTimeout         time.Duration // Timeout for ping operations

	// Retry settings
	MaxRetries     int           // Maximum retry attempts
	RetryDelay     time.Duration // Initial delay between retries
	RetryBackoff   float64       // Backoff multiplier for retries
}

// PoolMetrics tracks connection pool statistics
type PoolMetrics struct {
	mu              sync.RWMutex
	TotalConns      int64
	IdleConns       int64
	ActiveConns     int64
	WaitCount       int64
	WaitDuration    time.Duration
	HealthChecks    int64
	FailedHealths   int64
	QueriesExecuted int64
	QueryErrors     int64
}

// DefaultPoolConfig returns a pool config with sensible defaults
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpenConns:        25,
		MaxIdleConns:        5,
		ConnMaxLifetime:     5 * time.Minute,
		ConnMaxIdleTime:     1 * time.Minute,
		HealthCheckInterval: 30 * time.Second,
		HealthCheckTimeout:  5 * time.Second,
		PingTimeout:         2 * time.Second,
		MaxRetries:          3,
		RetryDelay:          100 * time.Millisecond,
		RetryBackoff:        2.0,
	}
}

// NewDBPool creates a new database pool
func NewDBPool(db *sql.DB, config PoolConfig) *DBPool {
	// Apply configuration to sql.DB
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)

	pool := &DBPool{
		db:      db,
		config:  config,
		metrics: &PoolMetrics{},
		healthy: true,
		stopCh:  make(chan struct{}),
	}

	// Start health check routine if configured
	if config.HealthCheckInterval > 0 {
		pool.wg.Add(1)
		go pool.healthCheckRoutine()
	}

	return pool
}

// Connect creates a new database connection and pool
func Connect(driverName, dataSourceName string, config PoolConfig) (*DBPool, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), config.PingTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return NewDBPool(db, config), nil
}

// DB returns the underlying sql.DB
func (p *DBPool) DB() *sql.DB {
	return p.db
}

// IsHealthy returns the current health status
func (p *DBPool) IsHealthy() bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.healthy
}

// Ping checks if the database is reachable
func (p *DBPool) Ping(ctx context.Context) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), p.config.PingTimeout)
		defer cancel()
	}

	return p.db.PingContext(ctx)
}

// healthCheckRoutine periodically checks database health
func (p *DBPool) healthCheckRoutine() {
	defer p.wg.Done()

	ticker := time.NewTicker(p.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.runHealthCheck()
		}
	}
}

// runHealthCheck executes a single health check
func (p *DBPool) runHealthCheck() {
	p.metrics.mu.Lock()
	p.metrics.HealthChecks++
	p.metrics.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), p.config.HealthCheckTimeout)
	defer cancel()

	err := p.db.PingContext(ctx)

	p.mu.Lock()
	if err != nil {
		p.healthy = false
		p.metrics.mu.Lock()
		p.metrics.FailedHealths++
		p.metrics.mu.Unlock()
	} else {
		p.healthy = true
	}
	p.mu.Unlock()
}

// updateMetrics updates pool metrics from sql.DBStats
func (p *DBPool) updateMetrics() {
	stats := p.db.Stats()

	p.metrics.mu.Lock()
	p.metrics.TotalConns = int64(stats.OpenConnections)
	p.metrics.IdleConns = int64(stats.Idle)
	p.metrics.ActiveConns = int64(stats.InUse)
	p.metrics.WaitCount = stats.WaitCount
	p.metrics.WaitDuration = stats.WaitDuration
	p.metrics.mu.Unlock()
}

// GetMetrics returns a copy of current metrics
func (p *DBPool) GetMetrics() PoolMetrics {
	p.updateMetrics()

	p.metrics.mu.RLock()
	defer p.metrics.mu.RUnlock()

	return PoolMetrics{
		TotalConns:      p.metrics.TotalConns,
		IdleConns:       p.metrics.IdleConns,
		ActiveConns:     p.metrics.ActiveConns,
		WaitCount:       p.metrics.WaitCount,
		WaitDuration:    p.metrics.WaitDuration,
		HealthChecks:    p.metrics.HealthChecks,
		FailedHealths:   p.metrics.FailedHealths,
		QueriesExecuted: p.metrics.QueriesExecuted,
		QueryErrors:     p.metrics.QueryErrors,
	}
}

// Stats returns sql.DBStats
func (p *DBPool) Stats() sql.DBStats {
	return p.db.Stats()
}

// Close closes the database pool
func (p *DBPool) Close() error {
	close(p.stopCh)
	p.wg.Wait()
	return p.db.Close()
}

// ExecContext executes a query without returning rows
func (p *DBPool) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	result, err := p.db.ExecContext(ctx, query, args...)

	p.metrics.mu.Lock()
	p.metrics.QueriesExecuted++
	if err != nil {
		p.metrics.QueryErrors++
	}
	p.metrics.mu.Unlock()

	return result, err
}

// QueryContext executes a query that returns rows
func (p *DBPool) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	rows, err := p.db.QueryContext(ctx, query, args...)

	p.metrics.mu.Lock()
	p.metrics.QueriesExecuted++
	if err != nil {
		p.metrics.QueryErrors++
	}
	p.metrics.mu.Unlock()

	return rows, err
}

// QueryRowContext executes a query that returns at most one row
func (p *DBPool) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	p.metrics.mu.Lock()
	p.metrics.QueriesExecuted++
	p.metrics.mu.Unlock()

	return p.db.QueryRowContext(ctx, query, args...)
}

// BeginTx starts a transaction
func (p *DBPool) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return p.db.BeginTx(ctx, opts)
}

// WithRetry executes a function with retry logic
func (p *DBPool) WithRetry(ctx context.Context, fn func() error) error {
	var err error
	delay := p.config.RetryDelay

	for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Don't retry on last attempt
		if attempt == p.config.MaxRetries {
			break
		}

		// Wait before retry
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			delay = time.Duration(float64(delay) * p.config.RetryBackoff)
		}
	}

	return fmt.Errorf("max retries exceeded: %w", err)
}
