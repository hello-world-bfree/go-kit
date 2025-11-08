package dbpool

import (
	"context"
	"fmt"
	"sync"
)

// MultiPool manages multiple database connection pools
type MultiPool struct {
	pools map[string]*DBPool
	mu    sync.RWMutex
}

// NewMultiPool creates a new multi-pool manager
func NewMultiPool() *MultiPool {
	return &MultiPool{
		pools: make(map[string]*DBPool),
	}
}

// Add adds a connection pool with a name
func (mp *MultiPool) Add(name string, pool *DBPool) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if _, exists := mp.pools[name]; exists {
		return fmt.Errorf("pool %s already exists", name)
	}

	mp.pools[name] = pool
	return nil
}

// AddNew creates and adds a new connection pool
func (mp *MultiPool) AddNew(name, driverName, dataSourceName string, config PoolConfig) error {
	pool, err := Connect(driverName, dataSourceName, config)
	if err != nil {
		return fmt.Errorf("failed to create pool %s: %w", name, err)
	}

	return mp.Add(name, pool)
}

// Get retrieves a pool by name
func (mp *MultiPool) Get(name string) (*DBPool, error) {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	pool, exists := mp.pools[name]
	if !exists {
		return nil, fmt.Errorf("pool %s not found", name)
	}

	return pool, nil
}

// Remove removes a pool by name and closes it
func (mp *MultiPool) Remove(name string) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	pool, exists := mp.pools[name]
	if !exists {
		return fmt.Errorf("pool %s not found", name)
	}

	if err := pool.Close(); err != nil {
		return fmt.Errorf("failed to close pool %s: %w", name, err)
	}

	delete(mp.pools, name)
	return nil
}

// List returns all pool names
func (mp *MultiPool) List() []string {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	names := make([]string, 0, len(mp.pools))
	for name := range mp.pools {
		names = append(names, name)
	}
	return names
}

// HealthCheck checks the health of all pools
func (mp *MultiPool) HealthCheck(ctx context.Context) map[string]error {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	results := make(map[string]error)
	for name, pool := range mp.pools {
		results[name] = pool.Ping(ctx)
	}
	return results
}

// GetAllMetrics returns metrics for all pools
func (mp *MultiPool) GetAllMetrics() map[string]PoolMetrics {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	metrics := make(map[string]PoolMetrics)
	for name, pool := range mp.pools {
		metrics[name] = pool.GetMetrics()
	}
	return metrics
}

// Close closes all pools
func (mp *MultiPool) Close() error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	var errs []error
	for name, pool := range mp.pools {
		if err := pool.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close pool %s: %w", name, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pools: %v", errs)
	}

	mp.pools = make(map[string]*DBPool)
	return nil
}

// WithPool executes a function with a named pool
func (mp *MultiPool) WithPool(name string, fn func(*DBPool) error) error {
	pool, err := mp.Get(name)
	if err != nil {
		return err
	}
	return fn(pool)
}

// WithTransaction executes a transaction on a named pool
func (mp *MultiPool) WithTransaction(ctx context.Context, poolName string, fn TxFunc) error {
	pool, err := mp.Get(poolName)
	if err != nil {
		return err
	}
	return pool.WithTransaction(ctx, fn)
}

// ShardedPool manages sharded database connections
type ShardedPool struct {
	pools      []*DBPool
	shardCount int
	mu         sync.RWMutex
}

// NewShardedPool creates a new sharded pool
func NewShardedPool(shardCount int) *ShardedPool {
	return &ShardedPool{
		pools:      make([]*DBPool, shardCount),
		shardCount: shardCount,
	}
}

// AddShard adds a pool for a specific shard
func (sp *ShardedPool) AddShard(shardID int, pool *DBPool) error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	if shardID < 0 || shardID >= sp.shardCount {
		return fmt.Errorf("invalid shard ID: %d (must be 0-%d)", shardID, sp.shardCount-1)
	}

	sp.pools[shardID] = pool
	return nil
}

// GetShard retrieves a pool by shard ID
func (sp *ShardedPool) GetShard(shardID int) (*DBPool, error) {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	if shardID < 0 || shardID >= sp.shardCount {
		return nil, fmt.Errorf("invalid shard ID: %d", shardID)
	}

	pool := sp.pools[shardID]
	if pool == nil {
		return nil, fmt.Errorf("shard %d not initialized", shardID)
	}

	return pool, nil
}

// GetShardByKey retrieves a pool by hashing a key
func (sp *ShardedPool) GetShardByKey(key string) (*DBPool, error) {
	shardID := sp.hashKey(key)
	return sp.GetShard(shardID)
}

// hashKey computes the shard ID for a key
func (sp *ShardedPool) hashKey(key string) int {
	var hash uint32
	for _, c := range key {
		hash = hash*31 + uint32(c)
	}
	return int(hash % uint32(sp.shardCount))
}

// WithShard executes a function with a specific shard
func (sp *ShardedPool) WithShard(shardID int, fn func(*DBPool) error) error {
	pool, err := sp.GetShard(shardID)
	if err != nil {
		return err
	}
	return fn(pool)
}

// WithShardByKey executes a function with a shard determined by key
func (sp *ShardedPool) WithShardByKey(key string, fn func(*DBPool) error) error {
	pool, err := sp.GetShardByKey(key)
	if err != nil {
		return err
	}
	return fn(pool)
}

// Close closes all shard pools
func (sp *ShardedPool) Close() error {
	sp.mu.Lock()
	defer sp.mu.Unlock()

	var errs []error
	for i, pool := range sp.pools {
		if pool != nil {
			if err := pool.Close(); err != nil {
				errs = append(errs, fmt.Errorf("failed to close shard %d: %w", i, err))
			}
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing shards: %v", errs)
	}

	return nil
}

// HealthCheckAll checks health of all shards
func (sp *ShardedPool) HealthCheckAll(ctx context.Context) map[int]error {
	sp.mu.RLock()
	defer sp.mu.RUnlock()

	results := make(map[int]error)
	for i, pool := range sp.pools {
		if pool != nil {
			results[i] = pool.Ping(ctx)
		} else {
			results[i] = fmt.Errorf("shard not initialized")
		}
	}
	return results
}
