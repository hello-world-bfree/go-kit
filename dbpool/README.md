# DBPool Package

The `dbpool` package provides enhanced database connection pool management with health checks, metrics, and utilities.

## Features

### Connection Pool

Manages database connections with automatic health checks and metrics.

```go
import "github.com/flarco/g/dbpool"

config := dbpool.DefaultPoolConfig()
config.MaxOpenConns = 25
config.MaxIdleConns = 5
config.HealthCheckInterval = 30 * time.Second

pool, err := dbpool.Connect("postgres", "connection-string", config)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// Check health
if pool.IsHealthy() {
    fmt.Println("Database is healthy")
}

// Get metrics
metrics := pool.GetMetrics()
fmt.Printf("Active connections: %d\n", metrics.ActiveConns)
```

### Query Execution with Retry

```go
ctx := context.Background()

// Execute with automatic retry
err := pool.WithRetry(ctx, func() error {
    _, err := pool.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "John")
    return err
})
```

### Transaction Management

```go
// Automatic commit/rollback
err := pool.WithTransaction(ctx, func(tx *sql.Tx) error {
    _, err := tx.Exec("INSERT INTO users (name) VALUES (?)", "Alice")
    if err != nil {
        return err // Automatic rollback
    }
    _, err = tx.Exec("INSERT INTO logs (action) VALUES (?)", "user_created")
    return err // Automatic commit on success
})

// Transaction manager with retry
tm := dbpool.NewTxManager(pool)
err := tm.Execute(ctx, func(tx *sql.Tx) error {
    // Transaction logic
    return nil
})
```

### Query Builder

Fluent interface for building SQL queries.

```go
qb := dbpool.NewQueryBuilder(pool)

// SELECT
rows, err := qb.Select("id", "name", "email").
    From("users").
    Where("age > ?", 18).
    OrderBy("name ASC").
    Limit(10).
    Query(ctx)

// INSERT
result, err := qb.Insert("users").
    Values(1, "John", "john@example.com").
    Exec(ctx)

// UPDATE
result, err := qb.Update("users").
    Set("email", "newemail@example.com").
    Where("id = ?", 1).
    Exec(ctx)

// COUNT
count, err := qb.Select().
    From("users").
    Where("active = ?", true).
    Count(ctx)
```

### Multi-Pool Management

Manage multiple database connections.

```go
mp := dbpool.NewMultiPool()

// Add pools
mp.AddNew("primary", "postgres", "primary-connection", config)
mp.AddNew("replica", "postgres", "replica-connection", config)

// Use a specific pool
err := mp.WithPool("primary", func(pool *dbpool.DBPool) error {
    _, err := pool.ExecContext(ctx, "INSERT INTO users (name) VALUES (?)", "Bob")
    return err
})

// Health check all pools
results := mp.HealthCheck(ctx)
for name, err := range results {
    fmt.Printf("%s: %v\n", name, err)
}
```

### Sharded Pools

Distribute data across multiple database shards.

```go
sharded := dbpool.NewShardedPool(4) // 4 shards

// Add shard pools
for i := 0; i < 4; i++ {
    pool, _ := dbpool.Connect("postgres", fmt.Sprintf("shard-%d-connection", i), config)
    sharded.AddShard(i, pool)
}

// Access by key (automatic sharding)
err := sharded.WithShardByKey("user:12345", func(pool *dbpool.DBPool) error {
    _, err := pool.ExecContext(ctx, "INSERT INTO users (id, name) VALUES (?, ?)", 12345, "Alice")
    return err
})
```

## Configuration

```go
config := dbpool.PoolConfig{
    // Connection settings
    MaxOpenConns:    25,
    MaxIdleConns:    5,
    ConnMaxLifetime: 5 * time.Minute,
    ConnMaxIdleTime: 1 * time.Minute,

    // Health check settings
    HealthCheckInterval: 30 * time.Second,
    HealthCheckTimeout:  5 * time.Second,
    PingTimeout:         2 * time.Second,

    // Retry settings
    MaxRetries:   3,
    RetryDelay:   100 * time.Millisecond,
    RetryBackoff: 2.0,
}
```

## Metrics

Track pool performance and health:

- `TotalConns`: Total open connections
- `IdleConns`: Idle connections
- `ActiveConns`: Active connections
- `WaitCount`: Number of times waited for connection
- `WaitDuration`: Total wait duration
- `HealthChecks`: Number of health checks performed
- `FailedHealths`: Number of failed health checks
- `QueriesExecuted`: Total queries executed
- `QueryErrors`: Number of query errors

## Use Cases

- **Production databases**: Automatic health monitoring and connection management
- **High availability**: Multi-pool support for primary/replica setups
- **Sharded databases**: Distribute load across multiple database instances
- **Reliable operations**: Automatic retry with exponential backoff
- **Monitoring**: Built-in metrics for observability
