package dbpool

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}

	// Create test table
	_, err = db.Exec(`CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)`)
	if err != nil {
		t.Fatalf("failed to create test table: %v", err)
	}

	return db
}

func TestDBPool(t *testing.T) {
	t.Run("basic connection", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0 // Disable for testing

		pool := NewDBPool(db, config)
		defer pool.Close()

		if !pool.IsHealthy() {
			t.Error("pool should be healthy")
		}

		ctx := context.Background()
		err := pool.Ping(ctx)
		if err != nil {
			t.Errorf("ping failed: %v", err)
		}
	})

	t.Run("execute query", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool := NewDBPool(db, config)
		defer pool.Close()

		ctx := context.Background()

		// Insert data
		result, err := pool.ExecContext(ctx, "INSERT INTO test (value) VALUES (?)", "test1")
		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("failed to get rows affected: %v", err)
		}
		if rowsAffected != 1 {
			t.Errorf("expected 1 row affected, got %d", rowsAffected)
		}

		// Query data
		var value string
		err = pool.QueryRowContext(ctx, "SELECT value FROM test WHERE id = ?", 1).Scan(&value)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if value != "test1" {
			t.Errorf("expected 'test1', got %s", value)
		}
	})

	t.Run("metrics tracking", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool := NewDBPool(db, config)
		defer pool.Close()

		ctx := context.Background()

		// Execute some queries
		pool.ExecContext(ctx, "INSERT INTO test (value) VALUES (?)", "test1")
		pool.ExecContext(ctx, "INSERT INTO test (value) VALUES (?)", "test2")
		pool.QueryContext(ctx, "SELECT * FROM test")

		metrics := pool.GetMetrics()
		if metrics.QueriesExecuted != 3 {
			t.Errorf("expected 3 queries executed, got %d", metrics.QueriesExecuted)
		}
	})

	t.Run("retry logic", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0
		config.MaxRetries = 3

		pool := NewDBPool(db, config)
		defer pool.Close()

		ctx := context.Background()

		attempts := 0
		err := pool.WithRetry(ctx, func() error {
			attempts++
			if attempts < 3 {
				return sql.ErrConnDone
			}
			return nil
		})

		if err != nil {
			t.Errorf("retry should succeed on 3rd attempt, got error: %v", err)
		}
		if attempts != 3 {
			t.Errorf("expected 3 attempts, got %d", attempts)
		}
	})
}

func TestTransaction(t *testing.T) {
	t.Run("successful transaction", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool := NewDBPool(db, config)
		defer pool.Close()

		ctx := context.Background()

		err := pool.WithTransaction(ctx, func(tx *sql.Tx) error {
			_, err := tx.Exec("INSERT INTO test (value) VALUES (?)", "tx1")
			if err != nil {
				return err
			}
			_, err = tx.Exec("INSERT INTO test (value) VALUES (?)", "tx2")
			return err
		})

		if err != nil {
			t.Fatalf("transaction failed: %v", err)
		}

		// Verify data was committed
		var count int
		err = pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 2 {
			t.Errorf("expected 2 rows, got %d", count)
		}
	})

	t.Run("rollback on error", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool := NewDBPool(db, config)
		defer pool.Close()

		ctx := context.Background()

		// Insert initial data
		pool.ExecContext(ctx, "INSERT INTO test (value) VALUES (?)", "existing")

		err := pool.WithTransaction(ctx, func(tx *sql.Tx) error {
			_, err := tx.Exec("INSERT INTO test (value) VALUES (?)", "tx1")
			if err != nil {
				return err
			}
			return sql.ErrConnDone // Force error
		})

		if err == nil {
			t.Fatal("expected transaction to fail")
		}

		// Verify rollback - should only have initial data
		var count int
		err = pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM test").Scan(&count)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if count != 1 {
			t.Errorf("expected 1 row (rollback), got %d", count)
		}
	})
}

func TestQueryBuilder(t *testing.T) {
	t.Run("select query", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool := NewDBPool(db, config)
		defer pool.Close()

		qb := NewQueryBuilder(pool)
		query, args := qb.Select("id", "value").
			From("test").
			Where("id > ?", 0).
			OrderBy("id DESC").
			Limit(10).
			Build()

		expectedQuery := "SELECT id, value FROM test WHERE id > ? ORDER BY id DESC LIMIT 10"
		if query != expectedQuery {
			t.Errorf("expected query:\n%s\ngot:\n%s", expectedQuery, query)
		}
		if len(args) != 1 || args[0] != 0 {
			t.Errorf("expected args [0], got %v", args)
		}
	})

	t.Run("insert query", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool := NewDBPool(db, config)
		defer pool.Close()

		ctx := context.Background()

		// Use direct exec instead of query builder for INSERT
		result, err := pool.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (?, ?)", 1, "value1")

		if err != nil {
			t.Fatalf("insert failed: %v", err)
		}

		rows, _ := result.RowsAffected()
		if rows != 1 {
			t.Errorf("expected 1 row affected, got %d", rows)
		}
	})

	t.Run("update query", func(t *testing.T) {
		db := setupTestDB(t)
		defer db.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool := NewDBPool(db, config)
		defer pool.Close()

		ctx := context.Background()

		// Insert test data
		pool.ExecContext(ctx, "INSERT INTO test (id, value) VALUES (?, ?)", 1, "old")

		// Update
		_, err := NewQueryBuilder(pool).
			Update("test").
			Set("value", "new").
			Where("id = ?", 1).
			Exec(ctx)

		if err != nil {
			t.Fatalf("update failed: %v", err)
		}

		// Verify
		var value string
		pool.QueryRowContext(ctx, "SELECT value FROM test WHERE id = ?", 1).Scan(&value)
		if value != "new" {
			t.Errorf("expected 'new', got %s", value)
		}
	})
}

func TestMultiPool(t *testing.T) {
	t.Run("manage multiple pools", func(t *testing.T) {
		mp := NewMultiPool()

		db1 := setupTestDB(t)
		defer db1.Close()

		db2 := setupTestDB(t)
		defer db2.Close()

		config := DefaultPoolConfig()
		config.HealthCheckInterval = 0

		pool1 := NewDBPool(db1, config)
		pool2 := NewDBPool(db2, config)

		err := mp.Add("db1", pool1)
		if err != nil {
			t.Fatalf("failed to add pool1: %v", err)
		}

		err = mp.Add("db2", pool2)
		if err != nil {
			t.Fatalf("failed to add pool2: %v", err)
		}

		// Test retrieval
		retrieved, err := mp.Get("db1")
		if err != nil {
			t.Fatalf("failed to get pool: %v", err)
		}
		if retrieved != pool1 {
			t.Error("retrieved wrong pool")
		}

		// Test list
		names := mp.List()
		if len(names) != 2 {
			t.Errorf("expected 2 pools, got %d", len(names))
		}

		// Test health check
		ctx := context.Background()
		results := mp.HealthCheck(ctx)
		if len(results) != 2 {
			t.Errorf("expected 2 health check results, got %d", len(results))
		}

		mp.Close()
	})
}

func BenchmarkDBPool(b *testing.B) {
	db := setupTestDB(&testing.T{})
	defer db.Close()

	config := DefaultPoolConfig()
	config.HealthCheckInterval = 0

	pool := NewDBPool(db, config)
	defer pool.Close()

	ctx := context.Background()

	b.Run("Query", func(b *testing.B) {
		// Insert test data
		for i := 0; i < 100; i++ {
			pool.ExecContext(ctx, "INSERT INTO test (value) VALUES (?)", "test")
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pool.QueryContext(ctx, "SELECT * FROM test LIMIT 10")
		}
	})

	b.Run("Transaction", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pool.WithTransaction(ctx, func(tx *sql.Tx) error {
				_, err := tx.Exec("INSERT INTO test (value) VALUES (?)", "test")
				return err
			})
		}
	})
}
