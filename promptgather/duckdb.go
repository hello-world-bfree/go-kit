package promptgather

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/marcboeker/go-duckdb" // DuckDB driver
)

// DuckDBSource provides optimized data fetching from DuckDB
// DuckDB is an embedded analytical database perfect for fast aggregations
type DuckDBSource struct {
	BaseDataSource
	db    *sql.DB
	query string
	args  []interface{}
}

// NewDuckDBSource creates a DuckDB data source
// Example query: "SELECT category, COUNT(*) as count, AVG(price) as avg_price FROM products GROUP BY category"
func NewDuckDBSource(name string, db *sql.DB, query string, args ...interface{}) *DuckDBSource {
	return &DuckDBSource{
		BaseDataSource: NewBaseDataSource(name, 0),
		db:             db,
		query:          query,
		args:           args,
	}
}

// WithPriority sets the priority for this source
func (d *DuckDBSource) WithPriority(priority int) *DuckDBSource {
	d.priority = priority
	return d
}

// Fetch executes the DuckDB query and returns results
// Optimized for analytical queries with aggregations
func (d *DuckDBSource) Fetch(ctx context.Context) (map[string]interface{}, error) {
	rows, err := d.db.QueryContext(ctx, d.query, d.args...)
	if err != nil {
		return nil, fmt.Errorf("DuckDB query failed: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Get column types
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Prepare result
	result := make(map[string]interface{})
	resultRows := make([]map[string]interface{}, 0)

	// Scan rows
	for rows.Next() {
		// Create a slice of interface{} to hold each column value
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// Use proper types for better performance
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create row map with type information
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]

			// Convert byte arrays to strings for text columns
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}
		resultRows = append(resultRows, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	// Store results with metadata
	result["rows"] = resultRows
	result["count"] = len(resultRows)
	result["columns"] = columns

	// Extract column types for metadata
	typeNames := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeNames[i] = ct.DatabaseTypeName()
	}
	result["column_types"] = typeNames

	// If there's only one row, also flatten the first row into top-level keys
	// This is useful for aggregation queries that return a single summary row
	if len(resultRows) == 1 {
		for key, value := range resultRows[0] {
			result[key] = value
		}
	}

	// If there's a single column with a single row, also store as "value"
	if len(resultRows) == 1 && len(columns) == 1 {
		result["value"] = resultRows[0][columns[0]]
	}

	return result, nil
}

// DuckDBHelper provides utility functions for working with DuckDB
type DuckDBHelper struct {
	db *sql.DB
}

// NewDuckDBHelper creates a new DuckDB helper
func NewDuckDBHelper(dbPath string) (*DuckDBHelper, error) {
	// Open DuckDB connection
	// Use ":memory:" for in-memory database or provide a file path
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	// Set connection parameters for optimal performance
	_, err = db.Exec(`
		SET threads TO 4;
		SET memory_limit = '1GB';
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure DuckDB: %w", err)
	}

	return &DuckDBHelper{db: db}, nil
}

// GetDB returns the underlying database connection
func (h *DuckDBHelper) GetDB() *sql.DB {
	return h.db
}

// Close closes the DuckDB connection
func (h *DuckDBHelper) Close() error {
	return h.db.Close()
}

// CreateTableFromCSV creates a DuckDB table from a CSV file
func (h *DuckDBHelper) CreateTableFromCSV(ctx context.Context, tableName, csvPath string) error {
	query := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s')", tableName, csvPath)
	_, err := h.db.ExecContext(ctx, query)
	return err
}

// CreateTableFromParquet creates a DuckDB table from a Parquet file
func (h *DuckDBHelper) CreateTableFromParquet(ctx context.Context, tableName, parquetPath string) error {
	query := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", tableName, parquetPath)
	_, err := h.db.ExecContext(ctx, query)
	return err
}

// CreateTableFromJSON creates a DuckDB table from a JSON file
func (h *DuckDBHelper) CreateTableFromJSON(ctx context.Context, tableName, jsonPath string) error {
	query := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_json_auto('%s')", tableName, jsonPath)
	_, err := h.db.ExecContext(ctx, query)
	return err
}

// QueryAggregation executes an aggregation query and returns a data source
func (h *DuckDBHelper) QueryAggregation(name, query string, args ...interface{}) *DuckDBSource {
	return NewDuckDBSource(name, h.db, query, args...)
}

// QuickStats generates quick statistics for a table
func (h *DuckDBHelper) QuickStats(ctx context.Context, tableName string) (map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s", tableName)

	var rowCount int64
	err := h.db.QueryRowContext(ctx, query).Scan(&rowCount)
	if err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"table":     tableName,
		"row_count": rowCount,
	}, nil
}

// TopN returns the top N rows from a table ordered by a column
func (h *DuckDBHelper) TopN(name, tableName, orderBy string, limit int) *DuckDBSource {
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s DESC LIMIT %d", tableName, orderBy, limit)
	return NewDuckDBSource(name, h.db, query)
}

// GroupByCount performs a GROUP BY with COUNT aggregation
func (h *DuckDBHelper) GroupByCount(name, tableName, groupByCol string, limit int) *DuckDBSource {
	query := fmt.Sprintf(
		"SELECT %s, COUNT(*) as count FROM %s GROUP BY %s ORDER BY count DESC LIMIT %d",
		groupByCol, tableName, groupByCol, limit,
	)
	return NewDuckDBSource(name, h.db, query)
}

// TimeSeriesAggregation performs time-based aggregations
func (h *DuckDBHelper) TimeSeriesAggregation(name, tableName, timeCol, aggCol, interval string) *DuckDBSource {
	query := fmt.Sprintf(
		"SELECT DATE_TRUNC('%s', %s) as period, COUNT(*) as count, AVG(%s) as avg_value, MIN(%s) as min_value, MAX(%s) as max_value FROM %s GROUP BY period ORDER BY period DESC",
		interval, timeCol, aggCol, aggCol, aggCol, tableName,
	)
	return NewDuckDBSource(name, h.db, query)
}
