package dbpool

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// QueryBuilder provides a fluent interface for building SQL queries
type QueryBuilder struct {
	pool      *DBPool
	queryType string
	table     string
	columns   []string
	where     []string
	whereArgs []interface{}
	orderBy   []string
	groupBy   []string
	having    string
	limit     int
	offset    int
	joins     []string
	values    []interface{}
}

// NewQueryBuilder creates a new query builder
func NewQueryBuilder(pool *DBPool) *QueryBuilder {
	return &QueryBuilder{
		pool: pool,
	}
}

// Select starts a SELECT query
func (qb *QueryBuilder) Select(columns ...string) *QueryBuilder {
	qb.queryType = "SELECT"
	qb.columns = columns
	return qb
}

// Insert starts an INSERT query
func (qb *QueryBuilder) Insert(table string) *QueryBuilder {
	qb.queryType = "INSERT"
	qb.table = table
	return qb
}

// Update starts an UPDATE query
func (qb *QueryBuilder) Update(table string) *QueryBuilder {
	qb.queryType = "UPDATE"
	qb.table = table
	return qb
}

// Delete starts a DELETE query
func (qb *QueryBuilder) Delete(table string) *QueryBuilder {
	qb.queryType = "DELETE"
	qb.table = table
	return qb
}

// From specifies the table for SELECT
func (qb *QueryBuilder) From(table string) *QueryBuilder {
	qb.table = table
	return qb
}

// Where adds a WHERE condition
func (qb *QueryBuilder) Where(condition string, args ...interface{}) *QueryBuilder {
	qb.where = append(qb.where, condition)
	qb.whereArgs = append(qb.whereArgs, args...)
	return qb
}

// OrderBy adds an ORDER BY clause
func (qb *QueryBuilder) OrderBy(columns ...string) *QueryBuilder {
	qb.orderBy = append(qb.orderBy, columns...)
	return qb
}

// GroupBy adds a GROUP BY clause
func (qb *QueryBuilder) GroupBy(columns ...string) *QueryBuilder {
	qb.groupBy = append(qb.groupBy, columns...)
	return qb
}

// Having adds a HAVING clause
func (qb *QueryBuilder) Having(condition string) *QueryBuilder {
	qb.having = condition
	return qb
}

// Limit sets the LIMIT
func (qb *QueryBuilder) Limit(limit int) *QueryBuilder {
	qb.limit = limit
	return qb
}

// Offset sets the OFFSET
func (qb *QueryBuilder) Offset(offset int) *QueryBuilder {
	qb.offset = offset
	return qb
}

// Join adds a JOIN clause
func (qb *QueryBuilder) Join(join string) *QueryBuilder {
	qb.joins = append(qb.joins, join)
	return qb
}

// Values sets values for INSERT
func (qb *QueryBuilder) Values(values ...interface{}) *QueryBuilder {
	qb.values = values
	return qb
}

// Set adds a column = value for UPDATE
func (qb *QueryBuilder) Set(column string, value interface{}) *QueryBuilder {
	qb.columns = append(qb.columns, column)
	qb.values = append(qb.values, value)
	return qb
}

// Build constructs the SQL query
func (qb *QueryBuilder) Build() (string, []interface{}) {
	var query strings.Builder
	var args []interface{}

	switch qb.queryType {
	case "SELECT":
		query.WriteString("SELECT ")
		if len(qb.columns) == 0 {
			query.WriteString("*")
		} else {
			query.WriteString(strings.Join(qb.columns, ", "))
		}
		query.WriteString(" FROM ")
		query.WriteString(qb.table)

		// Add JOINs
		for _, join := range qb.joins {
			query.WriteString(" ")
			query.WriteString(join)
		}

		// Add WHERE
		if len(qb.where) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(qb.where, " AND "))
			args = append(args, qb.whereArgs...)
		}

		// Add GROUP BY
		if len(qb.groupBy) > 0 {
			query.WriteString(" GROUP BY ")
			query.WriteString(strings.Join(qb.groupBy, ", "))
		}

		// Add HAVING
		if qb.having != "" {
			query.WriteString(" HAVING ")
			query.WriteString(qb.having)
		}

		// Add ORDER BY
		if len(qb.orderBy) > 0 {
			query.WriteString(" ORDER BY ")
			query.WriteString(strings.Join(qb.orderBy, ", "))
		}

		// Add LIMIT
		if qb.limit > 0 {
			query.WriteString(fmt.Sprintf(" LIMIT %d", qb.limit))
		}

		// Add OFFSET
		if qb.offset > 0 {
			query.WriteString(fmt.Sprintf(" OFFSET %d", qb.offset))
		}

	case "INSERT":
		query.WriteString("INSERT INTO ")
		query.WriteString(qb.table)

		if len(qb.columns) > 0 {
			query.WriteString(" (")
			query.WriteString(strings.Join(qb.columns, ", "))
			query.WriteString(") VALUES (")

			placeholders := make([]string, len(qb.values))
			for i := range qb.values {
				placeholders[i] = "?"
			}
			query.WriteString(strings.Join(placeholders, ", "))
			query.WriteString(")")
			args = qb.values
		}

	case "UPDATE":
		query.WriteString("UPDATE ")
		query.WriteString(qb.table)
		query.WriteString(" SET ")

		setClauses := make([]string, len(qb.columns))
		for i, col := range qb.columns {
			setClauses[i] = col + " = ?"
		}
		query.WriteString(strings.Join(setClauses, ", "))
		args = append(args, qb.values...)

		if len(qb.where) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(qb.where, " AND "))
			args = append(args, qb.whereArgs...)
		}

	case "DELETE":
		query.WriteString("DELETE FROM ")
		query.WriteString(qb.table)

		if len(qb.where) > 0 {
			query.WriteString(" WHERE ")
			query.WriteString(strings.Join(qb.where, " AND "))
			args = qb.whereArgs
		}
	}

	return query.String(), args
}

// Exec executes the query
func (qb *QueryBuilder) Exec(ctx context.Context) (sql.Result, error) {
	query, args := qb.Build()
	return qb.pool.ExecContext(ctx, query, args...)
}

// Query executes the query and returns rows
func (qb *QueryBuilder) Query(ctx context.Context) (*sql.Rows, error) {
	query, args := qb.Build()
	return qb.pool.QueryContext(ctx, query, args...)
}

// QueryRow executes the query and returns a single row
func (qb *QueryBuilder) QueryRow(ctx context.Context) *sql.Row {
	query, args := qb.Build()
	return qb.pool.QueryRowContext(ctx, query, args...)
}

// Scan executes the query and scans into dest
func (qb *QueryBuilder) Scan(ctx context.Context, dest ...interface{}) error {
	query, args := qb.Build()
	return qb.pool.QueryRowContext(ctx, query, args...).Scan(dest...)
}

// Count returns the count of matching rows
func (qb *QueryBuilder) Count(ctx context.Context) (int64, error) {
	originalColumns := qb.columns
	qb.columns = []string{"COUNT(*)"}

	var count int64
	err := qb.Scan(ctx, &count)

	qb.columns = originalColumns
	return count, err
}

// Exists checks if any rows match the query
func (qb *QueryBuilder) Exists(ctx context.Context) (bool, error) {
	count, err := qb.Count(ctx)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}
