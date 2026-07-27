// Package schema embeds and applies the spike's database schema.
//
// The spike uses a single idempotent schema file rather than a versioned migration
// chain. Versioned migrations arrive with the real driver; here, re-applying the file
// is always safe and always sufficient.
package schema

import (
	"context"
	_ "embed"
	"fmt"
)

//go:embed schema.sql
var sql string

// SQL returns the schema definition.
func SQL() string { return sql }

// ExecFunc runs a SQL statement. Taking a function rather than a driver interface keeps
// this package free of any third-party dependency, per the core module's constraint.
type ExecFunc func(ctx context.Context, sql string) error

// TableName is the spike's only table.
const TableName = "cygnus_job"

// FetchIndexName is the partial index the fetch query must use. A plan that does not
// reference it means the hot path has regressed to a sequential scan.
const FetchIndexName = "cygnus_job_fetch_idx"

// Apply creates the schema if it is absent. It is safe to call repeatedly.
func Apply(ctx context.Context, exec ExecFunc) error {
	if err := exec(ctx, sql); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// Drop removes everything the schema created. Intended for tests and `db-reset`; it is
// destructive and deliberately not wired into any default command path.
func Drop(ctx context.Context, exec ExecFunc) error {
	const stmt = `
		DROP TABLE IF EXISTS cygnus_job;
		DROP TYPE IF EXISTS cygnus_job_state;
	`
	if err := exec(ctx, stmt); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}
	return nil
}
