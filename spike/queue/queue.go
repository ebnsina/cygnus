// Package queue implements the spike's storage operations against PostgreSQL.
//
// This is deliberately not the eventual public API. It is the narrowest code that can
// answer the Phase 0 question: does a SKIP LOCKED fetch loop sustain the throughput and
// tail latency the design assumes, at realistic table depth?
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NotifyChannel carries an announcement that new work is available.
const NotifyChannel = "cygnus_insert"

// Queue reads and writes jobs.
type Queue struct {
	pool *pgxpool.Pool
}

// New returns a Queue backed by pool. The pool is owned by the caller.
func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// Job is a leased unit of work.
type Job struct {
	ID          int64
	Kind        string
	Queue       string
	Priority    int16
	Args        []byte
	Attempt     int16
	MaxAttempts int16
	CreatedAt   time.Time
	AttemptedAt time.Time
}

// InsertParams describes a job to enqueue.
type InsertParams struct {
	Kind        string
	Queue       string
	Priority    int16
	Args        string // JSON; empty is treated as "{}"
	MaxAttempts int16
}

func (p *InsertParams) withDefaults() {
	if p.Queue == "" {
		p.Queue = "default"
	}
	if p.Priority == 0 {
		p.Priority = 1
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 25
	}
	if p.Args == "" {
		p.Args = "{}"
	}
}

// copyBatchSize bounds how many rows are materialised per COPY round trip. Large enough
// that per-batch overhead is negligible, small enough that seeding ten million jobs does
// not hold the whole set in memory.
const copyBatchSize = 10_000

var copyColumns = []string{"kind", "queue", "priority", "args", "max_attempts"}

// Insert enqueues jobs using COPY, which is materially faster than multi-row INSERT for
// the volumes the benchmark needs. It returns the number of rows written.
//
// State, timestamps, and identity are all left to their column defaults, so every
// timestamp originates from the database rather than from this process.
func (q *Queue) Insert(ctx context.Context, params []InsertParams) (int64, error) {
	var total int64

	for start := 0; start < len(params); start += copyBatchSize {
		end := min(start+copyBatchSize, len(params))

		batch := params[start:end]
		rows := make([][]any, len(batch))
		for i := range batch {
			batch[i].withDefaults()
			rows[i] = []any{
				batch[i].Kind,
				batch[i].Queue,
				batch[i].Priority,
				batch[i].Args,
				batch[i].MaxAttempts,
			}
		}

		n, err := q.pool.CopyFrom(ctx, pgx.Identifier{"cygnus_job"}, copyColumns, pgx.CopyFromRows(rows))
		if err != nil {
			return total, fmt.Errorf("copy jobs: %w", err)
		}
		total += n
	}

	return total, nil
}

// FetchSQL leases the next batch of available jobs.
//
// The mechanics that matter:
//
//   - FOR UPDATE SKIP LOCKED lets concurrent producers pass over rows another producer
//     has already locked. Without it, every worker serialises on the same head-of-queue
//     rows and throughput does not scale with worker count.
//   - The ORDER BY must match cygnus_job_fetch_idx column-for-column, or PostgreSQL adds
//     a sort node. `spike explain` asserts this rather than trusting it.
//   - Leasing and returning happen in one statement, so a batch costs one round trip.
//   - The lease interval is passed as seconds to make_interval rather than relying on any
//     particular Go-duration-to-interval mapping in the driver.
const FetchSQL = `
WITH locked AS (
    SELECT id
    FROM cygnus_job
    WHERE state = 'available'
      AND queue = $1
      AND scheduled_at <= now()
    ORDER BY priority, scheduled_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE cygnus_job AS j
SET state            = 'running',
    attempt          = j.attempt + 1,
    attempted_at     = now(),
    lease_expires_at = now() + make_interval(secs => $3),
    attempted_by     = array_append(j.attempted_by, $4)
FROM locked
WHERE j.id = locked.id
RETURNING j.id, j.kind, j.queue, j.priority, j.args, j.attempt, j.max_attempts,
          j.created_at, j.attempted_at
`

// FetchParams controls a single fetch.
type FetchParams struct {
	Queue    string
	Max      int32
	Lease    time.Duration
	ClientID string
}

// Fetch leases up to Max available jobs, marking them running. An empty result is not an
// error; it means the queue is drained.
func (q *Queue) Fetch(ctx context.Context, params FetchParams) ([]Job, error) {
	rows, err := q.pool.Query(ctx, FetchSQL,
		params.Queue, params.Max, params.Lease.Seconds(), params.ClientID)
	if err != nil {
		return nil, fmt.Errorf("fetch jobs: %w", err)
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.ID, &j.Kind, &j.Queue, &j.Priority, &j.Args,
			&j.Attempt, &j.MaxAttempts, &j.CreatedAt, &j.AttemptedAt); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate jobs: %w", err)
	}

	return jobs, nil
}

// Complete finalises jobs as succeeded. Restricting the update to rows still in 'running'
// means a job whose lease already expired and was rescued is not silently resurrected.
func (q *Queue) Complete(ctx context.Context, ids []int64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	const stmt = `
		UPDATE cygnus_job
		SET state = 'completed', finalized_at = now(), lease_expires_at = NULL
		WHERE id = ANY($1) AND state = 'running'
	`
	tag, err := q.pool.Exec(ctx, stmt, ids)
	if err != nil {
		return 0, fmt.Errorf("complete jobs: %w", err)
	}

	return tag.RowsAffected(), nil
}

// Notify announces that work is available, waking listeners.
func (q *Queue) Notify(ctx context.Context, payload string) error {
	if _, err := q.pool.Exec(ctx, "SELECT pg_notify($1, $2)", NotifyChannel, payload); err != nil {
		return fmt.Errorf("notify: %w", err)
	}
	return nil
}

// Counts holds a per-state census of the job table.
type Counts map[string]int64

// Counts returns the number of jobs in each state.
func (q *Queue) Counts(ctx context.Context) (Counts, error) {
	rows, err := q.pool.Query(ctx, `SELECT state::text, count(*) FROM cygnus_job GROUP BY 1`)
	if err != nil {
		return nil, fmt.Errorf("count jobs: %w", err)
	}
	defer rows.Close()

	counts := make(Counts)
	for rows.Next() {
		var state string
		var n int64
		if err := rows.Scan(&state, &n); err != nil {
			return nil, fmt.Errorf("scan count: %w", err)
		}
		counts[state] = n
	}

	return counts, rows.Err()
}

// Truncate empties the job table. Test and benchmark support only.
func (q *Queue) Truncate(ctx context.Context) error {
	if _, err := q.pool.Exec(ctx, "TRUNCATE cygnus_job RESTART IDENTITY"); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	return nil
}

// ErrNoRows reports that a lookup found nothing.
var ErrNoRows = errors.New("cygnus: no rows")

// Get returns a single job by ID, for assertions in tests.
func (q *Queue) Get(ctx context.Context, id int64) (Job, string, error) {
	const stmt = `
		SELECT id, kind, queue, priority, args, attempt, max_attempts,
		       created_at, coalesce(attempted_at, created_at), state::text
		FROM cygnus_job WHERE id = $1
	`
	var j Job
	var state string
	err := q.pool.QueryRow(ctx, stmt, id).Scan(&j.ID, &j.Kind, &j.Queue, &j.Priority,
		&j.Args, &j.Attempt, &j.MaxAttempts, &j.CreatedAt, &j.AttemptedAt, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, "", ErrNoRows
	}
	if err != nil {
		return Job{}, "", fmt.Errorf("get job %d: %w", id, err)
	}

	return j, state, nil
}
