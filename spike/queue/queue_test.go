package queue_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ebnsina/cygnus/spike/queue"
	"github.com/ebnsina/cygnus/spike/schema"
)

// newPool connects to the development database, skipping the test when none is
// configured. There are no database mocks in this project: the behaviour under test is
// PostgreSQL's row locking, which only PostgreSQL can exhibit.
func newPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("CYGNUS_DATABASE_URL")
	if dsn == "" {
		t.Skip("CYGNUS_DATABASE_URL not set; run `make db-up` to enable database tests")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("database unreachable at %s: %v", dsn, err)
	}

	apply := func(ctx context.Context, sql string) error {
		_, err := pool.Exec(ctx, sql)
		return err
	}
	if err := schema.Apply(ctx, apply); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	return pool
}

// newQueue returns a Queue over an empty job table.
func newQueue(t *testing.T) *queue.Queue {
	t.Helper()

	q := queue.New(newPool(t))
	if err := q.Truncate(context.Background()); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	return q
}

func seed(t *testing.T, q *queue.Queue, n int) {
	t.Helper()

	params := make([]queue.InsertParams, n)
	for i := range params {
		params[i] = queue.InsertParams{
			Kind: "test_job",
			Args: fmt.Sprintf(`{"n":%d}`, i),
		}
	}

	written, err := q.Insert(context.Background(), params)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if written != int64(n) {
		t.Fatalf("inserted %d jobs, want %d", written, n)
	}
}

func TestInsertAndFetch(t *testing.T) {
	ctx := context.Background()
	q := newQueue(t)
	seed(t, q, 10)

	jobs, err := q.Fetch(ctx, queue.FetchParams{
		Queue: "default", Max: 4, Lease: time.Minute, ClientID: "test",
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(jobs) != 4 {
		t.Fatalf("fetched %d jobs, want 4", len(jobs))
	}

	for _, j := range jobs {
		if j.Attempt != 1 {
			t.Errorf("job %d has attempt %d, want 1", j.ID, j.Attempt)
		}
		if j.Kind != "test_job" {
			t.Errorf("job %d has kind %q, want %q", j.ID, j.Kind, "test_job")
		}
		if j.AttemptedAt.IsZero() {
			t.Errorf("job %d has no attempted_at", j.ID)
		}
	}

	counts, err := q.Counts(ctx)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts["running"] != 4 {
		t.Errorf("running = %d, want 4", counts["running"])
	}
	if counts["available"] != 6 {
		t.Errorf("available = %d, want 6", counts["available"])
	}
}

func TestFetchOrdersByPriority(t *testing.T) {
	ctx := context.Background()
	q := newQueue(t)

	// Insert in reverse priority order, so returning them in insert order would fail.
	for _, p := range []int16{4, 3, 2, 1} {
		if _, err := q.Insert(ctx, []queue.InsertParams{
			{Kind: "test_job", Priority: p},
		}); err != nil {
			t.Fatalf("insert priority %d: %v", p, err)
		}
	}

	jobs, err := q.Fetch(ctx, queue.FetchParams{
		Queue: "default", Max: 4, Lease: time.Minute, ClientID: "test",
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(jobs) != 4 {
		t.Fatalf("fetched %d jobs, want 4", len(jobs))
	}

	for i, j := range jobs {
		if want := int16(i + 1); j.Priority != want {
			t.Errorf("position %d has priority %d, want %d", i, j.Priority, want)
		}
	}
}

func TestFetchEmptyQueue(t *testing.T) {
	jobs, err := newQueue(t).Fetch(context.Background(), queue.FetchParams{
		Queue: "default", Max: 10, Lease: time.Minute, ClientID: "test",
	})
	if err != nil {
		t.Fatalf("fetch on empty queue returned an error: %v", err)
	}
	if len(jobs) != 0 {
		t.Fatalf("fetched %d jobs from an empty queue, want 0", len(jobs))
	}
}

// TestConcurrentFetchNeverDuplicates is the correctness property the whole design rests
// on. Many workers fetch concurrently from one queue; SKIP LOCKED must ensure every job
// is handed to exactly one of them. A failure here invalidates the storage engine.
func TestConcurrentFetchNeverDuplicates(t *testing.T) {
	const (
		totalJobs = 2_000
		workers   = 16
		batchSize = 20
	)

	ctx := context.Background()
	q := newQueue(t)
	seed(t, q, totalJobs)

	var (
		mu      sync.Mutex
		seen    = make(map[int64]string, totalJobs)
		dupes   []string
		fetched int
	)

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			clientID := fmt.Sprintf("worker-%d", w)
			for {
				jobs, err := q.Fetch(ctx, queue.FetchParams{
					Queue: "default", Max: batchSize, Lease: time.Minute, ClientID: clientID,
				})
				if err != nil {
					t.Errorf("%s fetch: %v", clientID, err)
					return
				}
				if len(jobs) == 0 {
					return // drained
				}

				mu.Lock()
				for _, j := range jobs {
					if prev, ok := seen[j.ID]; ok {
						dupes = append(dupes,
							fmt.Sprintf("job %d fetched by both %s and %s", j.ID, prev, clientID))
					}
					seen[j.ID] = clientID
					fetched++
				}
				mu.Unlock()

				ids := make([]int64, len(jobs))
				for i, j := range jobs {
					ids[i] = j.ID
				}
				if _, err := q.Complete(ctx, ids); err != nil {
					t.Errorf("%s complete: %v", clientID, err)
					return
				}
			}
		})
	}
	wg.Wait()

	if len(dupes) > 0 {
		t.Fatalf("SKIP LOCKED handed the same job to multiple workers (%d cases):\n  %s",
			len(dupes), dupes[0])
	}
	if fetched != totalJobs {
		t.Errorf("fetched %d jobs, want %d", fetched, totalJobs)
	}
	if len(seen) != totalJobs {
		t.Errorf("saw %d distinct jobs, want %d", len(seen), totalJobs)
	}

	counts, err := q.Counts(ctx)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts["completed"] != totalJobs {
		t.Errorf("completed = %d, want %d", counts["completed"], totalJobs)
	}
	if counts["available"] != 0 {
		t.Errorf("available = %d, want 0 after draining", counts["available"])
	}
}

// TestCompleteIgnoresNonRunningJobs guards the rescue path: a job whose lease expired and
// was moved on must not be resurrected by a late Complete from the original worker.
func TestCompleteIgnoresNonRunningJobs(t *testing.T) {
	ctx := context.Background()
	q := newQueue(t)
	seed(t, q, 1)

	jobs, err := q.Fetch(ctx, queue.FetchParams{
		Queue: "default", Max: 1, Lease: time.Minute, ClientID: "test",
	})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("fetched %d jobs, want 1", len(jobs))
	}
	id := jobs[0].ID

	affected, err := q.Complete(ctx, []int64{id})
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if affected != 1 {
		t.Fatalf("first complete affected %d rows, want 1", affected)
	}

	// The job is no longer running, so a second completion must be a no-op.
	affected, err = q.Complete(ctx, []int64{id})
	if err != nil {
		t.Fatalf("second complete: %v", err)
	}
	if affected != 0 {
		t.Errorf("second complete affected %d rows, want 0", affected)
	}
}

func TestCompleteNoIDs(t *testing.T) {
	affected, err := newQueue(t).Complete(context.Background(), nil)
	if err != nil {
		t.Fatalf("complete with no ids: %v", err)
	}
	if affected != 0 {
		t.Errorf("affected = %d, want 0", affected)
	}
}
