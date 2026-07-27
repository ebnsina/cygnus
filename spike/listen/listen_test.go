package listen_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	_ "github.com/lib/pq"              // registers the "postgres" database/sql driver

	"github.com/ebnsina/cygnus/spike/listen"
)

func dsn(t *testing.T) string {
	t.Helper()

	v := os.Getenv("CYGNUS_DATABASE_URL")
	if v == "" {
		t.Skip("CYGNUS_DATABASE_URL not set; run `make db-up` to enable database tests")
	}

	return v
}

// notifier returns a function that issues NOTIFY over its own pool, so notifications
// always originate from a different connection than the one listening.
func notifier(t *testing.T, dsn string) func(channel, payload string) {
	t.Helper()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect notifier: %v", err)
	}
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("database unreachable: %v", err)
	}

	return func(channel, payload string) {
		t.Helper()
		if _, err := pool.Exec(ctx, "SELECT pg_notify($1, $2)", channel, payload); err != nil {
			t.Fatalf("notify %q: %v", channel, err)
		}
	}
}

// newListener constructs the backend under test, along with whatever handle it needs.
func newListener(t *testing.T, backend listen.Backend, dsn string) listen.Listener {
	t.Helper()

	ctx := context.Background()

	switch backend {
	case listen.BackendPgx:
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			t.Fatalf("pgxpool: %v", err)
		}
		t.Cleanup(pool.Close)

		l, err := listen.NewPgx(ctx, pool)
		if err != nil {
			t.Fatalf("NewPgx: %v", err)
		}
		return l

	case listen.BackendStdlib:
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			t.Fatalf("sql.Open pgx: %v", err)
		}
		t.Cleanup(func() { _ = db.Close() })

		l, err := listen.NewStdlib(ctx, db)
		if err != nil {
			t.Fatalf("NewStdlib: %v", err)
		}
		return l

	case listen.BackendPq:
		l, err := listen.NewPq(dsn)
		if err != nil {
			t.Fatalf("NewPq: %v", err)
		}
		return l

	default:
		t.Fatalf("unknown backend %q", backend)
		return nil
	}
}

// TestBackendsDeliverNotifications is the Phase 0 gate for the project's positioning.
//
// Every supported driver, including the two database/sql ones, must receive a real
// notification. If the database/sql backends cannot, the "first-class database/sql
// support" claim is false and the plan needs revisiting before any more is built on it.
func TestBackendsDeliverNotifications(t *testing.T) {
	url := dsn(t)
	notify := notifier(t, url)

	for _, backend := range listen.Backends {
		t.Run(string(backend), func(t *testing.T) {
			channel := fmt.Sprintf("cygnus_test_%s", backend)
			payload := fmt.Sprintf("payload-for-%s", backend)

			l := newListener(t, backend, url)
			t.Cleanup(func() { _ = l.Close() })

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := l.Listen(ctx, channel); err != nil {
				t.Fatalf("Listen: %v", err)
			}

			// lib/pq establishes its connection asynchronously, so a NOTIFY sent
			// immediately can land before the listener is attached. Retry until the
			// notification arrives or the context expires; a delivered-at-least-once
			// notification is what is under test, not the first attempt specifically.
			var (
				received *listen.Notification
				sent     time.Time
			)
			for received == nil {
				if ctx.Err() != nil {
					t.Fatalf("no notification within the timeout")
				}

				sent = time.Now()
				notify(channel, payload)

				attemptCtx, attemptCancel := context.WithTimeout(ctx, time.Second)
				n, err := l.Wait(attemptCtx)
				attemptCancel()

				switch {
				case err == nil:
					received = n
				case ctx.Err() != nil:
					t.Fatalf("no notification within the timeout")
				case attemptCtx.Err() != nil:
					continue // retry
				default:
					t.Fatalf("Wait: %v", err)
				}
			}

			latency := time.Since(sent)

			if received.Channel != channel {
				t.Errorf("channel = %q, want %q", received.Channel, channel)
			}
			if received.Payload != payload {
				t.Errorf("payload = %q, want %q", received.Payload, payload)
			}
			if received.PID == 0 {
				t.Errorf("PID = 0, want the notifying backend's process id")
			}

			t.Logf("%s: delivered in %v", backend, latency.Round(time.Microsecond))
		})
	}
}

// TestStdlibRejectsUnsupportedDriver checks the graceful-degradation path. A database/sql
// handle whose driver cannot yield a PostgreSQL connection must fail immediately and
// legibly, so the caller can choose polling rather than silently receiving nothing.
func TestStdlibRejectsUnsupportedDriver(t *testing.T) {
	url := dsn(t)

	// lib/pq is a perfectly good driver, but its connections are not *stdlib.Conn, so
	// the pgx extraction path must reject it rather than misbehave.
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatalf("sql.Open postgres: %v", err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Ping(); err != nil {
		t.Skipf("database unreachable: %v", err)
	}

	l, err := listen.NewStdlib(context.Background(), db)
	if err == nil {
		_ = l.Close()
		t.Fatal("NewStdlib accepted a lib/pq handle, want ErrUnsupportedDriver")
	}
	if !errors.Is(err, listen.ErrUnsupportedDriver) {
		t.Fatalf("error = %v, want ErrUnsupportedDriver", err)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	url := dsn(t)

	for _, backend := range listen.Backends {
		t.Run(string(backend), func(t *testing.T) {
			l := newListener(t, backend, url)

			if err := l.Close(); err != nil {
				t.Fatalf("first Close: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}

			if _, err := l.Wait(context.Background()); !errors.Is(err, listen.ErrClosed) {
				t.Errorf("Wait after Close = %v, want ErrClosed", err)
			}
		})
	}
}

func TestBackendValid(t *testing.T) {
	for _, b := range listen.Backends {
		if !b.Valid() {
			t.Errorf("%q reported invalid", b)
		}
	}
	if listen.Backend("redis").Valid() {
		t.Error(`"redis" reported valid`)
	}
}
