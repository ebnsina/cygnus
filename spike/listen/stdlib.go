package listen

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// stdlibListener delivers real LISTEN/NOTIFY over a database/sql handle.
//
// This is the interesting one. database/sql has no notification primitive, which is why
// comparable libraries downgrade database/sql users to polling. The way through is
// (*sql.Conn).Raw, which hands back the driver's own connection object; for pgx/stdlib
// that is a *stdlib.Conn, whose Conn method yields the *pgx.Conn underneath. From there
// the full pgx notification API is available.
//
// Two structural consequences follow, and both are handled below:
//
//   - The connection must be pinned. sql.DB hands out arbitrary pooled connections, but
//     LISTEN is connection-scoped, so a notification would be delivered to whichever
//     connection happened to issue the LISTEN. db.Conn pins exactly one.
//   - Raw runs its callback synchronously and holds the connection for its duration.
//     Blocking inside it forever is precisely what a listener wants, so the wait loop
//     lives inside the callback and hands notifications out over a channel.
type stdlibListener struct {
	conn *sql.Conn

	notifications chan *Notification
	failure       chan error

	startOnce sync.Once
	closeOnce sync.Once
	cancel    context.CancelFunc
	stopped   chan struct{}
	closed    bool
}

// NewStdlib pins a connection from db and prepares it to receive notifications. db must
// be backed by the pgx/stdlib driver; for lib/pq, use NewPq instead.
func NewStdlib(ctx context.Context, db *sql.DB) (Listener, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("listen: pin connection: %w", err)
	}

	// Fail fast on an incompatible driver rather than at the first Wait, so the caller
	// can fall back to polling before it has committed to this path.
	if err := conn.Raw(func(driverConn any) error {
		_, err := pgxConnFrom(driverConn)
		return err
	}); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return &stdlibListener{
		conn: conn,
		// Buffered so a burst of notifications does not block the pump while the
		// consumer is busy working the jobs the last notification announced.
		notifications: make(chan *Notification, 64),
		failure:       make(chan error, 1),
		stopped:       make(chan struct{}),
	}, nil
}

// pgxConnFrom extracts the *pgx.Conn underlying a database/sql driver connection.
func pgxConnFrom(driverConn any) (*pgx.Conn, error) {
	c, ok := driverConn.(*stdlib.Conn)
	if !ok {
		return nil, fmt.Errorf("%w: got %T, want *stdlib.Conn", ErrUnsupportedDriver, driverConn)
	}
	return c.Conn(), nil
}

func (l *stdlibListener) Listen(ctx context.Context, channel string) error {
	if l.closed {
		return ErrClosed
	}

	// A short-lived Raw call, valid only because the wait loop has not started yet and
	// the pinned connection is therefore idle.
	err := l.conn.Raw(func(driverConn any) error {
		conn, err := pgxConnFrom(driverConn)
		if err != nil {
			return err
		}
		_, err = conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize())
		return err
	})
	if err != nil {
		return fmt.Errorf("listen on %q: %w", channel, err)
	}

	return nil
}

// start launches the wait loop. It runs at most once, on the first Wait.
func (l *stdlibListener) start() {
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel

	go func() {
		defer close(l.stopped)

		// Raw blocks here for the listener's whole lifetime.
		err := l.conn.Raw(func(driverConn any) error {
			conn, err := pgxConnFrom(driverConn)
			if err != nil {
				return err
			}

			for {
				n, err := conn.WaitForNotification(ctx)
				if err != nil {
					return err
				}

				select {
				case l.notifications <- &Notification{
					Channel: n.Channel,
					Payload: n.Payload,
					PID:     n.PID,
				}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})

		// Non-blocking: nobody may be waiting, and the buffer only needs the first error.
		select {
		case l.failure <- err:
		default:
		}
	}()
}

func (l *stdlibListener) Wait(ctx context.Context) (*Notification, error) {
	if l.closed {
		return nil, ErrClosed
	}
	l.startOnce.Do(l.start)

	select {
	case n := <-l.notifications:
		return n, nil
	case err := <-l.failure:
		if err == nil {
			return nil, ErrClosed
		}
		return nil, fmt.Errorf("wait for notification: %w", err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *stdlibListener) Close() error {
	l.closeOnce.Do(func() {
		l.closed = true

		if l.cancel != nil {
			l.cancel()
			// Let the wait loop leave Raw before returning the connection; closing it
			// underneath an in-flight Raw callback is not safe.
			<-l.stopped
		}

		_ = l.conn.Close()
	})

	return nil
}
