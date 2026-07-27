package listen

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxListener holds one connection out of a pool for the listener's lifetime.
//
// A listening connection cannot be shared: it is blocked inside WaitForNotification and
// is unusable for queries. Dedicating one connection is the cost of LISTEN, and it is why
// a pool sized N leaves N-1 connections for actual work.
type pgxListener struct {
	conn *pgxpool.Conn

	closeOnce sync.Once
	closed    bool
}

// NewPgx dedicates a connection from pool to receiving notifications. Close returns it.
func NewPgx(ctx context.Context, pool *pgxpool.Pool) (Listener, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("listen: acquire connection: %w", err)
	}

	return &pgxListener{conn: conn}, nil
}

func (l *pgxListener) Listen(ctx context.Context, channel string) error {
	if l.closed {
		return ErrClosed
	}

	// LISTEN takes an identifier, not a bind parameter, so the channel name is quoted
	// rather than parameterised.
	stmt := "LISTEN " + pgx.Identifier{channel}.Sanitize()
	if _, err := l.conn.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("listen on %q: %w", channel, err)
	}

	return nil
}

func (l *pgxListener) Wait(ctx context.Context) (*Notification, error) {
	if l.closed {
		return nil, ErrClosed
	}

	n, err := l.conn.Conn().WaitForNotification(ctx)
	if err != nil {
		return nil, fmt.Errorf("wait for notification: %w", err)
	}

	return &Notification{Channel: n.Channel, Payload: n.Payload, PID: n.PID}, nil
}

func (l *pgxListener) Close() error {
	l.closeOnce.Do(func() {
		l.closed = true
		l.conn.Release()
	})

	return nil
}
