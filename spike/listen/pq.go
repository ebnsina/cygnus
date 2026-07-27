package listen

import (
	"context"
	"fmt"
	"sync"

	"github.com/lib/pq"
)

// pqListener adapts lib/pq's notification support to the Listener interface.
//
// lib/pq inverts control relative to pgx: pq.NewListener owns its connection, dials it
// itself from a DSN, reconnects with backoff on failure, and pushes notifications onto a
// channel. There is no wait call to drive, so Wait here is a receive rather than a block
// on the driver.
//
// The reconnect handling is the reason to prefer this over driving lib/pq manually: a nil
// value on the channel signals that the connection dropped and was re-established, and
// pq re-issues every LISTEN automatically. Cygnus treats that as a cue to re-check the
// queue, since notifications sent while disconnected were missed.
type pqListener struct {
	listener *pq.Listener

	closeOnce sync.Once
	closed    bool
}

// NewPq creates a listener over its own connection to dsn. Unlike the other backends it
// takes a connection string rather than an existing handle, because pq.Listener manages
// its connection itself.
func NewPq(dsn string) (Listener, error) {
	// The event callback is required but intentionally quiet here: connection state is
	// surfaced to the caller through Wait, and the spike has no logger to route it to.
	l := pq.NewListener(dsn, defaultMinReconnect, defaultMaxReconnect,
		func(pq.ListenerEventType, error) {})

	return &pqListener{listener: l}, nil
}

func (l *pqListener) Listen(_ context.Context, channel string) error {
	if l.closed {
		return ErrClosed
	}

	// pq quotes the channel name itself.
	if err := l.listener.Listen(channel); err != nil {
		return fmt.Errorf("listen on %q: %w", channel, err)
	}

	return nil
}

func (l *pqListener) Wait(ctx context.Context) (*Notification, error) {
	if l.closed {
		return nil, ErrClosed
	}

	for {
		select {
		case n, ok := <-l.listener.Notify:
			if !ok {
				return nil, ErrClosed
			}
			if n == nil {
				// Reconnected. Notifications sent while the connection was down were
				// missed, so keep waiting; the caller's periodic sweep is what actually
				// covers the gap.
				continue
			}

			return &Notification{
				Channel: n.Channel,
				Payload: n.Extra,
				PID:     uint32(n.BePid),
			}, nil

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (l *pqListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.closed = true
		err = l.listener.Close()
	})

	return err
}
