// Package listen provides LISTEN/NOTIFY across the PostgreSQL drivers Cygnus intends to
// support.
//
// This package exists to answer the single most load-bearing question in the project's
// positioning: can a database/sql user get real LISTEN/NOTIFY, rather than the degraded
// polling that comparable libraries fall back to?
//
// The answer is yes, and it takes two different mechanisms, because the drivers expose
// notifications in structurally different ways:
//
//   - pgx/v5 and pgx/stdlib block on Conn.WaitForNotification, so a caller drives them.
//   - lib/pq inverts control: pq.NewListener owns its own connection, reconnects on its
//     own, and delivers notifications on a channel.
//
// The Listener interface below hides that difference behind a single blocking Wait, which
// is the shape the eventual driver interface needs.
package listen

import (
	"context"
	"errors"
	"slices"
	"time"
)

// Notification is a message received on a channel.
type Notification struct {
	Channel string
	Payload string

	// PID identifies the backend that issued the NOTIFY. It is zero when the driver does
	// not report it.
	PID uint32
}

// Listener receives notifications on one or more channels.
//
// Implementations are not safe for concurrent use by multiple goroutines. Each is
// intended to be driven by a single loop, which is how a producer consumes one.
type Listener interface {
	// Listen subscribes to a channel. It may be called more than once, but every call
	// must happen before the first Wait.
	//
	// That ordering is not incidental. A listening connection is blocked inside the
	// driver's wait call and cannot issue LISTEN at the same time, so subscriptions are
	// established while the connection is still idle. A producer knows its channels up
	// front, so this costs nothing in practice and avoids interleaving that would
	// otherwise risk poisoning the connection.
	Listen(ctx context.Context, channel string) error

	// Wait blocks until a notification arrives, the context is cancelled, or the
	// connection fails. It returns ErrClosed once the Listener has been closed.
	Wait(ctx context.Context) (*Notification, error)

	// Close releases the underlying connection. It is safe to call more than once.
	Close() error
}

// ErrClosed is returned by Wait after the Listener has been closed.
var ErrClosed = errors.New("listen: listener is closed")

// ErrUnsupportedDriver is returned when a *sql.DB is backed by a driver this package
// cannot obtain a raw PostgreSQL connection from. Callers are expected to fall back to
// polling and to say so, rather than silently degrading.
var ErrUnsupportedDriver = errors.New("listen: driver does not expose a usable PostgreSQL connection")

// Backend names a Listener implementation.
type Backend string

const (
	// BackendPgx uses a connection acquired from a pgxpool.Pool.
	BackendPgx Backend = "pgx"

	// BackendStdlib uses a database/sql handle whose driver is pgx/stdlib.
	BackendStdlib Backend = "stdlib"

	// BackendPq uses a database/sql handle whose driver is lib/pq.
	BackendPq Backend = "pq"
)

// Backends lists every supported backend, in the order the CLI exercises them.
var Backends = []Backend{BackendPgx, BackendStdlib, BackendPq}

// Valid reports whether b names a known backend.
func (b Backend) Valid() bool {
	return slices.Contains(Backends, b)
}

// defaultReconnect bounds lib/pq's reconnection backoff. The other backends surface
// connection loss to the caller instead, since a producer already has a supervision loop.
const (
	defaultMinReconnect = time.Second
	defaultMaxReconnect = time.Minute
)
