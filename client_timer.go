package erio

import (
	"time"

	"github.com/netiogreem/erio/internal"
)

// clientTimerError is the error type returned by [clientTimer] methods.
// It is a string so that each error can be declared as a comparable constant.
type clientTimerError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this clientTimerError) Error() string {
	return string(this)
}

const (
	ErrClientTimerUninitialized clientTimerError = "erio: client registry is uninitialized" // Returned by [clientTimer.Register] when the registry was not created with [newClientTimer].
	ErrClientTimerNilHandler    clientTimerError = "erio: client registry handler is nil"   // Returned by [clientTimer.Register] when handler is nil.
)

// clientTimer tracks one expiration time per [ClientHandler] and reports
// which handlers have expired.
type clientTimer struct {
	registry *internal.TimerRegistry[ClientHandler] // Min-heap of handlers keyed by expiration time, with one entry per handler.
}

// newClientTimer creates a [clientTimer] with an empty [internal.TimerRegistry].
//
// Returns:
//   - The registry with no handler registered.
func newClientTimer() *clientTimer {
	return &clientTimer{
		registry: internal.NewTimerRegistry[ClientHandler](),
	}
}

// Register sets the expiration time of handler, replacing any time registered earlier.
//
// Parameters:
//   - expiresAt is the point in time at which handler expires. A time already in the past
//     makes handler expire on the next [clientTimer.PopExpired] call.
//   - handler is the handler to track and must not be nil.
//
// Returns:
//   - nil when the expiration time was registered.
//   - [ErrClientTimerUninitialized] when the registry was not created with [newClientTimer].
//   - [ErrClientTimerNilHandler] when handler is nil.
//
// Notes:
//   - A handler has at most one expiration time. Registering again moves it to the new time.
func (this *clientTimer) Register(expiresAt time.Time, handler ClientHandler) error {
	if this.registry == nil {
		return ErrClientTimerUninitialized
	}
	if handler == nil {
		return ErrClientTimerNilHandler
	}

	return this.registry.Register(expiresAt, handler)
}

// Unregister removes the expiration time of handler.
//
// Parameters:
//   - handler is the handler whose expiration time is removed.
//
// Returns:
//   - true when handler had a registered expiration time and it was removed.
//   - false when handler was not registered.
func (this *clientTimer) Unregister(handler ClientHandler) bool {
	if this.registry == nil {
		return false
	}
	return this.registry.Unregister(handler)
}

// NextTimeout reports how long until the earliest registered expiration time.
//
// Returns:
//   - remaining is the duration until the earliest expiration time, or 0 when that time
//     has already passed or no handler is registered.
//   - hasTimer is true when at least one handler is registered, false otherwise.
func (this *clientTimer) NextTimeout() (remaining time.Duration, hasTimer bool) {
	if this.registry == nil {
		return 0, false
	}

	expiresAt, hasTimer := this.registry.NextExpiration()
	if !hasTimer {
		return 0, false
	}

	remaining = time.Until(expiresAt)
	if remaining <= 0 {
		return 0, true
	}

	return remaining, true
}

// PopExpired removes every handler whose expiration time is at or before time.Now() and calls onExpired
// for each of them.
//
// Parameters:
//   - onExpired receives each expired handler, in expiration time order. Handlers sharing the same
//     expiration time come in no particular order. nil only removes the handlers.
//
// Returns:
//   - The number of expired handlers.
//   - 0 when no handler has expired or the registry was not created with [newClientTimer].
//
// Notes:
//   - All expired handlers are removed from registry before the first onExpired call, so [clientTimer.Register]
//     and [clientTimer.Unregister] inside onExpired work on the remaining entries. A handler registered
//     again inside onExpired is not reported in the same call.
//   - Handlers passed to onExpired are no longer tracked; call [clientTimer.Register] to track them again.
func (this *clientTimer) PopExpired(onExpired func(handler ClientHandler)) int {
	if this.registry == nil {
		return 0
	}

	return this.registry.PopExpired(onExpired)
}
