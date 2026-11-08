package erio

import (
	"time"

	"github.com/emirpasic/gods/maps/treemap"
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
	ErrClientTimerUninitialized clientTimerError = "erio: client timer is uninitialized" // Returned by [clientTimer.Register] when the timer was not created with [newClientTimer].
	ErrClientTimerNilHandler    clientTimerError = "erio: client timer handler is nil"   // Returned by [clientTimer.Register] when handler is nil.
)

// clientTimer tracks one expiration time per [ClientHandler] and reports
// which handlers have expired.
type clientTimer struct {
	handlersByTime *treemap.Map                // Expiration time -> set of handlers (map[ClientHandler]struct{}) expiring at that time, ordered by time.
	timeByHandler  map[ClientHandler]time.Time // Handler -> its registered expiration time, used for lookup and removal.
}

// newClientTimer creates a [clientTimer] with an empty handlersByTime treemap ordered by time.Time.Compare
// and an empty timeByHandler map.
//
// Returns:
//   - The timer with no handler registered.
func newClientTimer() *clientTimer {
	return &clientTimer{
		handlersByTime: treemap.NewWith(func(first, second interface{}) int {
			return first.(time.Time).Compare(second.(time.Time))
		}),
		timeByHandler: make(map[ClientHandler]time.Time),
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
//   - [ErrClientTimerUninitialized] when the timer was not created with [newClientTimer].
//   - [ErrClientTimerNilHandler] when handler is nil.
//
// Notes:
//   - A handler has at most one expiration time. Registering again moves it to the new time.
func (this *clientTimer) Register(expiresAt time.Time, handler ClientHandler) error {
	if this.handlersByTime == nil || this.timeByHandler == nil {
		return ErrClientTimerUninitialized
	}
	if handler == nil {
		return ErrClientTimerNilHandler
	}

	this.Unregister(handler)
	value, found := this.handlersByTime.Get(expiresAt)
	var handlers map[ClientHandler]struct{}
	if found {
		handlers = value.(map[ClientHandler]struct{})
	} else {
		handlers = make(map[ClientHandler]struct{})
		this.handlersByTime.Put(expiresAt, handlers)
	}
	handlers[handler] = struct{}{}
	this.timeByHandler[handler] = expiresAt
	return nil
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
	expiresAt, contains := this.timeByHandler[handler]
	if !contains {
		return false
	}
	value, _ := this.handlersByTime.Get(expiresAt)
	handlers := value.(map[ClientHandler]struct{})
	delete(handlers, handler)
	if len(handlers) == 0 {
		this.handlersByTime.Remove(expiresAt)
	}
	delete(this.timeByHandler, handler)
	return true
}

// NextTimeout reports how long until the earliest registered expiration time.
//
// Returns:
//   - remaining is the duration until the earliest expiration time, or 0 when that time
//     has already passed or no handler is registered.
//   - hasTimer is true when at least one handler is registered, false otherwise.
func (this *clientTimer) NextTimeout() (remaining time.Duration, hasTimer bool) {
	if len(this.timeByHandler) == 0 {
		return 0, false
	}
	expiresAt, _ := this.handlersByTime.Min()
	remaining = time.Until(expiresAt.(time.Time))
	if remaining <= 0 {
		return 0, true
	}
	return remaining, true
}

// PopExpired removes and returns every handler whose expiration time is at or before now.
//
// Returns:
//   - The expired handlers, ordered by expiration time. Handlers sharing the same
//     expiration time are in no particular order.
//   - An empty slice when no handler has expired.
//   - nil when the timer was not created with [newClientTimer].
//
// Notes:
//   - Returned handlers are no longer tracked; call [clientTimer.Register] to track them again.
func (this *clientTimer) PopExpired() []ClientHandler {
	if this.handlersByTime == nil {
		return nil
	}

	expired := []ClientHandler{}
	if this.handlersByTime.Empty() {
		return expired
	}

	cutoff := time.Now()
	for !this.handlersByTime.Empty() {
		expiresAt, value := this.handlersByTime.Min()
		if expiresAt.(time.Time).After(cutoff) {
			break
		}
		for handler := range value.(map[ClientHandler]struct{}) {
			expired = append(expired, handler)
			delete(this.timeByHandler, handler)
		}
		this.handlersByTime.Remove(expiresAt)
	}

	return expired
}
