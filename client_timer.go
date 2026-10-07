package erio

import (
	"time"

	"github.com/emirpasic/gods/maps/treemap"
)

// clientTimerError represents an error returned by clientTimer.
type clientTimerError string

// Error returns the error string.
//
// It is the clientTimer error message.
func (this clientTimerError) Error() string {
	return string(this)
}

// Errors returned by clientTimer.
const (
	ErrClientTimerUninitialized clientTimerError = "erio: client timer is uninitialized"
	ErrClientTimerNilHandler    clientTimerError = "erio: client timer handler is nil"
)

// clientTimer manages the expiration times of ClientHandlers.
type clientTimer struct {
	handlersByTime *treemap.Map                // Set of ClientHandlers per expiration time.
	timeByHandler  map[ClientHandler]time.Time // Expiration time per ClientHandler.
}

// newClientTimer creates an empty clientTimer.
//
// It is the created clientTimer.
func newClientTimer() *clientTimer {
	return &clientTimer{
		handlersByTime: treemap.NewWith(func(first, second interface{}) int {
			return first.(time.Time).Compare(second.(time.Time))
		}),
		timeByHandler: make(map[ClientHandler]time.Time),
	}
}

// Register registers the ClientHandler at the given expiration time.
//
//   - expiresAt: time at which the ClientHandler expires.
//   - handler: ClientHandler to register.
//
// It returns an error if registration fails.
// It returns ErrClientTimerUninitialized if it was not created with newClientTimer, and
// ErrClientTimerNilHandler if handler is nil.
//
// For an already registered ClientHandler, it removes the existing registration and then registers
// it with the new expiration time.
func (this *clientTimer) Register(expiresAt time.Time, handler ClientHandler) error {
	if this.handlersByTime == nil || this.timeByHandler == nil {
		return ErrClientTimerUninitialized
	}
	if handler == nil {
		return ErrClientTimerNilHandler
	}
	// It registers a new entry even if there is no existing registration.
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

// Unregister removes the timer registration of the ClientHandler.
//
//   - handler: ClientHandler whose registration is removed.
//
// It returns true if the registration was removed, and false if it was not registered.
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

// NextTimeout returns the time remaining until the nearest timer.
//
// remaining is the time remaining until the nearest timer, and it is 0 if already expired.
//
// hasTimer returns true if there is a registered timer.
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

// PopExpired removes and returns all ClientHandlers expired as of the current time.
//
// It is the list of expired ClientHandlers, and it is an empty slice if no entries have expired.
// It is nil if it was not created with newClientTimer.
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
