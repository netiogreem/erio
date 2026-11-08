package erio

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"reflect"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/netiogreem/erio/internal"
)

// HandlerContextError is the error type returned by [HandlerContext] methods.
// It is a string so that each error can be declared as a comparable constant.
type HandlerContextError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this HandlerContextError) Error() string {
	return string(this)
}

const (
	ErrHandlerContextUninitialized               HandlerContextError = "erio: handler context is not bound"                   // Returned when the context is a zero value or is not yet registered with a reactor.
	ErrHandlerContextClosed                      HandlerContextError = "erio: handler context connection is closed"           // Returned by [HandlerContext.Write], [HandlerContext.PostUserEvent], [HandlerContext.Wake], [HandlerContext.SetTimeout], and [HandlerContext.UnsetTimeout] after [HandlerContext.Close] or [HandlerContext.AbortiveClose], or before registration.
	ErrHandlerContextAlreadyClosed               HandlerContextError = "erio: handler context connection is already closed"   // Returned by [HandlerContext.Close] when a close was already requested, and by [HandlerContext.AbortiveClose] when an abortive close was already requested.
	ErrHandlerContextNilHandler                  HandlerContextError = "erio: handler context handler is nil"                 // Returned during registration when the handler is nil.
	ErrHandlerContextNilCommandMailbox           HandlerContextError = "erio: handler context command mailbox is nil"         // Returned during registration when the reactor's command queue is nil.
	ErrHandlerContextNilUserCommand              HandlerContextError = "erio: handler context user command is nil"            // Returned by [HandlerContext.PostUserEvent] when userEventData is nil or a nil pointer.
	ErrHandlerContextAlreadyBound                HandlerContextError = "erio: handler context is already bound"               // Returned by [TCPReactor.RegisterHandler] when the context is already registered with a reactor.
	ErrHandlerContextInvalidFileDescriptor       HandlerContextError = "erio: invalid handler context file descriptor"        // Returned by [NewHandlerContext] and [HandlerContext.Init] when handlerFD is negative.
	ErrHandlerContextInvalidPeerAddrPort         HandlerContextError = "erio: invalid handler context peer address"           // Returned by [NewHandlerContext] and [HandlerContext.Init], wrapped with the getpeername error when there is one, when the peer address cannot be read or is not IPv4 or IPv6.
	ErrHandlerContextInvalidListenAddrPort       HandlerContextError = "erio: invalid handler context listen address"         // Returned by [NewHandlerContext] and [HandlerContext.Init] when listenAddrPort is not valid.
	ErrHandlerContextInvalidCommandQuota         HandlerContextError = "erio: invalid handler context command quota"          // Returned by [NewHandlerContext] when commandQuota is 0.
	ErrHandlerContextInvalidAbortiveCloseTimeout HandlerContextError = "erio: invalid handler context abortive close timeout" // Returned by [NewHandlerContext] when abortiveCloseTimeout is negative.
)

// closeStateType is the value stored in [HandlerContext.close].
type closeStateType = uint32

const (
	closeStateFalse closeStateType = iota // Connection is open.
	closeStateClose                       // Graceful close was requested, or the context is not yet registered.
	closeStateAbort                       // Abortive close was requested.
)

// HandlerContext is the connection object a [ClientHandler] uses to send data, post events, set
// timers, and close the connection. One HandlerContext belongs to one connection and holds its
// socket, write buffer, timers, close state, and addresses. Embed it in a handler struct; it provides
// [ClientHandler.GetCommandQuota], the context accessor, and default implementations of
// [ClientHandler.OnUserEvent], [ClientHandler.OnWake], [ClientHandler.OnTimeout], and
// [ClientHandler.OnReadClose].
//
// Notes:
//   - All methods except [HandlerContext.Init] and [HandlerContext.Reset] are thread-safe and can be
//     called from other goroutines.
//   - Write, SetTimeout, UnsetTimeout, Close, AbortiveClose, PostUserEvent, and Wake submit
//     asynchronous requests to the reactor, which processes them on the session's goroutine;
//     the returned error only covers submitting the request.
//   - Write, PostUserEvent, SetTimeout, and UnsetTimeout share the command quota given to
//     [NewHandlerContext]; Wake, Close, and AbortiveClose do not use it.
//   - To reuse a context for another connection, call Reset last inside [ClientHandler.OnClose]
//     and Init with the new connection before returning the handler from [ClientHandlerFactory].
type HandlerContext struct {
	close                 atomic.Uint32            // Current closeStateType.
	abortiveCloseTimeout  time.Duration            // How long Close waits for pending data before aborting; 0 waits without limit.
	abortiveCloseDeadline time.Time                // Time at which a graceful close turns abortive; zero when none.
	handlerFD             internal.FileDescriptor  // Connected socket; -1 after Reset.
	writeBuffer           *internal.BufferDirector // Data queued by Write and not yet sent.
	handler               ClientHandler            // Handler that embeds this context; nil until bind.
	commands              *commandMailbox          // Command queue of the reactor the handler is registered with; nil until bind.
	commandQuota          uint32                   // Maximum quota-limited commands per event loop iteration.
	wake                  atomic.Bool              // true while a Wake request is queued and not yet delivered.
	timer                 *internal.TimerRegistry  // Timers set by SetTimeout, keyed by timerKey; nil until bind.
	peerAddrPort          netip.AddrPort           // Remote address read from the socket at Init.
	listenAddrPort        netip.AddrPort           // Local listen address the connection was accepted on.
	handlerCounter        *atomic.Uint32           // Reactor's handler counter, shared after bind.
	immediateWrite        bool                     // Set by the mailbox while a write is recorded for the current iteration.
}

// NewHandlerContext creates a [HandlerContext] for an accepted connection.
//
// Parameters:
//   - handlerFD is the connected socket and must be non-negative. The context owns it and closes it
//     when the connection is closed.
//   - listenAddrPort is the local address the connection was accepted on and must be valid.
//   - writeBufferSize is the capacity of the write buffer that holds data queued by [HandlerContext.Write]
//     and not yet sent. Must be accepted by [internal.NewBufferDirector].
//   - commandQuota is the number of [HandlerContext.Write], [HandlerContext.PostUserEvent],
//     [HandlerContext.SetTimeout], and [HandlerContext.UnsetTimeout] requests accepted per event loop
//     iteration. Must be at least 1.
//   - abortiveCloseTimeout 0 sends all pending data without a time limit and then closes; a positive
//     value closes with RST when the data is not sent within it. Must not be negative.
//
// Returns:
//   - The context ready to be registered, and nil.
//   - [ErrHandlerContextInvalidCommandQuota] when commandQuota is 0.
//   - [ErrHandlerContextInvalidAbortiveCloseTimeout] when abortiveCloseTimeout is negative.
//   - The [internal.NewBufferDirector] error when writeBufferSize is not accepted.
//   - The [HandlerContext.Init] errors for handlerFD and listenAddrPort.
//
// Caution:
//   - handlerFD must be a non-blocking socket; a blocking socket stalls every session on the reactor.
func NewHandlerContext(handlerFD FileDescriptor, listenAddrPort netip.AddrPort,
	writeBufferSize uint32, commandQuota uint32, abortiveCloseTimeout time.Duration) (*HandlerContext, error) {

	if commandQuota == 0 {
		return nil, ErrHandlerContextInvalidCommandQuota
	}

	if abortiveCloseTimeout < 0 {
		return nil, ErrHandlerContextInvalidAbortiveCloseTimeout
	}

	writeBuffer, bufferError := internal.NewBufferDirector(writeBufferSize)
	if bufferError != nil {
		return nil, bufferError
	}

	context := &HandlerContext{
		writeBuffer:          writeBuffer,
		commandQuota:         commandQuota,
		abortiveCloseTimeout: abortiveCloseTimeout,
	}
	if err := context.Init(handlerFD, listenAddrPort); err != nil {
		return nil, err
	}

	return context, nil
}

// Init prepares the context for a new connection, keeping the write buffer, command quota, and
// abortive close timeout from [NewHandlerContext].
//
// Parameters:
//   - handlerFD is the connected socket and must be non-negative.
//   - listenAddrPort is the local address the connection was accepted on and must be valid.
//
// Returns:
//   - nil when the context is ready to be registered.
//   - [ErrHandlerContextUninitialized] when the context is a zero value.
//   - [ErrHandlerContextInvalidFileDescriptor] when handlerFD is negative.
//   - [ErrHandlerContextInvalidListenAddrPort] when listenAddrPort is not valid.
//   - [ErrHandlerContextInvalidPeerAddrPort], wrapped with the getpeername error, when the peer
//     address cannot be read from handlerFD.
//   - The write buffer reset error.
//
// Notes:
//   - The context starts in the closed state; it becomes open when the reactor registers it.
//   - Not thread-safe. Call it only before the handler is registered with the reactor, that is,
//     before returning the handler from [ClientHandlerFactory].
func (this *HandlerContext) Init(handlerFD FileDescriptor, listenAddrPort netip.AddrPort) error {
	if this.writeBuffer == nil {
		return ErrHandlerContextUninitialized
	}

	if handlerFD < 0 {
		return ErrHandlerContextInvalidFileDescriptor
	}

	if !listenAddrPort.IsValid() {
		return ErrHandlerContextInvalidListenAddrPort
	}

	peerAddrPort, addressError := peerAddrPort(handlerFD)
	if addressError != nil {
		return fmt.Errorf("%w: %w", ErrHandlerContextInvalidPeerAddrPort, addressError)
	}

	if err := this.writeBuffer.Reset(); err != nil {
		return err
	}

	this.close.Store(closeStateClose)
	this.abortiveCloseDeadline = time.Time{}
	this.handlerFD = handlerFD
	this.handler = nil
	this.commands = nil
	this.wake.Store(false)
	this.timer = nil
	this.peerAddrPort = peerAddrPort
	this.listenAddrPort = listenAddrPort
	this.handlerCounter = &atomic.Uint32{}
	this.immediateWrite = false

	return nil
}

// Reset clears all state except the command quota and abortive close timeout, and sets the file
// descriptor to -1. The write buffer keeps its capacity.
//
// Returns:
//   - nil when cleared.
//   - [ErrHandlerContextUninitialized] when the context is a zero value.
//   - The write buffer reset error.
//
// Notes:
//   - Not thread-safe. If needed, call it last inside [ClientHandler.OnClose].
//   - Call [HandlerContext.Init] to use the context for another connection.
func (this *HandlerContext) Reset() error {
	if this.writeBuffer == nil {
		return ErrHandlerContextUninitialized
	}

	if err := this.writeBuffer.Reset(); err != nil {
		return err
	}

	this.close.Store(closeStateClose)
	this.abortiveCloseDeadline = time.Time{}
	this.handlerFD = -1
	this.handler = nil
	this.commands = nil

	this.wake.Store(false)
	this.timer = nil
	this.peerAddrPort = netip.AddrPort{}
	this.listenAddrPort = netip.AddrPort{}
	this.handlerCounter = &atomic.Uint32{}
	this.immediateWrite = false

	return nil
}

// peerAddrPort calls syscall.Getpeername(fd) and converts the result to [netip.AddrPort].
//
// Parameters:
//   - fd is the connected socket.
//
// Returns:
//   - The address and nil when Getpeername returned [syscall.SockaddrInet4] or [syscall.SockaddrInet6].
//   - A zero AddrPort and the Getpeername error wrapped by os.NewSyscallError("getpeername", err).
//   - A zero AddrPort and [ErrHandlerContextInvalidPeerAddrPort] when the sockaddr is another type.
//
// Notes:
//   - For SockaddrInet6 the address goes through netip.Addr.Unmap, so an IPv4-mapped address becomes IPv4.
func peerAddrPort(fd FileDescriptor) (netip.AddrPort, error) {
	socketAddress, err := syscall.Getpeername(int(fd))
	if err != nil {
		return netip.AddrPort{}, os.NewSyscallError("getpeername", err)
	}

	switch address := socketAddress.(type) {
	case *syscall.SockaddrInet4:
		return netip.AddrPortFrom(netip.AddrFrom4(address.Addr), uint16(address.Port)), nil
	case *syscall.SockaddrInet6:
		return netip.AddrPortFrom(netip.AddrFrom16(address.Addr).Unmap(), uint16(address.Port)), nil
	}

	return netip.AddrPort{}, ErrHandlerContextInvalidPeerAddrPort
}

// GetCommandQuota returns the command quota of this session given to [NewHandlerContext].
//
// Returns:
//   - The commandQuota value. 0 for a zero-value context.
//
// Notes:
//   - Implements [ClientHandler.GetCommandQuota].
func (this *HandlerContext) GetCommandQuota() uint32 {
	return this.commandQuota
}

// context returns the context itself so a handler embedding [HandlerContext] satisfies [ClientHandler].
//
// Returns:
//   - The receiver.
func (this *HandlerContext) context() *HandlerContext {
	return this
}

// bind sets this.handler, this.commands, this.timer (a new [internal.TimerRegistry]), and this.handlerCounter,
// then stores [closeStateFalse] in close.
//
// Parameters:
//   - handler is the [ClientHandler] that embeds this context. Must not be nil.
//   - commands is the [commandMailbox] of the reactor. Must not be nil.
//   - handlerCounter is the reactor's handlerCounter, read by [HandlerContext.Count].
//
// Returns:
//   - nil when the fields were set and close is closeStateFalse.
//   - [ErrHandlerContextNilHandler] when handler is nil.
//   - [ErrHandlerContextNilCommandMailbox] when commands is nil.
//   - [ErrHandlerContextAlreadyBound] when this.handler is already set.
//
// Notes:
//   - Called on the event loop goroutine by [tcpReactor.handleCommandRegisterHandler].
func (this *HandlerContext) bind(handler ClientHandler, commands *commandMailbox, handlerCounter *atomic.Uint32) error {
	if handler == nil {
		return ErrHandlerContextNilHandler
	}

	if commands == nil {
		return ErrHandlerContextNilCommandMailbox
	}

	if this.handler != nil {
		return ErrHandlerContextAlreadyBound
	}

	this.handler = handler
	this.commands = commands
	this.timer = internal.NewTimerRegistry()
	this.handlerCounter = handlerCounter
	this.close.Store(closeStateFalse)
	return nil
}

// isBound returns this.handler != nil && this.commands != nil.
//
// Returns:
//   - true after [HandlerContext.bind].
//   - false before bind, and after [HandlerContext.Init] or [HandlerContext.Reset], which set both to nil.
func (this *HandlerContext) isBound() bool {
	return this.handler != nil && this.commands != nil
}

// Count returns the number of connected clients, counted across the reactors that share the
// handler counter.
//
// Returns:
//   - The number of registered handlers.
//   - 0 before registration.
func (this *HandlerContext) Count() uint32 {
	if this.handlerCounter == nil {
		return 0
	}
	return this.handlerCounter.Load()
}

// PeerAddrPort returns the remote address of the connection.
//
// Returns:
//   - The remote address read from the socket at [HandlerContext.Init].
//   - A zero value after [HandlerContext.Reset] or for a zero-value context.
func (this *HandlerContext) PeerAddrPort() netip.AddrPort {
	return this.peerAddrPort
}

// ListenAddrPort returns the local address the connection was accepted on.
//
// Returns:
//   - The address given to [HandlerContext.Init].
//   - A zero value after [HandlerContext.Reset] or for a zero-value context.
func (this *HandlerContext) ListenAddrPort() netip.AddrPort {
	return this.listenAddrPort
}

// OnUserEvent is the default [ClientHandler.OnUserEvent] and does nothing.
//
// Parameters:
//   - context is ignored.
//   - userEventData is ignored.
//
// Notes:
//   - Override it to receive events from [HandlerContext.PostUserEvent].
func (this *HandlerContext) OnUserEvent(context *HandlerContext, userEventData any) {}

// OnWake is the default [ClientHandler.OnWake] and does nothing.
//
// Parameters:
//   - context is ignored.
//
// Notes:
//   - Override it to be called after [HandlerContext.Wake].
func (this *HandlerContext) OnWake(context *HandlerContext) {}

// OnTimeout is the default [ClientHandler.OnTimeout] and does nothing.
//
// Parameters:
//   - context is ignored.
//   - timerKey is ignored.
//
// Notes:
//   - Override it to be called when a timer set by [HandlerContext.SetTimeout] expires.
func (this *HandlerContext) OnTimeout(context *HandlerContext, timerKey uint64) {}

// OnReadClose is the default [ClientHandler.OnReadClose]. It requests [HandlerContext.Close] so
// pending data is sent and the connection is closed.
//
// Parameters:
//   - context is ignored; the receiver is used instead.
//
// Notes:
//   - A Close error is passed to [ClientHandler.OnError].
//   - The connection is still open when it is called. Override it to keep writing after the peer has
//     closed its write side.
func (this *HandlerContext) OnReadClose(context *HandlerContext) {
	if err := this.Close(); err != nil {
		this.handler.OnError(this, fmt.Errorf("HandlerContext.OnReadClose() Error: %w", err))
	}
}

// Write sends buffer to the peer. The data is copied into the send buffer and written
// asynchronously; completion is reported through [ClientHandler.OnWritten] with streamID.
//
// Parameters:
//   - streamID identifies the message and is passed back to [ClientHandler.OnWritten] when the
//     whole buffer has been sent.
//   - buffer is copied into the write buffer; the caller may reuse it after Write returns.
//     An empty buffer is ignored.
//
// Returns:
//   - nil when the data was accepted.
//   - [ErrHandlerContextClosed] after [HandlerContext.Close] or [HandlerContext.AbortiveClose], or before registration.
//   - [ErrHandlerContextUninitialized] when the context is a zero value or not registered.
//   - The write buffer error when buffer does not fit.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when the command quota is used up until the reactor processes the pending requests.
//   - [ErrCommandMailboxClosed] when the reactor is stopping.
//   - A system call error when waking the reactor fails.
//
// Notes:
//   - Data is sent on the session's reactor goroutine. [ClientHandler.OnWritten] means the data was handed
//     to the kernel, not that the peer received it. [HandlerContext.UnwrittenSize] reports what is still pending.
func (this *HandlerContext) Write(streamID int32, buffer []byte) error {
	if this.close.Load() != closeStateFalse {
		return ErrHandlerContextClosed
	}

	if this.writeBuffer == nil || this.handler == nil || this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	if len(buffer) == 0 {
		return nil
	}

	return this.writeBuffer.AppendWith(streamID, buffer, func() error {
		return this.commands.EnqueueWrite(this.handler)
	})
}

// PostUserEvent delivers userEventData to [ClientHandler.OnUserEvent] on the session's goroutine.
//
// Parameters:
//   - userEventData is passed to [ClientHandler.OnUserEvent] unchanged and must not be nil or a nil pointer.
//
// Returns:
//   - nil when queued.
//   - [ErrHandlerContextClosed] after [HandlerContext.Close] or [HandlerContext.AbortiveClose], or before registration.
//   - [ErrHandlerContextUninitialized] when the context is not registered.
//   - [ErrHandlerContextNilUserCommand] when userEventData is nil or a nil pointer.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when the command quota is used up until the reactor processes the pending requests.
//   - [ErrCommandMailboxClosed] when the reactor is stopping.
//   - A system call error when waking the reactor fails.
func (this *HandlerContext) PostUserEvent(userEventData any) error {
	if this.close.Load() != closeStateFalse {
		return ErrHandlerContextClosed
	}

	if this.handler == nil || this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	if userEventData == nil {
		return ErrHandlerContextNilUserCommand
	}

	value := reflect.ValueOf(userEventData)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return ErrHandlerContextNilUserCommand
	}

	return this.commands.EnqueueUserEvent(this.handler, userEventData)
}

// Wake requests [ClientHandler.OnWake] on the session's goroutine without sending event data.
//
// Returns:
//   - nil when requested, or when a Wake is already pending.
//   - [ErrHandlerContextClosed] after [HandlerContext.Close] or [HandlerContext.AbortiveClose], or before registration.
//   - [ErrHandlerContextUninitialized] when the context is not registered.
//   - [ErrCommandMailboxClosed] when the reactor is stopping, or a system call error when waking the
//     reactor fails. Wake can be called again.
//
// Notes:
//   - Wake calls made before [ClientHandler.OnWake] starts are combined into one OnWake call. Calls made
//     during OnWake request another one.
//   - A nil return does not guarantee that OnWake runs. When the handler is removed before the
//     request is processed, OnWake is skipped.
//   - Does not use the command quota.
func (this *HandlerContext) Wake() error {
	if this.close.Load() != closeStateFalse {
		return ErrHandlerContextClosed
	}

	if this.handler == nil || this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	if !this.wake.CompareAndSwap(false, true) {
		return nil
	}

	if err := this.commands.EnqueueWake(this.handler); err != nil {
		this.wake.Store(false)
		return err
	}

	return nil
}

// SetTimeout arms a timer that calls [ClientHandler.OnTimeout] with timerKey after timeout.
//
// Parameters:
//   - timerKey identifies the timer. Setting an existing key again replaces its expiration.
//   - timeout is measured from the time of this call.
//
// Returns:
//   - nil when queued.
//   - [ErrHandlerContextClosed] after [HandlerContext.Close] or [HandlerContext.AbortiveClose], or before registration.
//   - [ErrHandlerContextUninitialized] when the context is not registered.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when the command quota is used up until the reactor processes the pending requests.
//   - [ErrCommandMailboxClosed] when the reactor is stopping.
//   - A system call error when waking the reactor fails.
//
// Notes:
//   - A timer fires once. Call SetTimeout again from [ClientHandler.OnTimeout] for a repeating timer.
func (this *HandlerContext) SetTimeout(timerKey uint64, timeout time.Duration) error {
	if this.close.Load() != closeStateFalse {
		return ErrHandlerContextClosed
	}

	if this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	return this.commands.EnqueueSetTimeout(this.handler, timerKey, time.Now().Add(timeout))
}

// UnsetTimeout cancels the timer identified by timerKey.
//
// Parameters:
//   - timerKey is the key given to [HandlerContext.SetTimeout]. An unknown key is ignored.
//
// Returns:
//   - nil when queued.
//   - [ErrHandlerContextClosed] after [HandlerContext.Close] or [HandlerContext.AbortiveClose], or before registration.
//   - [ErrHandlerContextUninitialized] when the context is not registered.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when the command quota is used up until the reactor processes the pending requests.
//   - [ErrCommandMailboxClosed] when the reactor is stopping.
//   - A system call error when waking the reactor fails.
//
// Notes:
//   - A timer that expires before the cancel is processed on the event loop still fires.
func (this *HandlerContext) UnsetTimeout(timerKey uint64) error {
	if this.close.Load() != closeStateFalse {
		return ErrHandlerContextClosed
	}

	if this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	return this.commands.EnqueueUnsetTimeout(this.handler, timerKey)
}

// Close requests a graceful close: pending data is sent, then the socket is closed and
// [ClientHandler.OnClose] is called with [ErrTCPReactorCloseRequested].
//
// Returns:
//   - nil when queued.
//   - [ErrHandlerContextUninitialized] when the context is not registered.
//   - [ErrHandlerContextAlreadyClosed] when Close or [HandlerContext.AbortiveClose] was already called.
//   - [ErrCommandMailboxClosed] when the reactor is stopping, or a system call error when waking the reactor fails.
//
// Notes:
//   - [HandlerContext.Write], [HandlerContext.PostUserEvent], [HandlerContext.Wake], [HandlerContext.SetTimeout],
//     and [HandlerContext.UnsetTimeout] fail with [ErrHandlerContextClosed] from this point.
//   - With abortiveCloseTimeout 0, the connection is closed only after all pending data is sent,
//     so it stays open as long as the peer does not read. Set abortiveCloseTimeout or call
//     [HandlerContext.AbortiveClose] when the connection must be closed.
//   - With a positive abortiveCloseTimeout, pending data not sent within it is discarded, the
//     connection is closed with RST, and [ClientHandler.OnClose] receives [ErrTCPReactorAbortiveCloseTimeout].
//   - Does not use the command quota.
func (this *HandlerContext) Close() error {
	if this.handler == nil || this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	if !this.close.CompareAndSwap(closeStateFalse, closeStateClose) {
		return ErrHandlerContextAlreadyClosed
	}

	deadline := time.Time{}
	if this.abortiveCloseTimeout > 0 {
		deadline = time.Now().Add(this.abortiveCloseTimeout)
	}

	return this.commands.EnqueueClose(this.handler, deadline)
}

// AbortiveClose requests an immediate close that discards pending data: the socket is closed
// with SO_LINGER 0 so the peer receives RST, and [ClientHandler.OnClose] is called with
// [ErrTCPReactorAbortiveClose].
//
// Returns:
//   - nil when queued.
//   - [ErrHandlerContextUninitialized] when the context is not registered.
//   - [ErrHandlerContextAlreadyClosed] when AbortiveClose was already called.
//   - [ErrCommandMailboxClosed] when the reactor is stopping, or a system call error when waking the reactor fails.
//
// Notes:
//   - May be called while a [HandlerContext.Close] request is still waiting for pending data to be sent.
//   - Data already reported through [ClientHandler.OnWritten] is discarded too when it has not
//     reached the peer yet.
//   - Does not use the command quota.
func (this *HandlerContext) AbortiveClose() error {
	if this.handler == nil || this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	if !this.close.CompareAndSwap(closeStateFalse, closeStateAbort) &&
		!this.close.CompareAndSwap(closeStateClose, closeStateAbort) {
		return ErrHandlerContextAlreadyClosed
	}

	return this.commands.EnqueueAbortiveClose(this.handler)
}

// IsClosed reports whether the connection can no longer be used.
//
// Returns:
//   - true when a close was requested, the connection was lost, or the context was initialized but
//     not yet registered.
//   - false while the connection is registered and open.
//
// Notes:
//   - A zero-value context that never went through [NewHandlerContext] or [HandlerContext.Init] reports false.
func (this *HandlerContext) IsClosed() bool {
	return this.close.Load() != closeStateFalse
}

// IsAbortiveClosed reports whether an abortive close was requested.
//
// Returns:
//   - true after [HandlerContext.AbortiveClose] was accepted.
//   - false otherwise, including after a graceful [HandlerContext.Close].
func (this *HandlerContext) IsAbortiveClosed() bool {
	return this.close.Load() == closeStateAbort
}

// IsOpen reports whether the connection is registered and no close was requested.
//
// Returns:
//   - true while the connection is registered and open.
//   - false when a close was requested, the connection was lost, or the context was initialized but
//     not yet registered.
//
// Notes:
//   - A zero-value context that never went through [NewHandlerContext] or [HandlerContext.Init] reports true.
func (this *HandlerContext) IsOpen() bool {
	return this.close.Load() == closeStateFalse
}

// UnwrittenSize returns the number of bytes passed to [HandlerContext.Write] that have not yet
// been written to the socket.
//
// Returns:
//   - The number of pending bytes in the write buffer. 0 for a zero-value context
func (this *HandlerContext) UnwrittenSize() uint64 {
	return this.writeBuffer.RemainingSize()
}

// isDrainedForClose returns close.Load() != closeStateFalse && writeBuffer.RemainingSize() == 0.
//
// Returns:
//   - true when Close or AbortiveClose was requested, or the connection was lost, and writeBuffer is empty.
//   - false otherwise.
func (this *HandlerContext) isDrainedForClose() bool {
	return this.close.Load() != closeStateFalse && this.writeBuffer.RemainingSize() == 0
}

// onReadable calls read(2) on handlerFD into readBuffer with syscall.RawSyscall(SYS_READ) and passes
// readBuffer[:recvBytes] to handler.OnRead.
//
// Parameters:
//   - readAll true repeats the read until it returns EAGAIN, 0, or an error; false reads once.
//   - readBuffer is the tcpReactor.readBuffer shared by all handlers of the reactor. Must not be empty.
//
// Returns:
//   - true when read returned 0, that is, the peer closed the connection.
//   - false when read returned EAGAIN or EWOULDBLOCK, when another error occurred, or after one read when
//     readAll is false.
//
// Notes:
//   - EINTR is retried.
//   - An error other than EAGAIN and EWOULDBLOCK is passed to handler.OnError as
//     os.NewSyscallError("read", err) and ends the read.
//   - The slice passed to OnRead is readBuffer[:recvBytes:recvBytes], valid only during the call.
func (this *HandlerContext) onReadable(readAll bool, readBuffer []byte) (isEof bool) {
	for {
		var recvBytes int
		var recvError error
		for {
			r, _, errno := syscall.RawSyscall(syscall.SYS_READ, uintptr(this.handlerFD),
				uintptr(unsafe.Pointer(&readBuffer[0])), uintptr(len(readBuffer)))
			recvBytes, recvError = int(r), nil
			if errno != 0 {
				recvError = errno
			}
			if recvError != syscall.EINTR {
				break
			}
		}

		if recvBytes > 0 {
			this.handler.OnRead(this, readBuffer[:recvBytes:recvBytes])
		}

		if recvError == syscall.EAGAIN || recvError == syscall.EWOULDBLOCK {
			return false
		}

		if recvError != nil {
			this.handler.OnError(this, os.NewSyscallError("read", recvError))
			return false
		}

		if recvBytes == 0 {
			return true
		}

		if !readAll {
			return false
		}
	}
}

// onWritable calls writeBuffer.ConsumeWith with a consumer that calls write(2) on handlerFD with
// syscall.RawSyscall(SYS_WRITE), then calls handler.OnWritten for each message that was written completely.
//
// Returns:
//   - The hasRemaining value of ConsumeWith: true when writeBuffer still has data, false when it is empty.
//
// Notes:
//   - EINTR is retried. EAGAIN and EWOULDBLOCK are turned into a nil error, so the consumer returns the bytes
//     written so far.
//   - handler.OnWritten(this, info.ID, info.Length) is called for every entry of consumedMesgs before the
//     consumeError, if any, is passed to handler.OnError as os.NewSyscallError("write", err).
func (this *HandlerContext) onWritable() (hasRemaining bool) {
	consumer := func(data []byte, _ []internal.MessageInfo) (uint32, error) {
		var writtenSize int
		var writeError error
		for {
			r, _, errno := syscall.RawSyscall(syscall.SYS_WRITE, uintptr(this.handlerFD),
				uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)))
			writtenSize, writeError = int(r), nil
			if errno != 0 {
				writeError = errno
			}
			if writeError != syscall.EINTR {
				break
			}
		}

		if writtenSize < 0 {
			writtenSize = 0
		}

		if writeError == syscall.EAGAIN || writeError == syscall.EWOULDBLOCK {
			writeError = nil
		}

		return uint32(writtenSize), os.NewSyscallError("write", writeError)
	}

	_, consumedMesgs, hasRemaining, consumeError := this.writeBuffer.ConsumeWith(consumer)

	for _, info := range consumedMesgs {
		this.handler.OnWritten(this, info.ID, info.Length)
	}

	if consumeError != nil {
		this.handler.OnError(this, consumeError)
	}

	return hasRemaining
}

// onUserEvent calls handler.OnUserEvent(this, userEventData).
//
// Parameters:
//   - userEventData is the value given to [HandlerContext.PostUserEvent].
func (this *HandlerContext) onUserEvent(userEventData any) {
	this.handler.OnUserEvent(this, userEventData)
}

// onWake calls wake.Store(false) and then handler.OnWake(this).
//
// Notes:
//   - wake is cleared first so that [HandlerContext.Wake] called inside OnWake passes its
//     wake.CompareAndSwap(false, true) and queues a new commandWake.
func (this *HandlerContext) onWake() {
	this.wake.Store(false)
	this.handler.OnWake(this)
}

// onTimeout calls handler.OnTimeout(this, timerKey) for every key returned by timer.PopExpired() and checks
// abortiveCloseDeadline.
//
// Returns:
//   - nextExpiration and hasTimer are the values of [HandlerContext.nextExpiration] after the expired keys
//     were removed.
//   - aborted is true when abortiveCloseDeadline is not zero and is not after time.Now().
//
// Notes:
//   - PopExpired removes the keys before OnTimeout runs, so [HandlerContext.SetTimeout] inside OnTimeout
//     with the same key re-arms it.
func (this *HandlerContext) onTimeout() (nextExpiration time.Time, hasTimer bool, aborted bool) {
	for _, timerKey := range this.timer.PopExpired() {
		this.handler.OnTimeout(this, timerKey)
	}

	if !this.abortiveCloseDeadline.IsZero() && !this.abortiveCloseDeadline.After(time.Now()) {
		aborted = true
	}

	nextExpiration, hasTimer = this.nextExpiration()
	return nextExpiration, hasTimer, aborted
}

// nextExpiration merges timer.NextExpiration() with abortiveCloseDeadline into the single time that
// [tcpReactor.updateClientTimer] registers in clientTimer.
//
// Returns:
//   - timer.NextExpiration() when abortiveCloseDeadline is zero.
//   - abortiveCloseDeadline and true when timer has no entry or abortiveCloseDeadline is before the timer's.
//   - timer.NextExpiration() otherwise.
//   - hasTimer is false only when timer is empty and abortiveCloseDeadline is zero.
func (this *HandlerContext) nextExpiration() (nextExpiration time.Time, hasTimer bool) {
	nextExpiration, hasTimer = this.timer.NextExpiration()
	if this.abortiveCloseDeadline.IsZero() {
		return nextExpiration, hasTimer
	}

	if !hasTimer || this.abortiveCloseDeadline.Before(nextExpiration) {
		return this.abortiveCloseDeadline, true
	}

	return nextExpiration, hasTimer
}

// setAbortiveCloseDeadline stores abortiveCloseDeadline when abortiveCloseTimeout > 0 and returns
// [HandlerContext.nextExpiration].
//
// Parameters:
//   - abortiveCloseDeadline is command.expiresAt of the commandClose, computed by [HandlerContext.Close].
//
// Returns:
//   - The values of nextExpiration after the store.
//
// Notes:
//   - When abortiveCloseTimeout is 0 the field stays zero, so [HandlerContext.onTimeout] never returns aborted true.
func (this *HandlerContext) setAbortiveCloseDeadline(abortiveCloseDeadline time.Time) (nextExpiration time.Time, hasTimer bool) {
	if this.abortiveCloseTimeout > 0 {
		this.abortiveCloseDeadline = abortiveCloseDeadline
	}

	return this.nextExpiration()
}

// abortConnection returns closeFileDescriptor(true): SO_LINGER 0 then close, so the peer receives RST and
// data in the kernel send buffer is discarded.
//
// Returns:
//   - The value of [HandlerContext.closeFileDescriptor].
func (this *HandlerContext) abortConnection() error {
	return this.closeFileDescriptor(true)
}

// closeConnection returns closeFileDescriptor(false): a plain close, so the kernel still sends data in its
// send buffer.
//
// Returns:
//   - The value of [HandlerContext.closeFileDescriptor].
func (this *HandlerContext) closeConnection() error {
	return this.closeFileDescriptor(false)
}

// closeFileDescriptor calls syscall.Close(handlerFD), after syscall.SetsockoptLinger with Onoff 1 and Linger 0
// when setLinger is true.
//
// Parameters:
//   - setLinger true sets SO_LINGER to 0 first, so the close sends RST and discards pending data.
//
// Returns:
//   - nil when SetsockoptLinger (if called) and Close succeeded.
//   - errors.Join of the "setsockopt" and "close" [os.SyscallError] values otherwise.
func (this *HandlerContext) closeFileDescriptor(setLinger bool) error {
	var lingerError error
	if setLinger {
		linger := syscall.Linger{Onoff: 1, Linger: 0}
		lingerError = os.NewSyscallError("setsockopt", syscall.SetsockoptLinger(int(this.handlerFD), syscall.SOL_SOCKET, syscall.SO_LINGER, &linger))
	}

	return errors.Join(lingerError, os.NewSyscallError("close", syscall.Close(int(this.handlerFD))))
}

// onSetTimeout calls timer.Register(expiresAt, timerKey) and returns [HandlerContext.nextExpiration].
//
// Parameters:
//   - timerKey is the key given to [HandlerContext.SetTimeout]. An existing key is moved to expiresAt.
//   - expiresAt is the time computed by SetTimeout.
//
// Returns:
//   - The values of nextExpiration after Register.
//
// Notes:
//   - The Register error is passed to handler.OnError and timer is left unchanged.
func (this *HandlerContext) onSetTimeout(timerKey uint64, expiresAt time.Time) (nextExpiration time.Time, hasTimer bool) {
	if err := this.timer.Register(expiresAt, timerKey); err != nil {
		this.handler.OnError(this, err)
		return this.nextExpiration()
	}

	return this.nextExpiration()
}

// onUnsetTimeout calls timer.Unregister(timerKey) and returns [HandlerContext.nextExpiration].
//
// Parameters:
//   - timerKey is the key given to [HandlerContext.UnsetTimeout]. Unregister returns false for an unknown
//     key, which is ignored.
//
// Returns:
//   - The values of nextExpiration after Unregister.
func (this *HandlerContext) onUnsetTimeout(timerKey uint64) (nextExpiration time.Time, hasTimer bool) {
	if !this.timer.Unregister(timerKey) {
		return this.nextExpiration()
	}

	return this.nextExpiration()
}

// fileDescriptor returns handlerFD.
//
// Returns:
//   - The handlerFD given to [HandlerContext.Init].
//   - -1 after [HandlerContext.Reset]. 0 for a zero-value context.
func (this *HandlerContext) fileDescriptor() internal.FileDescriptor {
	return this.handlerFD
}

// setClosed calls close.CompareAndSwap(closeStateFalse, closeStateClose).
//
// Notes:
//   - When close is already closeStateClose or closeStateAbort, nothing changes.
//   - Called by [tcpReactor.handleEvent] on EPOLLHUP, [tcpReactor.removeHandler], and
//     [tcpReactor.rejectHandler], so later Write, PostUserEvent, Wake, SetTimeout, and UnsetTimeout return
//     [ErrHandlerContextClosed].
func (this *HandlerContext) setClosed() {
	this.close.CompareAndSwap(closeStateFalse, closeStateClose)
}
