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

// HandlerContextError represents an error returned by HandlerContext.
type HandlerContextError string

// Error returns the error string.
//
// It returns the HandlerContext error message.
func (this HandlerContextError) Error() string {
	return string(this)
}

// Errors returned by HandlerContext.
const (
	ErrHandlerContextUninitialized         HandlerContextError = "erio: handler context is not bound"
	ErrHandlerContextClosed                HandlerContextError = "erio: handler context connection is closed"
	ErrHandlerContextNilHandler            HandlerContextError = "erio: handler context handler is nil"
	ErrHandlerContextNilCommandMailbox     HandlerContextError = "erio: handler context command mailbox is nil"
	ErrHandlerContextNilUserCommand        HandlerContextError = "erio: handler context user command is nil"
	ErrHandlerContextAlreadyBound          HandlerContextError = "erio: handler context is already bound"
	ErrHandlerContextInvalidFileDescriptor HandlerContextError = "erio: invalid handler context file descriptor"
	ErrHandlerContextInvalidPeerAddrPort   HandlerContextError = "erio: invalid handler context peer address"
	ErrHandlerContextInvalidListenAddrPort HandlerContextError = "erio: invalid handler context listen address"
	ErrHandlerContextInvalidCommandQuota   HandlerContextError = "erio: invalid handler context command quota"
)

// HandlerContext provides send/receive, close, timer, and user event features for a client TCP connection.
// It must be created with NewHandlerContext.
// Write, Close, SetTimeout, UnsetTimeout, and PostUserEvent can be called concurrently from multiple goroutines,
// and they return ErrHandlerContextClosed before it is registered with TCPReactor.
type HandlerContext struct {
	open           atomic.Bool              // Whether the connection is usable
	handlerFD      internal.FileDescriptor  // File descriptor duplicated from the client connection
	writeBuffer    *internal.BufferDirector // Write buffer
	handler        ClientHandler            // Connection event handler
	commands       *commandMailbox          // TCPReactor command management
	commandQuota   uint32                   // Per-connection limit on the total of pending timer, write, and user event commands
	timer          *internal.TimerRegistry  // Timers registered for the connection
	peerAddrPort   netip.AddrPort           // Client address and port
	listenAddrPort netip.AddrPort           // Address and port of the listener that accepted the connection
	handlerCounter *atomic.Uint32           // Atomic that stores the number of registered handlers
	immediateWrite bool                     // Whether this Handler is already queued in commandMailbox.immediateWrites during an immediate write section. Guarded by commandMailbox.accessMutex.
}

// NewHandlerContext creates a HandlerContext for a client TCP connection.
//
//   - It does not close handlerFD on creation failure, and the caller must clean up the connection.
//   - write, close, timer, and user event requests can be used after TCPReactor registration is
//     complete.
//   - The write buffer is a double buffer for minimizing locking and uses twice the specified size.
//
// The arguments are as follows.
//
//   - handlerFD: file descriptor of the client TCP connection. It must not be negative.
//   - listenAddrPort: address and port of the listener that accepted the connection.
//     It must be a valid address.
//   - writeBufferSize: write buffer size (bytes). It is a double buffer for reducing locking, so it
//     uses twice the specified size.
//     If the total including data waiting to be sent exceeds this size, Write returns an error
//     without waiting.
//   - commandQuota: limit on the total of requests on this connection not yet processed by the
//     Reactor for Write, SetTimeout, UnsetTimeout, and PostUserEvent.
//     If the limit is exceeded, the request returns an error.
//     Write calls made while the Reactor is processing epoll events or expired timers are not
//     included, regardless of the calling goroutine.
//     0 is not allowed.
//
// It returns the created HandlerContext and nil on success,
// and returns nil and an error if an argument is invalid or if preparing the connection or buffers
// fails.
// It returns ErrHandlerContextInvalidFileDescriptor if handlerFD is negative,
// ErrHandlerContextInvalidListenAddrPort if listenAddrPort is not valid,
// ErrHandlerContextInvalidCommandQuota if commandQuota is 0,
// ErrHandlerContextInvalidPeerAddrPort if the peer address cannot be retrieved,
// and internal.ErrBufferDirectorInvalidSize (wrapped) if writeBufferSize is 0 or too large.
func NewHandlerContext(handlerFD FileDescriptor, listenAddrPort netip.AddrPort,
	writeBufferSize uint32, commandQuota uint32) (*HandlerContext, error) {

	if commandQuota == 0 {
		return nil, ErrHandlerContextInvalidCommandQuota
	}

	writeBuffer, bufferError := internal.NewBufferDirector(writeBufferSize)
	if bufferError != nil {
		return nil, bufferError
	}

	context := &HandlerContext{
		writeBuffer:  writeBuffer,
		commandQuota: commandQuota,
	}
	if err := context.Init(handlerFD, listenAddrPort); err != nil {
		return nil, err
	}

	return context, nil
}

// Init reuses the write buffer and commandQuota, and sets up the state right after
// creation with the given connection information.
// It does not allocate a new write buffer and keeps its capacity.
// If validation fails, it does not change the current object.
// It does not close handlerFD on failure.
// It must not be called concurrently with other methods.
// It is safe to call only before the handler is registered with the Reactor (before returning it
// from ClientHandlerFactory), after OnClose returns, or as the last call in OnClose.
//
// It returns ErrHandlerContextUninitialized if it was not created with NewHandlerContext,
// ErrHandlerContextInvalidFileDescriptor if handlerFD is negative,
// ErrHandlerContextInvalidListenAddrPort if listenAddrPort is not valid,
// and ErrHandlerContextInvalidPeerAddrPort if the peer address cannot be retrieved.
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

	this.open.Store(false)
	this.handlerFD = handlerFD
	this.handler = nil
	this.commands = nil
	this.timer = nil
	this.peerAddrPort = peerAddrPort
	this.listenAddrPort = listenAddrPort
	this.handlerCounter = &atomic.Uint32{}
	this.immediateWrite = false

	return nil
}

// Reset clears all state of HandlerContext except commandQuota.
// The write buffer is emptied while keeping its capacity, and handlerFD is set to -1.
// It does not close handlerFD, so it must be called after the FD is closed.
// It must not be called concurrently with other methods.
// It is safe to call only before the handler is registered with the Reactor (before returning it
// from ClientHandlerFactory), after OnClose returns, or as the last call in OnClose.
//
// It returns ErrHandlerContextUninitialized if it was not created with NewHandlerContext.
func (this *HandlerContext) Reset() error {
	if this.writeBuffer == nil {
		return ErrHandlerContextUninitialized
	}

	if err := this.writeBuffer.Reset(); err != nil {
		return err
	}

	this.open.Store(false)
	this.handlerFD = -1
	this.handler = nil
	this.commands = nil
	// this.commandQuota = 0
	this.timer = nil
	this.peerAddrPort = netip.AddrPort{}
	this.listenAddrPort = netip.AddrPort{}
	this.handlerCounter = &atomic.Uint32{}
	this.immediateWrite = false

	return nil
}

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

// GetCommandQuota returns the command quota assigned to the Handler.
//
// It returns the configured total.
func (this *HandlerContext) GetCommandQuota() uint32 {
	return this.commandQuota
}

// context returns the HandlerContext embedded in the Handler.
//
// It returns the current HandlerContext pointer.
func (this *HandlerContext) context() *HandlerContext {
	return this
}

// bind connects the handler so that it can use the connection features of TCPReactor.
//
//   - handler: handler that receives connection events
//   - commands: command queue of TCPReactor
//   - handlerCounter: atomic that stores the number of registered handlers of TCPReactor. It must
//     not be nil.
//
// It returns nil on success, and returns an error if handler or commands is nil or if it is
// already connected.
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
	this.open.Store(true)
	return nil
}

// isBound checks whether a handler is registered.
//
// It returns true if a handler is registered, and false otherwise.
func (this *HandlerContext) isBound() bool {
	return this.handler != nil
}

// Count returns the number of connected clients.
// If multiple TCPReactors share the registration count, it returns the total of those Reactors.
//
// It returns the number of currently registered handlers, which is 0 before TCPReactor
// registration.
func (this *HandlerContext) Count() uint32 {
	if this.handlerCounter == nil {
		return 0
	}
	return this.handlerCounter.Load()
}

// PeerAddrPort returns the address and port of the client.
//
// It returns the client address and port.
func (this *HandlerContext) PeerAddrPort() netip.AddrPort {
	return this.peerAddrPort
}

// ListenAddrPort returns the address and port of the listener that accepted the connection.
//
// It returns the listen address and port.
func (this *HandlerContext) ListenAddrPort() netip.AddrPort {
	return this.listenAddrPort
}

// OnUserEvent provides the default implementation of the user event callback.
// The default implementation does nothing. To handle events, implement this method in the user
// handler.
//
//   - context: HandlerContext of the connection
//   - userEventData: event data enqueued by the user
func (this *HandlerContext) OnUserEvent(context *HandlerContext, userEventData any) {}

// OnTimeout is called when a timer expires.
// It provides the default implementation of the timer expiration callback. The default
// implementation does nothing.
//
//   - context: HandlerContext of the connection
//   - timerKey: key of the expired timer
func (this *HandlerContext) OnTimeout(context *HandlerContext, timerKey uint64) {}

// Write requests data transmission.
// Transmission is asynchronous. A nil return means success, and transmission completion is reported
// through ClientHandler.OnWritten.
// The same streamID is passed to ClientHandler.OnWritten.
// Because it stores a copy internally, the original buffer can be reused after return. An empty
// buffer is not sent.
//
//   - streamID: user-specified value that identifies the transmission
//   - buffer: data to send
//
// It returns nil on success, and returns an error if not connected, if the write request fails, if
// the command processing limit is exceeded, and so on.
// It returns ErrHandlerContextClosed before registration or if the connection is closed,
// and ErrWriteBufferFull if there is not enough space in the write prepare buffer.
func (this *HandlerContext) Write(streamID int32, buffer []byte) error {
	if !this.open.Load() {
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

// PostUserEvent requests user event processing. It can be called from external goroutines.
// For data passed with PostUserEvent, ClientHandler.OnUserEvent is called in the Reactor goroutine.
// Data passed with PostUserEvent must not be modified by external goroutines until
// ClientHandler.OnUserEvent processing finishes.
// A nil return means a successful enqueue. If the connection is closed or removed from registration
// before processing, the event is not delivered.
//
//   - userEventData: user data to deliver
//
// It returns nil on success, and returns an error if not connected, if the data is invalid, if the
// command processing limit is exceeded, and so on.
// It returns ErrHandlerContextClosed before registration or if the connection is closed,
// and ErrHandlerContextNilUserCommand if userEventData is nil or a nil pointer.
func (this *HandlerContext) PostUserEvent(userEventData any) error {
	if !this.open.Load() {
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

// SetTimeout requests setting a timer.
// If a timer with the same key exists, it is replaced with the new expiration time. Expiration is
// reported through OnTimeout.
// A timer setting is one-shot.
//
//   - timerKey: key that identifies the timer
//   - timeout: time from when this method is called until expiration
//
// It returns nil on success, and returns an error if not connected, if the command processing limit
// is exceeded, and so on.
// It returns ErrHandlerContextClosed before registration or if the connection is closed.
func (this *HandlerContext) SetTimeout(timerKey uint64, timeout time.Duration) error {
	if !this.open.Load() {
		return ErrHandlerContextClosed
	}

	if this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	return this.commands.EnqueueSetTimeout(this.handler, timerKey, time.Now().Add(timeout))
}

// UnsetTimeout requests unsetting a timer.
// Unsetting is asynchronous. A nil return means the request succeeded and does not guarantee that
// the timer has been unset immediately.
//
//   - timerKey: key of the timer to unset
//
// It returns nil on success, and returns an error if not connected, if the timer unset request
// fails, if the command processing limit is exceeded, and so on.
// It returns ErrHandlerContextClosed before registration or if the connection is closed.
func (this *HandlerContext) UnsetTimeout(timerKey uint64) error {
	if !this.open.Load() {
		return ErrHandlerContextClosed
	}

	if this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	return this.commands.EnqueueUnsetTimeout(this.handler, timerKey)
}

// Close requests a normal close of the connection and returns without waiting.
// OnClose is called after the connection is closed.
// Data passed to Write that has not yet been written to the socket is discarded.
// If received data remains unread, the peer receives RST instead of FIN.
// It is not subject to the command quota.
//
// It returns ErrHandlerContextClosed if already closed or not registered, or an error if the
// request cannot be submitted.
func (this *HandlerContext) Close() error {
	if !this.open.CompareAndSwap(true, false) {
		return ErrHandlerContextClosed
	}

	if this.handler == nil || this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	return this.commands.EnqueueClose(this.handler)
}

// Abort requests a forced close of the connection with RST and returns without waiting.
// OnClose is called after the connection is closed.
// Data not yet delivered to the peer is discarded, including data already reported by OnWritten.
// It is not subject to the command quota.
//
// It returns ErrHandlerContextClosed if already closed or not registered, or an error if the
// request cannot be submitted.
func (this *HandlerContext) Abort() error {
	if !this.open.CompareAndSwap(true, false) {
		return ErrHandlerContextClosed
	}

	if this.handler == nil || this.commands == nil {
		return ErrHandlerContextUninitialized
	}

	return this.commands.EnqueueAbort(this.handler)
}

// IsClosed checks whether the connection is closed.
//
// It returns true if closed and false if usable, and it is true even before TCPReactor
// registration.
func (this *HandlerContext) IsClosed() bool {
	return !this.open.Load()
}

// onReadable reads into readBuffer and passes received data to OnRead.
// readBuffer is shared by all connections of the Reactor, so to keep the data after OnRead,
// the callback must copy it.
// Receive processing errors are reported through OnError.
//
//   - readAll: if true, it reads all currently receivable data, and if false, it receives only
//     once.
//   - readBuffer: receive buffer of the Reactor. It must not be empty.
//
// Reading, writing, and closing the connection are all done only in the Reactor goroutine, so it
// uses handlerFD directly without rawConn.Control.
func (this *HandlerContext) onReadable(readAll bool, readBuffer []byte) {
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
			return
		}

		if recvError != nil {
			this.handler.OnError(this, os.NewSyscallError("read", recvError))
			return
		}

		if !readAll || recvBytes == 0 {
			return
		}
	}
}

// onWritable sends the data waiting to be sent to the client.
// Transmission completion is reported through OnWritten, and transmission processing errors through
// OnError.
//
// hasRemaining is true if data to send remains, and false otherwise.
//
// Reading, writing, and closing the connection are all done only in the Reactor goroutine, so it
// uses handlerFD directly without rawConn.Control.
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

// onUserEvent passes user event data to OnUserEvent.
// It does not deliver events to a closed connection.
//
//   - userEventData: user event data enqueued with PostUserEvent
func (this *HandlerContext) onUserEvent(userEventData any) {
	if this.IsClosed() {
		return
	}

	this.handler.OnUserEvent(this, userEventData)
}

// onTimeout reports timer expiration through OnTimeout.
//
// nextExpiration is the nearest next expiration time.
//
// hasTimer is true if there is a registered timer, and false otherwise.
func (this *HandlerContext) onTimeout() (nextExpiration time.Time, hasTimer bool) {
	for _, timerKey := range this.timer.PopExpired() {
		this.handler.OnTimeout(this, timerKey)
	}

	return this.timer.NextExpiration()
}

//  * Client connection close errors
// 	  errors.Is(err, net.ErrClosed) ||
// 		errors.Is(err, syscall.ECONNRESET) ||
// 		errors.Is(err, syscall.ECONNABORTED) ||
// 		errors.Is(err, syscall.ETIMEDOUT) ||
// 		errors.Is(err, syscall.ENOTCONN) ||
// 		errors.Is(err, syscall.EPIPE) ||
// 		errors.Is(err, syscall.ESHUTDOWN)

// abortConnection closes the client connection with SO_LINGER 0.
// It attempts to close the connection even if setting SO_LINGER fails.
//
// It returns nil on success, and returns an error if setting SO_LINGER or closing the connection
// fails.
func (this *HandlerContext) abortConnection() error {
	return this.closeFileDescriptor(true)
}

// closeConnection closes the client connection.
//
// It returns nil on success, and returns an error if closing the connection fails.
func (this *HandlerContext) closeConnection() error {
	return this.closeFileDescriptor(false)
}

// closeFileDescriptor closes handlerFD in the Reactor goroutine.
// If setLinger is true, it sets SO_LINGER 0 and then closes it.
func (this *HandlerContext) closeFileDescriptor(setLinger bool) error {
	var lingerError error
	if setLinger {
		linger := syscall.Linger{Onoff: 1, Linger: 0}
		lingerError = os.NewSyscallError("setsockopt", syscall.SetsockoptLinger(int(this.handlerFD), syscall.SOL_SOCKET, syscall.SO_LINGER, &linger))
	}

	return errors.Join(lingerError, os.NewSyscallError("close", syscall.Close(int(this.handlerFD))))
}

// setTimeout sets the timer for the given key.
// Registration errors are passed to OnError.
//
//   - timerKey: key that identifies the timer
//   - expiresAt: timer expiration time
//
// nextExpiration is the nearest timer expiration time.
//
// hasTimer is true if there is a registered timer, and false otherwise.
func (this *HandlerContext) setTimeout(timerKey uint64, expiresAt time.Time) (nextExpiration time.Time, hasTimer bool) {
	if err := this.timer.Register(expiresAt, timerKey); err != nil {
		this.handler.OnError(this, err)
		return this.timer.NextExpiration()
	}

	return this.timer.NextExpiration()
}

// unsetTimeout unsets the timer for the given key.
//
//   - timerKey: key of the timer to unset
//
// nextExpiration is the nearest timer expiration time.
//
// hasTimer is true if there is a registered timer, and false otherwise.
func (this *HandlerContext) unsetTimeout(timerKey uint64) (nextExpiration time.Time, hasTimer bool) {
	if !this.timer.Unregister(timerKey) {
		return this.timer.NextExpiration()
	}

	return this.timer.NextExpiration()
}

// fileDescriptor returns the file descriptor of the client connection.
// The return value cannot be used to determine whether the connection is closed.
//
// It is the file descriptor set by NewHandlerContext or Init, or -1 after Reset, and it returns
// the same value even after the connection is closed.
func (this *HandlerContext) fileDescriptor() internal.FileDescriptor {
	return this.handlerFD
}

// setClosed records only the closed state of the client connection without closing the socket.
func (this *HandlerContext) setClosed() {
	this.open.Store(false)
}
