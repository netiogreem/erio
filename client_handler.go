package erio

import (
	"net/netip"

	"github.com/netiogreem/erio/internal"
)

// FileDescriptor is the file descriptor of a connection socket, and is an alias of
// internal.FileDescriptor (int32).
type FileDescriptor = internal.FileDescriptor

// ClientHandlerFactory is the ClientHandler factory function that Acceptor calls for each accepted connection.
// fd is the FileDescriptor of the connection, and listenAddress is the listen address that accepted the connection.
// If it returns an error, a nil Handler, or a typed nil Handler, Acceptor rejects the connection.
type ClientHandlerFactory func(fd FileDescriptor, listenAddress netip.AddrPort) (ClientHandler, error)

// ClientHandler is the user-facing callback interface for handling client events in a TCPReactor.
// Users implement a handler that embeds HandlerContext.
// The handler is registered with the TCPReactor via ClientHandlerFactory,
// and when a socket event occurs, the TCPReactor calls the handler's callback for that event.
type ClientHandler interface {
	// OnConnect is called when the connection is established and registered with the Reactor.
	//
	//   - context: HandlerContext of the connection
	OnConnect(context *HandlerContext)

	// OnRead is called when data is received.
	// To keep the data after the callback, receivedData must be copied.
	//
	//   - context: HandlerContext of the connection
	//   - receivedData: received data
	OnRead(context *HandlerContext, receivedData []byte)

	// OnWritten is called when the data requested with Write has been written to the kernel.
	//
	//   - context: HandlerContext of the connection
	//   - streamID: streamID defined by the user in the Write call
	//   - writtenBytes: number of bytes written
	OnWritten(context *HandlerContext, streamID int32, writtenBytes uint32)

	// OnUserEvent processes user event data submitted via PostUserEvent.
	// It is optional for the user; HandlerContext provides an empty implementation.
	//
	//   - context: HandlerContext of the connection
	//   - userEventData: event data submitted by the user
	OnUserEvent(context *HandlerContext, userEventData any)

	// OnTimeout is called when a timer expires.
	// It is optional for the user; HandlerContext provides an empty implementation.
	//
	//   - context: HandlerContext of the connection
	//   - timerKey: key of the expired timer
	OnTimeout(context *HandlerContext, timerKey uint64)

	// OnError is called when an error occurs while handling an event.
	//
	//   - context: HandlerContext of the connection
	//   - clientError: error that occurred
	//
	// If TCPReactor registration fails, only OnError is called, and OnConnect and OnClose are not called.
	// If TCPReactor.RegisterHandler itself returns an error, no ClientHandler callback is called, and
	// the error is passed to the callback set with WithAcceptErrorCallback.
	OnError(context *HandlerContext, clientError error)

	// OnReadClose is called in the following cases:
	//  the client called CloseWrite
	//  the client called Close after receiving all data
	//
	// The server reads all data that the client sent before calling CloseWrite or Close
	// and passes it to OnRead before calling OnReadClose.
	// The connection remains open, so Write can still be used.
	// HandlerContext's default implementation calls Close to send the pending data
	// and then close the connection.
	//
	//   - context: HandlerContext of the connection
	OnReadClose(context *HandlerContext)

	// OnClose is called after the Reactor detaches the ClientHandler and calls close(handlerFD).
	//
	//   - context: HandlerContext of the connection
	//   - closeReason: reason the connection was closed
	//
	// closeReason is ErrTCPReactorCloseRequested if Close completes, ErrTCPReactorAbortiveClose if
	// AbortiveClose is processed, ErrTCPReactorAbortiveCloseTimeout if the pending write data is not
	// sent within abortiveCloseTimeout after Close, ErrTCPReactorHangup if the connection is lost, and
	// ErrTCPReactorStopped if TCPReactor is stopped.
	OnClose(context *HandlerContext, closeReason error)

	// GetCommandQuota returns the command limit.
	// It is a function implemented by HandlerContext.
	//
	// It returns the limit.
	GetCommandQuota() uint32

	// context returns the HandlerContext embedded in the Handler.
	//
	// It returns the HandlerContext pointer of the connection.
	context() *HandlerContext
}
