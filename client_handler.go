package erio

import (
	"github.com/netiogreem/erio/internal"
	"net/netip"
)

// FileDescriptor is the file descriptor of a connection socket, and is an alias of
// internal.FileDescriptor (int32).
type FileDescriptor = internal.FileDescriptor

// ClientHandlerFactory is the ClientHandler factory function that Acceptor calls for each accepted connection.
// fd is the FileDescriptor of the connection, and listenAddress is the listen address that accepted the connection.
// If it returns an error or a nil Handler, Acceptor rejects the connection.
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
	//
	//   - context: HandlerContext of the connection
	//   - timerKey: key of the expired timer
	OnTimeout(context *HandlerContext, timerKey uint64)

	// OnReadClosed is called when the peer closes its sending side (half-close).
	// The connection can still send, and no more data is received.
	// HandlerContext provides a default implementation that calls Close, so the connection is
	// closed after the remaining data is sent.
	// To keep the connection and continue sending, implement this method and call Close when
	// sending is finished.
	//
	//   - context: HandlerContext of the connection
	OnReadClosed(context *HandlerContext)

	// OnError is called when an error occurs while handling an event.
	//
	//   - context: HandlerContext of the connection
	//   - clientError: error that occurred
	//
	// If TCPReactor registration fails, only OnError is called, and OnConnect and OnClose are not called.
	// Send and receive errors do not close the connection. If the connection is lost, it is cleaned
	// up with OnClose(ErrTCPReactorHangup). To close the connection after checking the error, call
	// Close or AbortiveClose.
	OnError(context *HandlerContext, clientError error)

	// OnClose is called when the connection is lost and detached from the Reactor.
	//
	//   - context: HandlerContext of the connection
	//   - closeReason: reason the connection was closed
	//
	// closeReason is the situation at the time the connection is closed.
	//   - ErrTCPReactorCloseRequested: Close closed the connection, including Close called by the
	//     default OnReadClosed. The remaining data is sent before closing, but if registering
	//     write monitoring fails, OnError is called and the connection is closed without sending
	//     the remaining data.
	//   - ErrTCPReactorAbortiveCloseRequested: AbortiveClose closed the connection with RST.
	//   - ErrTCPReactorClosePendingWriteTimeout: the remaining data was not sent within
	//     closePendingWriteTimeout after Close, and the connection was closed with RST.
	//   - ErrTCPReactorHangup: the connection was lost.
	//   - ErrTCPReactorStopped: TCPReactor was stopped.
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
