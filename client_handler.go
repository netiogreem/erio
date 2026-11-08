package erio

import (
	"net/netip"

	"github.com/netiogreem/erio/internal"
)

// FileDescriptor is the socket descriptor type used by the public API. It is an alias of
// [internal.FileDescriptor].
type FileDescriptor = internal.FileDescriptor

// ClientHandlerFactory creates the [ClientHandler] for an accepted connection.
//
// Parameters:
//   - fd is the socket of the accepted connection. Pass it to [NewHandlerContext] or
//     [HandlerContext.Init] so the handler's context owns it.
//   - listenAddress is the local address the connection was accepted on.
//
// Returns:
//   - The handler to register with the reactor. Its context must be initialized with fd.
//   - An error when no handler can be created for the connection.
//
// Caution:
//   - When the factory returns an error or a nil handler, the acceptor closes fd. The factory must not close it.
type ClientHandlerFactory func(fd FileDescriptor, listenAddress netip.AddrPort) (ClientHandler, error)

// ClientHandler receives the events of one connection from a [TCPReactor]. Implement it by
// embedding [HandlerContext] in a handler struct and overriding the callbacks the handler needs.
//
// Notes:
//   - Every callback runs on the event loop goroutine of the reactor the handler is registered with.
//   - [HandlerContext] methods may be called inside any callback.
//   - After [ClientHandler.OnClose] no further callback is made for the connection.
//
// Caution:
//   - A callback must not block. The event loop handles every connection of the reactor on one goroutine,
//     so a blocking callback stalls all of them.
type ClientHandler interface {
	// OnConnect is called once after the handler is registered with the reactor and its socket is
	// armed for reading.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext]. The connection is open from this point.
	OnConnect(context *HandlerContext)

	// OnRead is called for each chunk of data read from the socket.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext].
	//   - receivedData is the data read, at most the reactor's readBufferSize bytes per call.
	//
	// Caution:
	//   - receivedData is a slice of the read buffer shared by every handler on the reactor and is valid
	//     only until OnRead returns. Copy it to keep the data.
	OnRead(context *HandlerContext, receivedData []byte)

	// OnWritten is called when a buffer given to [HandlerContext.Write] has been handed to the kernel completely.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext].
	//   - streamID is the value given to [HandlerContext.Write] for the buffer.
	//   - writtenBytes is the length of the buffer.
	//
	// Notes:
	//   - The data is in the kernel send buffer, not necessarily received by the peer.
	//     [HandlerContext.AbortiveClose] discards data that has not reached the peer yet.
	OnWritten(context *HandlerContext, streamID int32, writtenBytes uint32)

	// OnUserEvent is called for each [HandlerContext.PostUserEvent] request.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext].
	//   - userEventData is the value given to [HandlerContext.PostUserEvent], unchanged.
	OnUserEvent(context *HandlerContext, userEventData any)

	// OnWake is called after [HandlerContext.Wake].
	//
	// Parameters:
	//   - context is the handler's [HandlerContext].
	//
	// Notes:
	//   - Wake calls made before OnWake starts are combined into one call.
	OnWake(context *HandlerContext)

	// OnTimeout is called when a timer set by [HandlerContext.SetTimeout] expires.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext].
	//   - timerKey is the key given to [HandlerContext.SetTimeout].
	//
	// Notes:
	//   - A timer fires once. Call [HandlerContext.SetTimeout] again for a repeating timer.
	OnTimeout(context *HandlerContext, timerKey uint64)

	// OnError reports an error that occurred while the reactor was handling the connection.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext].
	//   - clientError is one of: a [syscall.Errno] from the socket's SO_ERROR, a read or write system
	//     call error, an epoll or timer registration error, or a registration failure described below.
	//
	// Notes:
	//   - The reactor does not close the connection because of an error reported here, except for a
	//     registration failure.
	//   - A registration failure is reported with [ErrTCPReactorInvalidFileDescriptor], [ErrTCPReactorDuplicateFD],
	//     or an error joined with the abort error for a bind failure, an epoll failure, or [ErrTCPReactorStopped].
	//
	// Caution:
	//   - After a registration failure, OnConnect and [ClientHandler.OnClose] are never called. With
	//     [ErrTCPReactorInvalidFileDescriptor] or [ErrTCPReactorDuplicateFD] the socket is left open; in the
	//     other cases it is already closed.
	OnError(context *HandlerContext, clientError error)

	// OnReadClose is called when the peer shut down its write side and no more data will arrive.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext]. The connection is still open and can be written to.
	//
	// Notes:
	//   - Not called when the hangup arrives together with the peer's full close; [ClientHandler.OnClose]
	//     with [ErrTCPReactorHangup] is called instead.
	//   - The default [HandlerContext.OnReadClose] requests [HandlerContext.Close].
	OnReadClose(context *HandlerContext)

	// OnClose is called last, after the socket has been closed and the handler has been removed from the reactor.
	//
	// Parameters:
	//   - context is the handler's [HandlerContext]. It may be reused with [HandlerContext.Reset] and
	//     [HandlerContext.Init] inside this call.
	//   - closeReason is [ErrTCPReactorCloseRequested], [ErrTCPReactorAbortiveClose],
	//     [ErrTCPReactorAbortiveCloseTimeout], [ErrTCPReactorHangup], or [ErrTCPReactorStopped].
	OnClose(context *HandlerContext, closeReason error)

	// GetCommandQuota returns how many quota-limited requests the handler may queue per event loop
	// iteration. [HandlerContext] implements it with the value given to [NewHandlerContext].
	//
	// Returns:
	//   - The quota shared by [HandlerContext.Write], [HandlerContext.PostUserEvent], [HandlerContext.SetTimeout],
	//     and [HandlerContext.UnsetTimeout].
	//
	// Notes:
	//   - The reactor reads the quota from the embedded [HandlerContext] directly, so overriding this
	//     method does not change the limit.
	GetCommandQuota() uint32

	// context returns the handler's [HandlerContext]. Being unexported, it is satisfied only by
	// embedding [HandlerContext].
	//
	// Returns:
	//   - The embedded context. It must not be nil; [TCPReactor.RegisterHandler] rejects a nil context.
	context() *HandlerContext
}
