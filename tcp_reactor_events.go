package erio

import (
	"fmt"
	"github.com/netiogreem/erio/internal"
	"os"
	"syscall"
)

// handleEvent processes the epoll events of a registered Handler in the order of read, write,
// error, and close.
//
//   - fd: file descriptor on which the event occurred.
//   - event: event delivered by epoll.
func (this *TCPReactor) handleEvent(fd internal.FileDescriptor, event syscall.EpollEvent) {
	handler, contains := this.handlers[fd]
	if !contains {
		return
	}

	isClosed := false
	if event.Events&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0 {
		// It is recorded first so that the receive callback for the remaining data can also check
		// the closed state.
		handler.context().setClosed()
		isClosed = true
	}

	if event.Events&syscall.EPOLLIN != 0 {
		this.handleEventReadable(handler, isClosed)
	}

	if event.Events&syscall.EPOLLOUT != 0 {
		this.handleEventWritable(handler, isClosed)
	}

	if event.Events&syscall.EPOLLERR != 0 {
		this.handleEventError(handler)
	}

	if event.Events&syscall.EPOLLHUP != 0 {
		this.handleEventHangup(handler)
		return
	}

	if event.Events&syscall.EPOLLRDHUP != 0 {
		this.handleEventReadHangup(handler)
	}
}

// handleEventReadable runs the receive processing of the ClientHandler.
//
//   - handler: ClientHandler on which the read event occurred.
//   - isClosed: true if a close event was delivered together.
func (this *TCPReactor) handleEventReadable(handler ClientHandler, isClosed bool) {
	handler.context().onReadable(isClosed == true)
}

// handleEventWritable handles write events for the ClientHandler.
// If data remains to be sent while handling a write event, it keeps the
// write event (EPOLLOUT) registered so that the next write event is received.
// If no data remains to be sent, it removes the write event.
//
//   - handler: ClientHandler on which the write event occurred.
//   - isClosed: true if a close event was delivered together.
func (this *TCPReactor) handleEventWritable(handler ClientHandler, isClosed bool) {
	// If unsent data remains, monitoring is kept so that the next write event is received.
	if isClosed == false && handler.context().onWritable() {
		return
	}

	if err := this.epoller.UnregisterWrite(handler.context().fileDescriptor()); err != nil {
		this.handleError(err)
		return
	}
}

// handleEventError retrieves the pending error of the socket and passes it to the ClientHandler.
// If retrieving the error itself fails, it is passed to the Reactor error callback.
//
//   - handler: ClientHandler on which the error event occurred.
func (this *TCPReactor) handleEventError(handler ClientHandler) {
	fd := handler.context().fileDescriptor()

	socketError, optionError := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_ERROR)
	if optionError != nil {
		this.handleError(fmt.Errorf("fd %d: %w", fd, os.NewSyscallError("getsockopt", optionError)))
		return
	}

	if socketError != 0 {
		handler.OnError(handler.context(), syscall.Errno(socketError))
	}
}

// handleEventHangup removes the ClientHandler on which a disconnection occurred.
//
//   - handler: ClientHandler to remove.
func (this *TCPReactor) handleEventHangup(handler ClientHandler) {
	this.removeHandler(handler, ErrTCPReactorHangup, false)
}

// handleEventReadHangup removes the ClientHandler on which a read-side close event occurred and
// closes the connection immediately.
//
//   - handler: ClientHandler to remove.
func (this *TCPReactor) handleEventReadHangup(handler ClientHandler) {
	this.removeHandler(handler, ErrTCPReactorReadHangup, true)
}

// removeHandler calls OnClose of the ClientHandler.
//
//   - handler: registered Handler to remove from this Reactor.
//   - closeReason: connection close reason to pass to OnClose.
//   - soLinger: if true, it applies the configured SO_LINGER value and closes the connection;
//     if false, it closes the connection without changing SO_LINGER.
func (this *TCPReactor) removeHandler(handler ClientHandler, closeReason error, soLinger bool) {
	fd := handler.context().fileDescriptor()

	handler.context().setClosed()
	this.clientTimer.Unregister(handler)
	if err := this.epoller.Unregister(fd); err != nil {
		this.handleError(err)
	}
	delete(this.handlers, fd)
	this.handlerCounter.Add(^uint32(0))

	if soLinger {
		if err := handler.context().abortConnection(); err != nil {
			this.handleError(err)
		}
	} else {
		if err := handler.context().closeConnection(); err != nil {
			this.handleError(err)
		}
	}

	handler.OnClose(handler.context(), closeReason)
}
