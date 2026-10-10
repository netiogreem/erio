package erio

import (
	"fmt"
	"os"
	"syscall"

	"github.com/netiogreem/erio/internal"
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
	if event.Events&syscall.EPOLLHUP != 0 {
		// It is recorded first so that the receive callback for the remaining data can also check
		// the closed state.
		handler.context().setClosed()
		isClosed = true
	}

	// If the peer closed its sending side, all remaining data is read before OnReadClosed.
	readAll := event.Events&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0
	if event.Events&syscall.EPOLLIN != 0 {
		this.handleEventReadable(handler, readAll)
	}

	if event.Events&syscall.EPOLLOUT != 0 {
		hasRemaining := this.handleEventWritable(handler, isClosed)

		// Close sends the remaining data before closing so that a response written after the peer's
		// half-close is not discarded, and the handler is removed once all remaining data is sent.
		if hasRemaining == false && handler.context().isClosing() {
			this.removeHandler(handler, ErrTCPReactorCloseRequested, false)
			return
		}
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

// handleEventReadable runs the receive processing of the ClientHandler with the receive buffer of
// this Reactor.
//
//   - handler: ClientHandler on which the read event occurred.
//   - readAll: true if a close event was delivered together, so that all remaining data is read.
func (this *TCPReactor) handleEventReadable(handler ClientHandler, readAll bool) {
	handler.context().onReadable(readAll, this.readBuffer)
}

// handleEventWritable handles write events for the ClientHandler.
// If data remains to be sent while handling a write event, it keeps the
// write event (EPOLLOUT) registered so that the next write event is received.
// If no data remains to be sent, it removes the write event.
//
//   - handler: ClientHandler on which the write event occurred.
//   - isClosed: true if a close event was delivered together.
//
// hasRemaining is true if unsent data remains and write monitoring is kept, and false if no data
// remains or isClosed is true.
func (this *TCPReactor) handleEventWritable(handler ClientHandler, isClosed bool) (hasRemaining bool) {
	// If unsent data remains, monitoring is kept so that the next write event is received.
	if isClosed == false && handler.context().onWritable() {
		return true
	}

	if err := this.epoller.UnregisterWrite(handler.context().fileDescriptor()); err != nil {
		this.handleError(err)
	}

	return false
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

// handleEventReadHangup stops receiving for the ClientHandler whose peer closed its sending side
// and calls OnReadClosed, keeping the connection so that it can still send.
// If Close or AbortiveClose was already requested, it does not call OnReadClosed.
//
//   - handler: ClientHandler whose peer closed its sending side.
func (this *TCPReactor) handleEventReadHangup(handler ClientHandler) {
	// Read monitoring is removed so that level-triggered EPOLLIN and EPOLLRDHUP are not repeated.
	if err := this.epoller.UnregisterReadWithHangup(handler.context().fileDescriptor()); err != nil {
		this.handleError(err)
	}

	if !handler.context().setReadClosed() {
		return
	}

	handler.OnReadClosed(handler.context())
}

// removeHandler calls OnClose of the ClientHandler.
//
//   - handler: registered Handler to remove from this Reactor.
//   - closeReason: connection close reason to pass to OnClose.
//   - soLinger: if true, it sets SO_LINGER 0 and closes the connection (HandlerContext.AbortiveClose);
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
