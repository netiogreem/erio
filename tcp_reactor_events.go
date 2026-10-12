package erio

import (
	"fmt"
	"os"
	"syscall"

	"github.com/netiogreem/erio/internal"
)

// handleEvent processes the epoll events of a registered Handler in the order of read, write,
// error, read close (EPOLLRDHUP without EPOLLHUP), and hangup (EPOLLHUP).
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

	if event.Events&syscall.EPOLLIN != 0 {
		this.handleEventReadable(handler, event.Events&(syscall.EPOLLHUP|syscall.EPOLLRDHUP) != 0)
	}

	if event.Events&syscall.EPOLLOUT != 0 {
		this.handleEventWritable(handler, isClosed)
	}

	if event.Events&syscall.EPOLLERR != 0 {
		this.handleEventError(handler)
	}

	if event.Events&syscall.EPOLLRDHUP != 0 && event.Events&syscall.EPOLLHUP == 0 {
		this.handleEventReadHangup(handler)
	}

	if event.Events&syscall.EPOLLHUP != 0 {
		this.handleEventHangup(handler)
		return
	}
}

// handleEventReadable runs the receive processing of the ClientHandler with the receive buffer of
// this Reactor.
// If read returns 0, it removes only the read event (EPOLLIN) from monitoring.
//
//   - handler: ClientHandler on which the read event occurred.
//   - readAll: true if EPOLLHUP or EPOLLRDHUP was delivered together. If true, it keeps reading
//     until read returns 0 (EOF), EAGAIN, or an error, so that the data received before the FIN
//     reaches OnRead before the read close or hangup is processed.
func (this *TCPReactor) handleEventReadable(handler ClientHandler, readAll bool) {
	if handler.context().onReadable(readAll, this.readBuffer) == true {
		if err := this.epoller.UnregisterRead(handler.context().fileDescriptor()); err != nil {
			this.handleError(err)
		}
	}
}

// handleEventWritable handles write events for the ClientHandler.
// If data remains to be sent while handling a write event, it keeps the
// write event (EPOLLOUT) registered so that the next write event is received.
// If no data remains to be sent, it removes the write event, and if Close or AbortiveClose was
// requested, it enqueues the unregister command as the close event.
//
//   - handler: ClientHandler on which the write event occurred.
//   - isClosed: true if EPOLLHUP was delivered together. If true, it removes the write event
//     without sending, because handleEventHangup removes the connection in the same event.
func (this *TCPReactor) handleEventWritable(handler ClientHandler, isClosed bool) {
	if isClosed == true {
		if err := this.epoller.UnregisterWrite(handler.context().fileDescriptor()); err != nil {
			this.handleError(err)
		}
		return
	}

	if handler.context().onWritable() == true {
		return
	}

	if err := this.epoller.UnregisterWrite(handler.context().fileDescriptor()); err != nil {
		this.handleError(err)
	}

	if handler.context().isDrainedForClose() == true {
		this.commands.EnqueueUnregisterHandler(handler,
			ErrTCPReactorCloseRequested,
			handler.context().IsAbortiveClosed())
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

// handleEventReadHangup stops monitoring EPOLLRDHUP of the ClientHandler whose peer closed its
// sending side and calls OnReadClose.
// If removing the monitoring fails, it returns immediately and retries when EPOLLRDHUP is
// delivered again at the next epoll wait.
//
//   - handler: ClientHandler whose peer closed its sending side.
func (this *TCPReactor) handleEventReadHangup(handler ClientHandler) {
	if err := this.epoller.UnregisterReadHangup(handler.context().fileDescriptor()); err != nil {
		this.handleError(err)
		return
	}

	handler.OnReadClose(handler.context())
}

// removeHandler removes the ClientHandler from this Reactor and closes the connection.
// It records the closed state, removes the Handler from the mailbox registry, the client timer,
// epoll monitoring, and the Handler map, decreases the Handler counter, closes the connection with
// FIN or RST, and then calls OnClose.
//
//   - handler: registered Handler to remove from this Reactor.
//   - closeReason: connection close reason to pass to OnClose.
//   - soLinger: if true, it sets SO_LINGER 0 and closes the connection with RST (AbortiveClose and
//     the abortive close timeout); if false, it closes the connection without changing SO_LINGER.
func (this *TCPReactor) removeHandler(handler ClientHandler, closeReason error, soLinger bool) {
	fd := handler.context().fileDescriptor()

	handler.context().setClosed()
	this.commands.UnregisterHandler(handler)
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
