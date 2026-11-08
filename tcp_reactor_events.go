package erio

import (
	"fmt"
	"os"
	"syscall"

	"github.com/netiogreem/erio/internal"
)

// handleEvent dispatches one epoll event to handlers[fd].
//
// Parameters:
//   - fd is the file descriptor from event.Fd. Returns when handlers has no entry for it.
//   - event is the epoll event; event.Events selects which handleEvent* functions run.
//
// Notes:
//   - When event.Events has EPOLLHUP, handler.context().setClosed() is called first, before any other flag
//     is handled, and [tcpReactor.handleEventHangup] runs last.
//   - The flags are handled in the order EPOLLIN ([tcpReactor.handleEventReadable]), EPOLLOUT
//     ([tcpReactor.handleEventWritable]), EPOLLERR ([tcpReactor.handleEventError]), EPOLLRDHUP
//     ([tcpReactor.handleEventReadHangup]), EPOLLHUP ([tcpReactor.handleEventHangup]).
//   - handleEventReadable gets readAll true when EPOLLHUP or EPOLLRDHUP is set, so the kernel buffer is
//     read in one pass before the hangup is handled.
//   - handleEventReadHangup runs only when EPOLLRDHUP is set and EPOLLHUP is not.
//   - handleEventWritable gets isClosed true when EPOLLHUP is set.
func (this *tcpReactor) handleEvent(fd internal.FileDescriptor, event syscall.EpollEvent) {
	handler, contains := this.handlers[fd]
	if !contains {
		return
	}

	isClosed := false
	if event.Events&syscall.EPOLLHUP != 0 {

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

// handleEventReadable calls handler.context().onReadable(readAll, readBuffer).
//
// Parameters:
//   - handler is handlers[fd] for the socket that reported EPOLLIN.
//   - readAll is passed to [HandlerContext.onReadable]. true makes it read until the socket is drained.
//
// Notes:
//   - When onReadable returns true (read returned 0, the peer closed), epoller.UnregisterRead(fd) disarms
//     EPOLLIN. The UnregisterRead error is passed to [tcpReactor.handleError].
func (this *tcpReactor) handleEventReadable(handler ClientHandler, readAll bool) {
	if handler.context().onReadable(readAll, this.readBuffer) == true {
		if err := this.epoller.UnregisterRead(handler.context().fileDescriptor()); err != nil {
			this.handleError(err)
		}
	}
}

// handleEventWritable calls handler.context().onWritable() to write writeBuffer to the socket.
//
// Parameters:
//   - handler is handlers[fd] for the socket that reported EPOLLOUT.
//   - isClosed is true when the same event had EPOLLHUP. epoller.UnregisterWrite(fd) is then called
//     without writing.
//
// Notes:
//   - When onWritable returns true (data remains), EPOLLOUT stays armed and nothing else is done.
//   - When onWritable returns false, epoller.UnregisterWrite(fd) disarms EPOLLOUT.
//   - When onWritable returned false and [HandlerContext.isDrainedForClose] is true,
//     [tcpReactor.removeHandler] is not called here. Instead commands.EnqueueUnregisterHandler(handler,
//     [ErrTCPReactorCloseRequested], handler.context().IsAbortiveClosed()) is called. The loop later drains
//     that command in [tcpReactor.handleCommands] and runs [tcpReactor.handleCommandUnregisterHandler], which
//     calls removeHandler: the socket is closed and [ClientHandler.OnClose] receives
//     [ErrTCPReactorCloseRequested]. Commands for this handler that were queued earlier are drained first.
//   - The EnqueueUnregisterHandler error is ignored. Calling removeHandler here would skip those earlier
//     commands. The handler is removed later by [tcpReactor.handleEventHangup], by
//     [tcpReactor.handleTimeouts] when abortiveCloseDeadline passes, or by [tcpReactor.cleanup].
//   - An UnregisterWrite error is passed to [tcpReactor.handleError].
func (this *tcpReactor) handleEventWritable(handler ClientHandler, isClosed bool) {
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

// handleEventError reads SO_ERROR of the socket with syscall.GetsockoptInt and reports it to the handler.
//
// Parameters:
//   - handler is handlers[fd] for the socket that reported EPOLLERR.
//
// Notes:
//   - When SO_ERROR is not 0, handler.OnError receives it as [syscall.Errno].
//   - When GetsockoptInt itself fails, its error is wrapped as "fd %d: getsockopt" and passed to
//     [tcpReactor.handleError]. handler.OnError is not called.
func (this *tcpReactor) handleEventError(handler ClientHandler) {
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

// handleEventHangup calls [tcpReactor.removeHandler](handler, [ErrTCPReactorHangup], false).
//
// Parameters:
//   - handler is handlers[fd] for the socket that reported EPOLLHUP.
//
// Notes:
//   - soLinger false closes the socket with [HandlerContext.closeConnection], and [ClientHandler.OnClose]
//     receives [ErrTCPReactorHangup] as closeReason.
func (this *tcpReactor) handleEventHangup(handler ClientHandler) {
	this.removeHandler(handler, ErrTCPReactorHangup, false)
}

// handleEventReadHangup calls epoller.UnregisterReadHangup(fd) and then handler.OnReadClose.
//
// Parameters:
//   - handler is handlers[fd] for the socket that reported EPOLLRDHUP without EPOLLHUP, that is, the peer
//     shut down its write side.
//
// Notes:
//   - When UnregisterReadHangup returns an error, it is passed to [tcpReactor.handleError] and OnReadClose
//     is not called.
func (this *tcpReactor) handleEventReadHangup(handler ClientHandler) {
	if err := this.epoller.UnregisterReadHangup(handler.context().fileDescriptor()); err != nil {
		this.handleError(err)
		return
	}

	handler.OnReadClose(handler.context())
}

// removeHandler detaches handler from the reactor, closes the socket, and calls handler.OnClose.
//
// Parameters:
//   - handler is the handler to remove. It must be handlers[handler.context().fileDescriptor()].
//   - closeReason is passed to [ClientHandler.OnClose] as closeReason.
//   - soLinger true calls [HandlerContext.abortConnection] (SO_LINGER 0, RST); false calls
//     [HandlerContext.closeConnection].
//
// Notes:
//   - The order is: handler.context().setClosed(), commands.UnregisterHandler(handler),
//     clientTimer.Unregister(handler), epoller.Unregister(fd), delete(handlers, fd),
//     handlerCounter.Add(^uint32(0)), abortConnection or closeConnection, handler.OnClose.
//   - The epoller.Unregister error and the abortConnection or closeConnection error are passed to
//     [tcpReactor.handleError].
//
// Caution:
//   - handlerCounter.Add(^uint32(0)) runs unconditionally, so calling this for a handler not in handlers
//     makes the count wrong.
func (this *tcpReactor) removeHandler(handler ClientHandler, closeReason error, soLinger bool) {
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
