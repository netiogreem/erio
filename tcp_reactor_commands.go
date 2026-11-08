package erio

import (
	"errors"
	"time"
)

// handleCommands calls commands.Drain(handleError) and dispatches each command by commandType to the
// matching handleCommand* function.
//
// Returns:
//   - true when a commandStop was in the batch.
//   - false otherwise.
//
// Notes:
//   - commandStop sets isStop and the remaining commands in the batch are still dispatched.
//   - A command whose handler is nil is passed to [tcpReactor.handleError] as [ErrTCPReactorNilHandler] and skipped.
//   - For every commandType except commandRegisterHandler, the command is skipped when
//     handlers[command.handler.context().fileDescriptor()] is missing or is a different handler. This is how
//     commands queued for a handler that [tcpReactor.removeHandler] already removed are discarded.
//   - A command with an unknown commandType is passed to handleError as [ErrTCPReactorCommand].
//   - The drained slice is cleared at the end because [commandMailbox.Drain] reuses it.
func (this *tcpReactor) handleCommands() (isStop bool) {
	isStop = false

	commands := this.commands.Drain(this.handleError)
	for _, command := range commands {
		if command.commandType == commandStop {
			isStop = true
			continue
		}

		if command.handler == nil {
			this.handleError(ErrTCPReactorNilHandler)
			continue
		}

		if command.commandType != commandRegisterHandler {
			handler, contains := this.handlers[command.handler.context().fileDescriptor()]
			if !contains || handler != command.handler {
				continue
			}
		}

		switch command.commandType {
		case commandRegisterHandler:
			this.handleCommandRegisterHandler(command.handler)

		case commandSetTimeout:
			this.handleCommandSetTimeout(command.handler, command.timerKey, command.expiresAt)

		case commandUnsetTimeout:
			this.handleCommandUnsetTimeout(command.handler, command.timerKey)

		case commandWrite:
			this.handleCommandWrite(command.handler)

		case commandUser:
			this.handleCommandUser(command.handler, command.userEventData)

		case commandWake:
			this.handleCommandWake(command.handler)

		case commandClose:
			this.handleCommandClose(command.handler, command.expiresAt)

		case commandAbortiveClose:
			this.handleCommandAbortiveClose(command.handler)

		case commandUnregisterHandler:
			this.handleCommandUnregisterHandler(command.handler, command.reason, command.soLinger)

		default:
			this.handleError(ErrTCPReactorCommand)
		}
	}

	clear(commands)
	return isStop
}

// handleCommandRegisterHandler calls handler.context().bind, epoller.RegisterReadWithHangup(fd), adds handler to
// handlers, increments handlerCounter, and calls handler.OnConnect.
//
// Parameters:
//   - handler is command.handler of a commandRegisterHandler. Its socket is open and not yet in handlers.
//
// Returns:
//   - nil when handler was added to handlers and OnConnect was called.
//   - [ErrTCPReactorInvalidFileDescriptor] when handler.context().fileDescriptor() is negative.
//   - [ErrTCPReactorDuplicateFD] when handlers already has that file descriptor or it equals commandFD.
//   - The [HandlerContext.bind] error or the epoller.RegisterReadWithHangup error.
//
// Notes:
//   - On every error handler.OnError receives it, and the deferred commands.UnregisterHandler(handler) stops
//     accepting commands for the handler. OnConnect and OnClose are not called.
//   - On ErrTCPReactorInvalidFileDescriptor and ErrTCPReactorDuplicateFD the socket is left open. On a bind
//     or RegisterReadWithHangup error [tcpReactor.rejectHandler] closes it with RST.
//   - [tcpReactor.handleCommands] discards the return value.
func (this *tcpReactor) handleCommandRegisterHandler(handler ClientHandler) (err error) {
	fd := handler.context().fileDescriptor()

	defer func() {
		if err != nil {
			this.commands.UnregisterHandler(handler)
		}
	}()

	if fd < 0 {
		err = ErrTCPReactorInvalidFileDescriptor
		handler.OnError(handler.context(), ErrTCPReactorInvalidFileDescriptor)
		return
	}

	if _, contains := this.handlers[fd]; contains || fd == this.commandFD {
		err = ErrTCPReactorDuplicateFD
		handler.OnError(handler.context(), err)
		return
	}

	if err = handler.context().bind(handler, this.commands, this.handlerCounter); err != nil {
		this.rejectHandler(handler, err)
		return
	}

	if err = this.epoller.RegisterReadWithHangup(fd); err != nil {
		this.rejectHandler(handler, err)
		return
	}

	this.handlers[fd] = handler
	this.handlerCounter.Add(1)
	handler.OnConnect(handler.context())
	return nil
}

// handleCommandUnregisterHandler calls [tcpReactor.removeHandler] for a commandUnregisterHandler.
//
// Parameters:
//   - handler is command.handler.
//   - reason is command.reason, passed to removeHandler as closeReason.
//   - soLinger is command.soLinger, passed to removeHandler.
//
// Notes:
//   - When handler.context().IsAbortiveClosed() is true, removeHandler is called with
//     [ErrTCPReactorAbortiveClose] and soLinger true instead, so an AbortiveClose requested after the
//     command was queued still wins.
func (this *tcpReactor) handleCommandUnregisterHandler(handler ClientHandler, reason error, soLinger bool) {
	if handler.context().IsAbortiveClosed() {
		this.removeHandler(handler, ErrTCPReactorAbortiveClose, handler.context().IsAbortiveClosed())
		return
	}
	this.removeHandler(handler, reason, soLinger)
}

// rejectHandler calls handler.context().setClosed() and handler.context().abortConnection(), then passes
// errors.Join(abort error, reason) to handler.OnError.
//
// Parameters:
//   - handler is the handler whose registration failed.
//   - reason is the registration error.
//
// Caution:
//   - Call only for handlers that were never added to handlers. handlers, handlerCounter, clientTimer, and
//     epoller are not touched, and [ClientHandler.OnClose] is not called.
func (this *tcpReactor) rejectHandler(handler ClientHandler, reason error) {
	handler.context().setClosed()
	handler.OnError(handler.context(), errors.Join(handler.context().abortConnection(), reason))
}

// handleCommandSetTimeout calls handler.context().onSetTimeout(timerKey, expiresAt) and passes its result to
// [tcpReactor.updateClientTimer].
//
// Parameters:
//   - handler is command.handler.
//   - timerKey is command.timerKey, the key given to [HandlerContext.SetTimeout].
//   - expiresAt is command.expiresAt, the time computed by [HandlerContext.SetTimeout].
func (this *tcpReactor) handleCommandSetTimeout(handler ClientHandler, timerKey uint64, expiresAt time.Time) {
	nextExpiration, hasTimer := handler.context().onSetTimeout(timerKey, expiresAt)
	this.updateClientTimer(handler, nextExpiration, hasTimer)
}

// handleCommandUnsetTimeout calls handler.context().onUnsetTimeout(timerKey) and passes its result to
// [tcpReactor.updateClientTimer].
//
// Parameters:
//   - handler is command.handler.
//   - timerKey is command.timerKey, the key given to [HandlerContext.UnsetTimeout].
func (this *tcpReactor) handleCommandUnsetTimeout(handler ClientHandler, timerKey uint64) {
	nextExpiration, hasTimer := handler.context().onUnsetTimeout(timerKey)
	this.updateClientTimer(handler, nextExpiration, hasTimer)
}

// handleCommandWrite calls epoller.RegisterWrite(fd) so [tcpReactor.handleEventWritable] writes the
// handler's writeBuffer on the next EPOLLOUT.
//
// Parameters:
//   - handler is command.handler of a commandWrite queued by [HandlerContext.Write] outside the immediate
//     write window.
//
// Notes:
//   - The RegisterWrite error is passed to handler.OnError.
func (this *tcpReactor) handleCommandWrite(handler ClientHandler) {
	if err := this.epoller.RegisterWrite(handler.context().fileDescriptor()); err != nil {
		handler.OnError(handler.context(), err)
	}
}

// handleCommandClose handles the commandClose queued by [HandlerContext.Close].
//
// Parameters:
//   - handler is command.handler.
//   - abortiveCloseDeadline is command.expiresAt, computed by [HandlerContext.Close] from
//     abortiveCloseTimeout. Zero when abortiveCloseTimeout is 0.
//
// Notes:
//   - Returns when handler.context().close is not [closeStateClose], that is, when AbortiveClose was
//     requested after Close.
//   - When [HandlerContext.isDrainedForClose] is true, commands.EnqueueUnregisterHandler(handler,
//     [ErrTCPReactorCloseRequested], false) is called at once. The loop later runs
//     [tcpReactor.handleCommandUnregisterHandler], which calls [tcpReactor.removeHandler]: the socket is
//     closed and [ClientHandler.OnClose] receives [ErrTCPReactorCloseRequested].
//   - When that EnqueueUnregisterHandler returns an error, handler.OnError receives it and removeHandler is
//     called directly. Direct removal is safe here because this runs while the commandClose is being
//     processed, so every command queued before it has already been handled.
//   - Otherwise handler.context().setAbortiveCloseDeadline(abortiveCloseDeadline) stores the deadline and
//     its result is passed to [tcpReactor.updateClientTimer]. The remaining writeBuffer is sent by
//     [tcpReactor.handleEventWritable] or [tcpReactor.writeImmediately], which queue the removal when it is empty.
func (this *tcpReactor) handleCommandClose(handler ClientHandler, abortiveCloseDeadline time.Time) {
	if handler.context().close.Load() != closeStateClose {
		return
	}

	if handler.context().isDrainedForClose() == true {
		if err := this.commands.EnqueueUnregisterHandler(handler, ErrTCPReactorCloseRequested, false); err != nil {
			handler.OnError(handler.context(), err)
			this.removeHandler(handler, ErrTCPReactorCloseRequested, false)
		}
		return
	}

	nextExpiration, hasTimer := handler.context().setAbortiveCloseDeadline(abortiveCloseDeadline)
	this.updateClientTimer(handler, nextExpiration, hasTimer)
}

// handleCommandAbortiveClose handles the commandAbortiveClose queued by [HandlerContext.AbortiveClose].
//
// Parameters:
//   - handler is command.handler.
//
// Notes:
//   - Returns when handler.context().close is not [closeStateAbort].
//   - Calls commands.EnqueueUnregisterHandler(handler, [ErrTCPReactorAbortiveClose], true). The loop later
//     runs [tcpReactor.handleCommandUnregisterHandler], which calls [tcpReactor.removeHandler] with soLinger
//     true: the socket is closed with RST and [ClientHandler.OnClose] receives [ErrTCPReactorAbortiveClose].
//   - When EnqueueUnregisterHandler returns an error, handler.OnError receives it and removeHandler is called
//     directly. Direct removal is safe here because this runs while the commandAbortiveClose is being
//     processed, so every command queued before it has already been handled.
func (this *tcpReactor) handleCommandAbortiveClose(handler ClientHandler) {
	if handler.context().close.Load() != closeStateAbort {
		return
	}

	if err := this.commands.EnqueueUnregisterHandler(handler, ErrTCPReactorAbortiveClose, true); err != nil {
		handler.OnError(handler.context(), err)
		this.removeHandler(handler, ErrTCPReactorAbortiveClose, true)
	}
}

// handleCommandUser calls handler.context().onUserEvent(userEventData), which calls [ClientHandler.OnUserEvent].
//
// Parameters:
//   - handler is command.handler.
//   - userEventData is command.userEventData, the value given to [HandlerContext.PostUserEvent].
func (this *tcpReactor) handleCommandUser(handler ClientHandler, userEventData any) {
	handler.context().onUserEvent(userEventData)
}

// handleCommandWake calls handler.context().onWake(), which clears the wake flag and calls [ClientHandler.OnWake].
//
// Parameters:
//   - handler is command.handler of the commandWake queued by [HandlerContext.Wake].
func (this *tcpReactor) handleCommandWake(handler ClientHandler) {
	handler.context().onWake()
}

// updateClientTimer calls clientTimer.Register(expiresAt, handler), or clientTimer.Unregister(handler) when
// hasTimer is false.
//
// Parameters:
//   - handler is the handler whose [HandlerContext.nextExpiration] changed.
//   - expiresAt is the nextExpiration value. Ignored when hasTimer is false.
//   - hasTimer is the hasTimer value of nextExpiration. false means no timer and no abortiveCloseDeadline
//     is set, so the handler's entry is removed from clientTimer.
//
// Notes:
//   - The clientTimer.Register error is passed to handler.OnError.
func (this *tcpReactor) updateClientTimer(handler ClientHandler, expiresAt time.Time, hasTimer bool) {
	if !hasTimer {

		this.clientTimer.Unregister(handler)
		return
	}

	if err := this.clientTimer.Register(expiresAt, handler); err != nil {
		handler.OnError(handler.context(), err)
		return
	}
}
