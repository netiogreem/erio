package erio

import (
	"errors"
	"time"
)

// handleCommands processes the commands drained from the Mailbox in order.
//
// isStop is true if a stop command was received, and the commands drained together with it are
// still processed.
func (this *TCPReactor) handleCommands() (isStop bool) {
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

// handleCommandRegisterHandler binds the handler and registers the read and hangup events.
// If the registration fails, it calls OnError and removes the Handler from the mailbox registry.
// If the FD is negative, already used by a registered Handler, or equal to the command FD, it does
// not close the connection; if binding or registering the epoll events fails, it closes the
// connection with RST.
//
//   - handler: Handler enqueued with the registration command.
//
// It returns the registration failure, or nil on success.
func (this *TCPReactor) handleCommandRegisterHandler(handler ClientHandler) (err error) {
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

// handleCommandUnregisterHandler processes the close event and removes the Handler.
// If AbortiveClose was requested by the time this command is processed, it closes the connection
// with RST and ErrTCPReactorAbortiveClose regardless of the reason and soLinger in the command.
//
//   - handler: Handler to remove, and the command is ignored if it differs from the currently
//     registered object.
//   - reason: close reason to pass to OnClose.
//   - soLinger: if true, it closes the connection with RST.
func (this *TCPReactor) handleCommandUnregisterHandler(handler ClientHandler, reason error, soLinger bool) {
	if handler.context().IsAbortiveClosed() {
		this.removeHandler(handler, ErrTCPReactorAbortiveClose, handler.context().IsAbortiveClosed())
		return
	}
	this.removeHandler(handler, reason, soLinger)
}

// rejectHandler reports the registration failure through OnError and immediately closes the
// handed-over connection.
//
//   - handler: Handler whose registration failed.
//   - reason: reason for the registration failure.
func (this *TCPReactor) rejectHandler(handler ClientHandler, reason error) {
	handler.context().setClosed()
	handler.OnError(handler.context(), errors.Join(handler.context().abortConnection(), reason))
}

// handleCommandSetTimeout sets the per-key timer of the currently registered Handler and updates
// the next expiration time.
//
//   - handler: target of the timer setting, and the command is ignored if it differs from the
//     currently registered object.
//   - timerKey: key of the timer to set or replace.
//   - expiresAt: timer expiration time.
func (this *TCPReactor) handleCommandSetTimeout(handler ClientHandler, timerKey uint64, expiresAt time.Time) {
	nextExpiration, hasTimer := handler.context().onSetTimeout(timerKey, expiresAt)
	this.updateClientTimer(handler, nextExpiration, hasTimer)
}

// handleCommandUnsetTimeout unsets the per-key timer of the currently registered Handler and
// updates the next expiration time.
//
//   - handler: target of the timer unset, and the command is ignored if it differs from the
//     currently registered object.
//   - timerKey: key of the timer to unset.
func (this *TCPReactor) handleCommandUnsetTimeout(handler ClientHandler, timerKey uint64) {
	nextExpiration, hasTimer := handler.context().onUnsetTimeout(timerKey)
	this.updateClientTimer(handler, nextExpiration, hasTimer)
}

// handleCommandWrite registers write monitoring (EPOLLOUT) for the Handler.
// When a write event occurs, handleEventWritable sends the data and OnWritten is called.
// If the registration fails, it calls OnError.
//
//   - handler: target of the transmission, and the command is ignored if it differs from the
//     currently registered object.
func (this *TCPReactor) handleCommandWrite(handler ClientHandler) {
	if err := this.epoller.RegisterWrite(handler.context().fileDescriptor()); err != nil {
		handler.OnError(handler.context(), err)
	}
}

// handleCommandClose processes the Close request.
// If no write data remains, it enqueues the unregister command as the close event.
// If data remains, it does not send the data itself; it sets the abortive close deadline when
// abortiveCloseTimeout is greater than 0, registers the next expiration in the client timer, and
// handleEventWritable or writeImmediately sends the remaining data and enqueues the close event.
// If AbortiveClose was requested after Close, it does nothing and the following abortive close
// command closes it.
// If enqueuing the close event fails, it calls OnError and removes the Handler immediately.
//
//   - handler: target of the close, and the command is ignored if it differs from the currently
//     registered object.
//   - abortiveCloseDeadline: abortive close deadline calculated by Close. Zero if
//     abortiveCloseTimeout is 0.
func (this *TCPReactor) handleCommandClose(handler ClientHandler, abortiveCloseDeadline time.Time) {
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

// handleCommandAbortiveClose processes the AbortiveClose request and enqueues the unregister
// command to close the connection with RST.
// If enqueuing the close event fails, it calls OnError and removes the Handler immediately.
//
//   - handler: target of the close, and the command is ignored if it differs from the currently
//     registered object.
func (this *TCPReactor) handleCommandAbortiveClose(handler ClientHandler) {
	if handler.context().close.Load() != closeStateAbort {
		return
	}

	if err := this.commands.EnqueueUnregisterHandler(handler, ErrTCPReactorAbortiveClose, true); err != nil {
		handler.OnError(handler.context(), err)
		this.removeHandler(handler, ErrTCPReactorAbortiveClose, true)
	}
}

// handleCommandUser calls the handler's OnUserEvent with the data delivered through PostUserEvent.
// The event is delivered even if Close was requested after it was enqueued.
// If the Handler is removed before it is processed, handleCommands discards the event and
// OnUserEvent is not called.
//
//   - handler: ClientHandler currently registered in the Reactor.
//   - userEventData: user event data enqueued through PostUserEvent.
func (this *TCPReactor) handleCommandUser(handler ClientHandler, userEventData any) {
	handler.context().onUserEvent(userEventData)
}

// updateClientTimer updates the timer expiration information of the ClientHandler.
// If hasTimer is false, it removes the Handler from ClientTimer.
//
//   - handler: Handler whose expiration information is updated.
//   - expiresAt: nearest expiration time among the timers remaining for the Handler.
//   - hasTimer: true if a timer remains, and if false, the Handler is removed from ClientTimer.
func (this *TCPReactor) updateClientTimer(handler ClientHandler, expiresAt time.Time, hasTimer bool) {
	if !hasTimer {
		// A handler that has already been drained or unregistered has no further registration to
		// unregister.
		this.clientTimer.Unregister(handler)
		return
	}

	if err := this.clientTimer.Register(expiresAt, handler); err != nil {
		handler.OnError(handler.context(), err)
		return
	}
}
