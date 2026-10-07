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

		default:
			this.handleError(ErrTCPReactorCommand)
		}
	}

	clear(commands)
	return isStop
}

// handleCommandRegisterHandler binds the handler and registers the read and hangup events.
// If the registration fails, it calls OnError and closes the connection.
//
//   - handler: Handler enqueued with the registration command.
func (this *TCPReactor) handleCommandRegisterHandler(handler ClientHandler) {
	fd := handler.context().fileDescriptor()
	if fd < 0 {
		handler.OnError(handler.context(), ErrTCPReactorInvalidFileDescriptor)
		return
	}

	if _, contains := this.handlers[fd]; contains || fd == this.commandFD {
		handler.OnError(handler.context(), ErrTCPReactorDuplicateFD)
		return
	}

	if err := handler.context().bind(handler, this.commands, this.handlerCounter); err != nil {
		this.rejectHandler(handler, err)
		return
	}

	if registerError := this.epoller.RegisterReadWithHangup(fd); registerError != nil {
		this.rejectHandler(handler, registerError)
		return
	}

	this.handlers[fd] = handler
	this.handlerCounter.Add(1)
	handler.OnConnect(handler.context())
}

// rejectHandler reports the registration failure through OnError and immediately closes the
// handed-over connection.
//
//   - handler: Handler whose registration failed.
//   - reason: reason for the registration failure.
func (this *TCPReactor) rejectHandler(handler ClientHandler, reason error) {
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
	nextExpiration, hasTimer := handler.context().setTimeout(timerKey, expiresAt)
	this.updateClientTimer(handler, nextExpiration, hasTimer)
}

// handleCommandUnsetTimeout unsets the per-key timer of the currently registered Handler and
// updates the next expiration time.
//
//   - handler: target of the timer unset, and the command is ignored if it differs from the
//     currently registered object.
//   - timerKey: key of the timer to unset.
func (this *TCPReactor) handleCommandUnsetTimeout(handler ClientHandler, timerKey uint64) {
	nextExpiration, hasTimer := handler.context().unsetTimeout(timerKey)
	this.updateClientTimer(handler, nextExpiration, hasTimer)
}

// handleCommandWrite registers the handler for write events.
// When a write event occurs, the epoller calls the handler's OnWrite.
//
//   - handler: target of the transmission, and the command is ignored if it differs from the
//     currently registered object.
func (this *TCPReactor) handleCommandWrite(handler ClientHandler) {
	if err := this.epoller.RegisterWrite(handler.context().fileDescriptor()); err != nil {
		handler.OnError(handler.context(), err)
	}
}

// handleCommandUser calls the handler's OnUserEvent with the data delivered through PostUserEvent.
// If the connection is closed after the event is enqueued but before it is processed,
// the event is discarded without returning an error.
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
