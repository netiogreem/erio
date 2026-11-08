package erio

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/netiogreem/erio/internal"
)

// commandType identifies the kind of [command] queued in a [commandMailbox].
type commandType uint8

const (
	commandStop              commandType = iota // Stops the event loop. Always drained last.
	commandSetTimeout                           // Sets the timer identified by timerKey to fire at expiresAt.
	commandUnsetTimeout                         // Cancels the timer identified by timerKey.
	commandWrite                                // Requests a write for the handler's pending data.
	commandRegisterHandler                      // Adds a handler to the event loop.
	commandUnregisterHandler                    // Removes a handler from the event loop with reason and soLinger.
	commandUser                                 // Delivers userEventData to the handler.
	commandWake                                 // Wakes the handler without data.
	commandClose                                // Closes the handler gracefully; falls back to an abortive close at expiresAt.
	commandAbortiveClose                        // Closes the handler immediately, discarding pending data.
)

// commandMailboxError is the error type returned by [commandMailbox] methods.
// It is a string so that each error can be declared as a comparable constant.
type commandMailboxError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this commandMailboxError) Error() string {
	return string(this)
}

const (
	ErrCommandMailboxUninitialized               commandMailboxError = "erio: command mailbox is uninitialized"             // Returned when the mailbox was not created with [newCommandMailbox].
	ErrCommandMailboxInvalidFileDescriptor       commandMailboxError = "erio: invalid command mailbox file descriptor"      // Returned by [newCommandMailbox] when commandFD is negative.
	ErrCommandMailboxClosed                      commandMailboxError = "erio: command mailbox is closed"                    // Returned when a command is enqueued before [commandMailbox.Open] or after [commandMailbox.Stop] or [commandMailbox.EnqueueStop].
	ErrCommandMailboxInvalidReserveSize          commandMailboxError = "erio: invalid command mailbox reserve size"         // Returned by [newCommandMailbox] when reserveSize is 0 or exceeds the maximum slice capacity.
	ErrCommandMailboxInvalidRegisterCommandQuota commandMailboxError = "erio: invalid register command quota"               // Returned by [newCommandMailbox] when registerCommandQuota is 0 or exceeds the maximum slice capacity.
	ErrCommandMailboxRegisterQuotaExceeded       commandMailboxError = "erio: register command quota exceeded"              // Returned by [commandMailbox.EnqueueRegisterHandler] when registerCommandQuota registrations are already queued since the last [commandMailbox.Drain].
	ErrCommandMailboxHandlerQuotaExceeded        commandMailboxError = "erio: handler command quota exceeded"               // Returned when a handler has already queued commandQuota commands of its [HandlerContext] since the last [commandMailbox.Drain].
	ErrCommandMailboxNilHandler                  commandMailboxError = "erio: handler is nil"                               // Returned when a command that requires a handler is enqueued with a nil handler.
	ErrCommandMailboxNotRegistered               commandMailboxError = "erio: handler is not registered in command mailbox" // Returned when a command is enqueued for a handler without a prior [commandMailbox.EnqueueRegisterHandler].
	ErrCommandMailboxInvalidCommand              commandMailboxError = "erio: invalid command"                              // Returned by enqueue when the commandType is unknown.
)

// command is a single request queued for the event loop.
// Only the fields relevant to commandType are set; the rest keep their zero value.
type command struct {
	commandType   commandType   // Kind of request.
	handler       ClientHandler // Target handler. nil only for commandStop.
	reason        error         // Reason passed to the handler on commandUnregisterHandler.
	soLinger      bool          // Close mode for commandUnregisterHandler: true aborts the connection, false closes it gracefully.
	timerKey      uint64        // Timer identifier for commandSetTimeout and commandUnsetTimeout.
	expiresAt     time.Time     // Timer expiration for commandSetTimeout, or the abortive close deadline for commandClose.
	userEventData any           // Payload for commandUser.
}

// handlerCommandCount tracks how many quota-limited commands a handler has
// queued in the current drain cycle.
type handlerCommandCount struct {
	generation uint64 // Drain cycle in which count was taken. A stale generation resets count to 0.
	count      uint32 // Quota-limited commands queued in that cycle.
}

// commandMailbox is a thread-safe queue that carries commands from any goroutine
// to the event loop and wakes the loop through an eventfd.
//
// Notes:
//   - Commands are collected in batches. The loop takes the whole batch with [commandMailbox.Drain].
//   - Per-handler and registration quotas apply within one batch and reset at each Drain.
//   - The eventfd is written only when the first command of a batch is queued, so one wakeup covers the batch.
type commandMailbox struct {
	commandFD             internal.FileDescriptor               // eventfd written to wake the event loop and read to clear the wakeup.
	commands              []command                             // Commands queued since the last Drain.
	commandsSpare         []command                             // Backing slice swapped in at Drain to avoid reallocating commands.
	stopCommand           *command                              // Pending stop request, appended after all other commands at Drain. nil when none.
	registerCommandQuota  uint32                                // Maximum register commands per batch.
	registerCommandCount  uint32                                // Register commands queued in the current batch.
	handlerCommandCounts  map[ClientHandler]handlerCommandCount // Registered handlers and their per-batch command counts.
	generation            uint64                                // Current batch number, incremented at each Drain.
	accessMutex           sync.Mutex                            // Guards every field below and above except commandFD.
	closed                bool                                  // true rejects new commands with [ErrCommandMailboxClosed].
	immediateWriteEnabled bool                                  // true routes write commands to immediateWrites instead of commands.
	immediateWrites       []ClientHandler                       // Handlers that requested a write while immediate writes were enabled, without duplicates.
	immediateWritesSpare  []ClientHandler                       // Backing slice swapped in at EndImmediateWrites to avoid reallocating immediateWrites.
}

// newCommandMailbox creates a closed [commandMailbox]; call [commandMailbox.Open] before enqueuing.
//
// Parameters:
//   - commandFD is the eventfd written to wake the event loop. Must be non-negative.
//   - reserveSize is the initial capacity of the command queue. Must be between 1 and the maximum slice capacity.
//   - registerCommandQuota is the maximum number of register commands per batch. Must be between 1 and the
//     maximum slice capacity.
//
// Returns:
//   - The mailbox with closed true, and nil, when every parameter is valid.
//   - [ErrCommandMailboxInvalidFileDescriptor] when commandFD is negative.
//   - [ErrCommandMailboxInvalidRegisterCommandQuota] wrapped with the value when registerCommandQuota is out of range.
//   - [ErrCommandMailboxInvalidReserveSize] wrapped with the value when reserveSize is out of range.
//
// Notes:
//   - The caller keeps ownership of commandFD.
func newCommandMailbox(commandFD internal.FileDescriptor, reserveSize uint32, registerCommandQuota uint32) (*commandMailbox, error) {
	if commandFD < 0 {
		return nil, ErrCommandMailboxInvalidFileDescriptor
	}

	if registerCommandQuota == 0 || uint64(registerCommandQuota) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: %d", ErrCommandMailboxInvalidRegisterCommandQuota, registerCommandQuota)
	}

	if reserveSize == 0 || uint64(reserveSize) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: %d", ErrCommandMailboxInvalidReserveSize, reserveSize)
	}

	return &commandMailbox{
		commandFD:            commandFD,
		commands:             make([]command, 0, int(reserveSize)),
		commandsSpare:        make([]command, 0, int(reserveSize)),
		stopCommand:          nil,
		registerCommandQuota: registerCommandQuota,
		registerCommandCount: 0,
		handlerCommandCounts: make(map[ClientHandler]handlerCommandCount),
		closed:               true,
		immediateWrites:      make([]ClientHandler, 0, 64),
		immediateWritesSpare: make([]ClientHandler, 0, 64),
	}, nil
}

// Open starts accepting commands.
//
// Parameters:
//   - clear is true to discard queued commands, handler registrations, and quota counts; false keeps them.
//
// Returns:
//   - nil when the mailbox is open.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//
// Notes:
//   - Immediate writes are disabled on Open regardless of clear.
//   - A pending stop request from [commandMailbox.EnqueueStop] is not cleared and is still delivered by the next [commandMailbox.Drain].
func (this *commandMailbox) Open(clear bool) error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()
	this.closed = false

	this.immediateWriteEnabled = false
	if clear == true {
		this.clear()
	}

	return nil
}

// Stop rejects further commands without queuing a stop command for the event loop.
//
// Parameters:
//   - clear is true to discard queued commands, handler registrations, and quota counts; false keeps them.
//
// Returns:
//   - nil when the mailbox was open and is now closed.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox was already closed.
//
// Notes:
//   - Use [commandMailbox.EnqueueStop] to also deliver a stop command to the event loop.
func (this *commandMailbox) Stop(clear bool) error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()
	if this.closed == true {
		return ErrCommandMailboxClosed
	}

	this.closed = true
	if clear == true {
		this.clear()
	}

	return nil
}

// Clear discards queued commands, handler registrations, and quota counts while keeping the open or closed state.
//
// Returns:
//   - nil when cleared.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//
// Notes:
//   - A pending stop request from [commandMailbox.EnqueueStop] is kept.
func (this *commandMailbox) Clear() error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()
	this.clear()
	return nil
}

// clear empties commands, sets registerCommandCount to 0, and empties handlerCommandCounts and immediateWrites.
//
// Notes:
//   - stopCommand is kept.
//
// Caution:
//   - The caller must hold accessMutex.
func (this *commandMailbox) clear() {
	clear(this.commands)
	this.commands = this.commands[:0]
	this.registerCommandCount = 0
	clear(this.handlerCommandCounts)
	clear(this.immediateWrites)
}

// EnqueueRegisterHandler queues a request to add handler to the event loop and
// starts accepting per-handler commands for it.
//
// Parameters:
//   - handler is the handler to register.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxRegisterQuotaExceeded] when the per-batch register quota is used up.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
//
// Notes:
//   - Per-handler commands such as [commandMailbox.EnqueueWrite] are accepted as soon as this returns nil, before the loop drains the registration.
func (this *commandMailbox) EnqueueRegisterHandler(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandRegisterHandler,
		handler:     handler,
		expiresAt:   time.Time{},
	})
}

// EnqueueUnregisterHandler queues a request to remove handler from the event loop and
// stops accepting per-handler commands for it.
//
// Parameters:
//   - handler is the handler to remove.
//   - reason is passed to the handler as the close reason.
//   - soLinger is true to abort the connection, discarding pending data, or false to close it gracefully.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
//
// Notes:
//   - Does not check whether handler is registered.
//   - Per-handler commands for handler fail with [ErrCommandMailboxNotRegistered] as soon as this returns.
func (this *commandMailbox) EnqueueUnregisterHandler(handler ClientHandler, reason error, soLinger bool) error {
	return this.enqueue(command{
		commandType: commandUnregisterHandler,
		handler:     handler,
		reason:      reason,
		soLinger:    soLinger,
		expiresAt:   time.Time{},
	})
}

// UnregisterHandler stops accepting per-handler commands for handler without queuing a command.
//
// Parameters:
//   - handler is the handler to forget.
//
// Returns:
//   - nil when done, including when handler was not registered.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//
// Notes:
//   - Commands already queued for handler stay in the queue and are still returned by [commandMailbox.Drain].
func (this *commandMailbox) UnregisterHandler(handler ClientHandler) error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()

	if this.closed == true {
		return ErrCommandMailboxClosed
	}

	delete(this.handlerCommandCounts, handler)
	return nil
}

// EnqueueSetTimeout queues a request to arm a timer for handler.
//
// Parameters:
//   - handler is the registered handler that owns the timer and must not be nil.
//   - timerKey identifies the timer and is passed back to [ClientHandler.OnTimeout].
//   - expiresAt is the time at which the timer fires.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handler was not registered.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when handler used up its per-batch quota.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
func (this *commandMailbox) EnqueueSetTimeout(handler ClientHandler, timerKey uint64, expiresAt time.Time) error {
	return this.enqueue(command{
		commandType: commandSetTimeout,
		handler:     handler,
		timerKey:    timerKey,
		expiresAt:   expiresAt,
	})
}

// EnqueueUnsetTimeout queues a request to cancel a timer for handler.
//
// Parameters:
//   - handler is the registered handler that owns the timer and must not be nil.
//   - timerKey identifies the timer to cancel.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handler was not registered.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when handler used up its per-batch quota.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
func (this *commandMailbox) EnqueueUnsetTimeout(handler ClientHandler, timerKey uint64) error {
	return this.enqueue(command{
		commandType: commandUnsetTimeout,
		handler:     handler,
		timerKey:    timerKey,
		expiresAt:   time.Time{},
	})
}

// EnqueueWrite requests that the event loop write handler's pending data.
//
// Parameters:
//   - handler is the registered handler with data to write and must not be nil.
//
// Returns:
//   - nil when queued or recorded as an immediate write.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handler was not registered and immediate writes are disabled.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when handler used up its per-batch quota and immediate writes are disabled.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
//
// Notes:
//   - Between [commandMailbox.BeginImmediateWrites] and [commandMailbox.EndImmediateWrites] the request is not queued.
//     Instead handler is recorded once, without a registration or quota check, and written by EndImmediateWrites.
func (this *commandMailbox) EnqueueWrite(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandWrite,
		handler:     handler,
		expiresAt:   time.Time{},
	})
}

// EnqueueUserEvent queues userEventData for delivery to handler through [ClientHandler.OnUserEvent].
//
// Parameters:
//   - handler is the registered handler that receives the event and must not be nil.
//   - userEventData is passed to the handler unchanged and may be nil.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handler was not registered.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when handler used up its per-batch quota.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
func (this *commandMailbox) EnqueueUserEvent(handler ClientHandler, userEventData any) error {
	return this.enqueue(command{
		commandType:   commandUser,
		handler:       handler,
		userEventData: userEventData,
	})
}

// EnqueueWake queues a request to call [ClientHandler.OnWake] on handler.
//
// Parameters:
//   - handler is the registered handler to wake and must not be nil.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handler was not registered.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
//
// Notes:
//   - Not counted against the handler's per-batch quota.
func (this *commandMailbox) EnqueueWake(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandWake,
		handler:     handler,
	})
}

// EnqueueClose queues a graceful close of handler.
//
// Parameters:
//   - handler is the registered handler to close and must not be nil.
//   - abortiveCloseDeadline is the time after which the loop gives up sending pending data and closes abortively.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handler was not registered.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
//
// Notes:
//   - Not counted against the handler's per-batch quota.
func (this *commandMailbox) EnqueueClose(handler ClientHandler, abortiveCloseDeadline time.Time) error {
	return this.enqueue(command{
		commandType: commandClose,
		handler:     handler,
		expiresAt:   abortiveCloseDeadline,
	})
}

// EnqueueAbortiveClose queues an immediate close of handler that discards pending data.
//
// Parameters:
//   - handler is the registered handler to close and must not be nil.
//
// Returns:
//   - nil when queued.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox is closed.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handler was not registered.
//   - A write error from the eventfd when waking the event loop fails; the command is not queued.
//
// Notes:
//   - Not counted against the handler's per-batch quota.
func (this *commandMailbox) EnqueueAbortiveClose(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandAbortiveClose,
		handler:     handler,
	})
}

// EnqueueStop closes the mailbox and queues a stop command that [commandMailbox.Drain] delivers last.
//
// Returns:
//   - nil when the stop command is pending, including when one was already pending.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when the mailbox was already closed.
//   - A write error from the eventfd when waking the event loop fails; the mailbox stays closed but no stop command is pending.
//
// Notes:
//   - Commands queued before this call are still delivered, ahead of the stop command.
func (this *commandMailbox) EnqueueStop() error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()

	if this.closed == true {
		return ErrCommandMailboxClosed
	}

	this.closed = true

	if this.stopCommand != nil {
		return nil
	}

	this.stopCommand = &command{
		commandType: commandStop,
		handler:     nil,
		expiresAt:   time.Time{},
	}

	if len(this.commands) != 0 {
		return nil
	}

	if notifyError := this.notifyFD(); notifyError != nil {
		this.stopCommand = nil
		return notifyError
	}

	return nil
}

// enqueue takes accessMutex and dispatches command by command.commandType to the matching enqueue* function.
//
// Parameters:
//   - command is the request to queue. command.commandType selects the validation and quota rules.
//
// Returns:
//   - nil when the command was appended to commands or recorded in immediateWrites.
//   - [ErrCommandMailboxUninitialized] when handlerCommandCounts is nil, that is, the mailbox was not
//     created with [newCommandMailbox].
//   - [ErrCommandMailboxClosed] when closed is true.
//   - [ErrCommandMailboxNilHandler] when command.handler is nil for a commandType other than
//     commandRegisterHandler and commandUnregisterHandler.
//   - [ErrCommandMailboxNotRegistered] when handlerCommandCounts has no entry for command.handler.
//   - [ErrCommandMailboxRegisterQuotaExceeded] from [commandMailbox.enqueueRegisterCommand], or
//     [ErrCommandMailboxHandlerQuotaExceeded] from [commandMailbox.enqueueHandlerCommand].
//   - [ErrCommandMailboxInvalidCommand] when commandType matches no case.
//   - The [commandMailbox.notifyFD] error from [commandMailbox.appendCommand].
//
// Notes:
//   - commandRegisterHandler goes to [commandMailbox.enqueueRegisterCommand], commandUnregisterHandler to
//     [commandMailbox.enqueueUnregisterCommand].
//   - commandWrite goes to [commandMailbox.enqueueImmediateWrite] while immediateWriteEnabled is true,
//     otherwise to [commandMailbox.enqueueHandlerCommand].
//   - commandSetTimeout, commandUnsetTimeout, and commandUser go to enqueueHandlerCommand, which counts them
//     against the handler's commandQuota.
//   - commandWake, commandClose, and commandAbortiveClose check command.handler != nil and the
//     handlerCommandCounts entry, then call [commandMailbox.appendCommand] without a quota check.
func (this *commandMailbox) enqueue(command command) error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()

	if this.closed == true {
		return ErrCommandMailboxClosed
	}

	switch command.commandType {
	case commandRegisterHandler:
		return this.enqueueRegisterCommand(command)
	case commandUnregisterHandler:
		return this.enqueueUnregisterCommand(command)
	case commandWrite:

		if this.immediateWriteEnabled == true {
			return this.enqueueImmediateWrite(command.handler)
		}
		return this.enqueueHandlerCommand(command)
	case commandSetTimeout, commandUnsetTimeout, commandUser:
		return this.enqueueHandlerCommand(command)
	case commandClose, commandAbortiveClose:

		if command.handler == nil {
			return ErrCommandMailboxNilHandler
		}
		if _, contains := this.handlerCommandCounts[command.handler]; !contains {
			return ErrCommandMailboxNotRegistered
		}
		return this.appendCommand(command)
	case commandWake:

		if command.handler == nil {
			return ErrCommandMailboxNilHandler
		}
		if _, contains := this.handlerCommandCounts[command.handler]; !contains {
			return ErrCommandMailboxNotRegistered
		}
		return this.appendCommand(command)
	default:
		return ErrCommandMailboxInvalidCommand
	}
}

// enqueueImmediateWrite appends handler to immediateWrites for [commandMailbox.EndImmediateWrites].
//
// Parameters:
//   - handler is command.handler of a commandWrite. Must not be nil.
//
// Returns:
//   - nil when appended, when handler.context().immediateWrite was already true, or when handler.context()
//     is nil.
//   - [ErrCommandMailboxNilHandler] when handler is nil.
//
// Notes:
//   - handler.context().immediateWrite is set to true on the first append and reset by EndImmediateWrites,
//     so each handler appears at most once per [commandMailbox.BeginImmediateWrites] window.
//
// Caution:
//   - The caller must hold accessMutex.
func (this *commandMailbox) enqueueImmediateWrite(handler ClientHandler) error {
	if handler == nil {
		return ErrCommandMailboxNilHandler
	}

	if handler.context() == nil {
		return nil
	}

	if handler.context().immediateWrite {
		return nil
	}

	handler.context().immediateWrite = true
	this.immediateWrites = append(this.immediateWrites, handler)
	return nil
}

// BeginImmediateWrites makes subsequent [commandMailbox.EnqueueWrite] calls record the handler
// for [commandMailbox.EndImmediateWrites] instead of queuing a command.
//
// Returns:
//   - nil when enabled.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//
// Notes:
//   - Intended for the event loop thread, so writes requested while it runs handler callbacks are performed in the same loop iteration without an eventfd wakeup.
func (this *commandMailbox) BeginImmediateWrites() error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()
	this.immediateWriteEnabled = true
	return nil
}

// EndImmediateWrites disables immediate writes and calls writer for every handler recorded since
// [commandMailbox.BeginImmediateWrites].
//
// Parameters:
//   - writer performs the write for one handler and must not be nil when any handler was recorded.
//
// Returns:
//   - nil when done.
//   - [ErrCommandMailboxUninitialized] when the mailbox was not created with [newCommandMailbox].
//
// Notes:
//   - writer is called after accessMutex is released, so it may enqueue commands.
//   - Each handler is passed to writer once, in the order it was first recorded.
func (this *commandMailbox) EndImmediateWrites(writer func(ClientHandler)) error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	this.immediateWriteEnabled = false

	handlers := this.immediateWrites
	for _, handler := range handlers {
		handler.context().immediateWrite = false
	}

	this.immediateWrites = this.immediateWritesSpare[:0]
	this.immediateWritesSpare = handlers[:0]
	this.accessMutex.Unlock()

	for _, handler := range handlers {
		writer(handler)
	}

	clear(handlers)
	return nil
}

// enqueueRegisterCommand appends a commandRegisterHandler and adds command.handler to handlerCommandCounts.
//
// Parameters:
//   - command is the commandRegisterHandler from [commandMailbox.EnqueueRegisterHandler].
//
// Returns:
//   - nil when appended.
//   - [ErrCommandMailboxRegisterQuotaExceeded] when registerCommandCount >= registerCommandQuota.
//   - The [commandMailbox.appendCommand] error.
//
// Notes:
//   - After a successful append, handlerCommandCounts[command.handler] is set to {generation, 0} and
//     registerCommandCount is incremented.
//
// Caution:
//   - The caller must hold accessMutex.
func (this *commandMailbox) enqueueRegisterCommand(command command) error {
	if this.registerCommandCount >= this.registerCommandQuota {
		return ErrCommandMailboxRegisterQuotaExceeded
	}

	if err := this.appendCommand(command); err != nil {
		return err
	}

	this.handlerCommandCounts[command.handler] = handlerCommandCount{generation: this.generation, count: 0}
	this.registerCommandCount++
	return nil
}

// enqueueUnregisterCommand deletes command.handler from handlerCommandCounts and appends the
// commandUnregisterHandler.
//
// Parameters:
//   - command is the commandUnregisterHandler from [commandMailbox.EnqueueUnregisterHandler].
//
// Returns:
//   - nil when appended.
//   - The [commandMailbox.appendCommand] error.
//
// Notes:
//   - The delete runs before appendCommand, so later commands for the handler get
//     [ErrCommandMailboxNotRegistered] even when the append fails.
//
// Caution:
//   - The caller must hold accessMutex.
func (this *commandMailbox) enqueueUnregisterCommand(command command) error {
	delete(this.handlerCommandCounts, command.handler)

	if err := this.appendCommand(command); err != nil {
		return err
	}

	return nil
}

// enqueueHandlerCommand appends a commandWrite, commandSetTimeout, commandUnsetTimeout, or commandUser after
// checking the handler's per-batch count in handlerCommandCounts.
//
// Parameters:
//   - command is the command to append. command.handler must not be nil.
//
// Returns:
//   - nil when appended.
//   - [ErrCommandMailboxNilHandler] when command.handler is nil.
//   - [ErrCommandMailboxNotRegistered] when handlerCommandCounts has no entry for command.handler.
//   - [ErrCommandMailboxHandlerQuotaExceeded] when entry.count >= command.handler.context().commandQuota.
//   - The [commandMailbox.appendCommand] error.
//
// Notes:
//   - When entry.generation != generation, entry is reset to {generation, 0} before the check.
//   - entry.count is incremented and stored only after a successful appendCommand.
//
// Caution:
//   - The caller must hold accessMutex.
func (this *commandMailbox) enqueueHandlerCommand(command command) error {
	if command.handler == nil {
		return ErrCommandMailboxNilHandler
	}

	handler := command.handler

	entry, contains := this.handlerCommandCounts[handler]

	if contains == false {
		return ErrCommandMailboxNotRegistered
	}

	if entry.generation != this.generation {
		entry = handlerCommandCount{generation: this.generation, count: 0}
	}

	// commandQuota is a value that does not change after creation.
	if entry.count >= handler.context().commandQuota {
		return ErrCommandMailboxHandlerQuotaExceeded
	}

	if err := this.appendCommand(command); err != nil {
		return err
	}

	entry.count++
	this.handlerCommandCounts[handler] = entry
	return nil
}

// appendCommand appends command to commands and calls [commandMailbox.notifyFD] for the first command of a batch.
//
// Parameters:
//   - command is the validated command to append.
//
// Returns:
//   - nil when appended.
//   - The notifyFD error. The appended command is removed from commands again in this case.
//
// Notes:
//   - notifyFD is called only when len(commands) became 1 and stopCommand is nil. Otherwise an earlier
//     append or [commandMailbox.EnqueueStop] has already written commandFD for this batch.
//
// Caution:
//   - The caller must hold accessMutex.
func (this *commandMailbox) appendCommand(command command) error {
	this.commands = append(this.commands, command)
	if len(this.commands) > 1 || this.stopCommand != nil {
		return nil
	}

	if notifyError := this.notifyFD(); notifyError != nil {
		lastIndex := len(this.commands) - 1
		clear(this.commands[lastIndex:])
		this.commands = this.commands[:lastIndex]
		return notifyError
	}

	return nil
}

// Drain clears the eventfd wakeup and takes every queued command, starting a new batch.
//
// Parameters:
//   - errorCallback receives the error when reading the eventfd fails and may be nil to ignore it.
//
// Returns:
//   - The queued commands in enqueue order, with a pending stop command appended last.
//   - nil when nothing is queued or the mailbox was not created with [newCommandMailbox].
//
// Notes:
//   - Resets the register quota and starts a new generation, which resets every handler's per-batch quota.
//   - Handler registrations are kept across Drain.
//
// Caution:
//   - The returned slice is reused as the queue on the next Drain. Finish processing it before calling Drain again.
func (this *commandMailbox) Drain(errorCallback func(error)) []command {
	if this.handlerCommandCounts == nil {
		return nil
	}

	if err := this.consumeFD(); err != nil {
		if errorCallback != nil {
			errorCallback(err)
		}
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()

	if len(this.commands) == 0 && this.stopCommand == nil {
		return nil
	}

	if this.stopCommand != nil {
		this.commands = append(this.commands, *this.stopCommand)
		this.stopCommand = nil
	}

	commands := this.commands
	this.commands = this.commandsSpare[:0]
	this.commandsSpare = commands[:0]
	this.registerCommandCount = 0
	this.generation++

	return commands
}

// notifyFD calls syscall.Write(commandFD, 8 bytes holding 1) to add 1 to the eventfd counter and wake epoller.Wait.
//
// Returns:
//   - nil when Write succeeded or returned EAGAIN.
//   - os.NewSyscallError("write", err) otherwise.
//
// Notes:
//   - EINTR is retried.
//   - EAGAIN means the eventfd counter is already at its maximum, so epoller.Wait will wake anyway.
func (this *commandMailbox) notifyFD() error {
	notification := [8]byte{}
	binary.NativeEndian.PutUint64(notification[:], 1)

	for {
		_, writeError := syscall.Write(int(this.commandFD), notification[:])
		if writeError == syscall.EINTR {
			continue
		}

		if writeError == nil || writeError == syscall.EAGAIN {
			return nil
		}

		return os.NewSyscallError("write", writeError)
	}
}

// consumeFD calls syscall.Read(commandFD, 8 bytes) to reset the eventfd counter to 0.
//
// Returns:
//   - nil when Read succeeded or returned EAGAIN.
//   - os.NewSyscallError("read", err) otherwise.
//
// Notes:
//   - EINTR is retried.
//   - EAGAIN means the counter was already 0, so there was no pending wakeup.
func (this *commandMailbox) consumeFD() error {
	notification := [8]byte{}

	for {
		_, readError := syscall.Read(int(this.commandFD), notification[:])
		if readError == syscall.EINTR {
			continue
		}

		if readError == nil || readError == syscall.EAGAIN {
			return nil
		}

		return os.NewSyscallError("read", readError)
	}
}
