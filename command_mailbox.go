package erio

import (
	"encoding/binary"
	"fmt"
	"github.com/netiogreem/erio/internal"
	"os"
	"sync"
	"syscall"
	"time"
)

// commandType represents the type of a command delivered by commandMailbox.
type commandType uint8

const (
	commandStop commandType = iota
	commandSetTimeout
	commandUnsetTimeout
	commandWrite
	commandRegisterHandler
	commandUser
	commandClose         // Normal connection close command
	commandAbortiveClose // Forced (RST) connection close command
)

// commandMailboxError represents an error returned by commandMailbox.
type commandMailboxError string

// Error returns the error string.
//
// It is the commandMailbox error message.
func (this commandMailboxError) Error() string {
	return string(this)
}

// Errors returned by commandMailbox.
const (
	ErrCommandMailboxUninitialized               commandMailboxError = "erio: command mailbox is uninitialized"
	ErrCommandMailboxInvalidFileDescriptor       commandMailboxError = "erio: invalid command mailbox file descriptor"
	ErrCommandMailboxClosed                      commandMailboxError = "erio: command mailbox is closed"
	ErrCommandMailboxInvalidReserveSize          commandMailboxError = "erio: invalid command mailbox reserve size"
	ErrCommandMailboxInvalidRegisterCommandQuota commandMailboxError = "erio: invalid register command quota"
	ErrCommandMailboxRegisterQuotaExceeded       commandMailboxError = "erio: register command quota exceeded"
	ErrCommandMailboxHandlerQuotaExceeded        commandMailboxError = "erio: handler command quota exceeded"
	ErrCommandMailboxNilHandler                  commandMailboxError = "erio: handler is nil"
	ErrCommandMailboxInvalidCommand              commandMailboxError = "erio: invalid command"
)

// command holds a command delivered through commandMailbox and its arguments.
type command struct {
	commandType   commandType   // Command type.
	handler       ClientHandler // ClientHandler that processes the command.
	timerKey      uint64        // Key that identifies the timer.
	expiresAt     time.Time     // Timer expiration time.
	userEventData any           // User event data.
}

// commandMailbox stores commands and notifies command arrival through a file descriptor.
type commandMailbox struct {
	commandFD             internal.FileDescriptor  // File descriptor that notifies command arrival.
	commands              []command                // Holds the commands currently being enqueued in input order.
	commandsSpare         []command                // Array to swap in for enqueuing at the next Drain.
	stopCommand           *command                 // Stop command to deliver after the regular commands.
	registerCommandQuota  uint32                   // Maximum number of pending registration commands.
	registerCommandCount  uint32                   // Current number of pending registration commands.
	handlerCommandCounts  map[ClientHandler]uint32 // Number of pending commands per Handler.
	accessMutex           sync.Mutex               // Protects concurrent access to the command storage, the closed state, and the immediate write state.
	closed                bool                     // Indicates whether commandMailbox is closed.
	immediateWriteEnabled bool                     // Indicates whether immediate writes are enqueued because Register is processing events.
	immediateWrites       []ClientHandler          // Holds the transmission targets enqueued as immediate writes.
	immediateWritesSpare  []ClientHandler          // map to swap in for enqueuing at the next EndImmediateWrites.
}

// newCommandMailbox manages commands using a notification FD, two command arrays, and a quota count.
// It does not manage the lifetime of commandFD, so it does not close that FD.
//
//   - commandFD: eventfd for command arrival notifications.
//   - reserveSize: initial capacity of one command array, and it is not an enqueue limit.
//   - registerCommandQuota: number of registration commands that can be enqueued between Drains.
//
// It returns the created commandMailbox and the creation error.
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
		handlerCommandCounts: make(map[ClientHandler]uint32),
		closed:               false,
		immediateWrites:      make([]ClientHandler, 0, 64),
		immediateWritesSpare: make([]ClientHandler, 0, 64),
	}, nil
}

// Open allows calls to the Enqueue* functions.
// If clear is true, it empties the pending commands and immediate write targets.
//
//   - clear: if true, it empties the pending commands and immediate write targets.
//
// It returns an error if not initialized.
// It returns ErrCommandMailboxUninitialized if it was not created with newCommandMailbox.
func (this *commandMailbox) Open(clear bool) error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()
	this.closed = false
	// If it stopped during event processing, it clears the remaining processing state.
	this.immediateWriteEnabled = false
	if clear == true {
		this.clear()
	}

	return nil
}

// Stop closes enqueuing of new commands.
// It does not add a stop command and does not close the FD.
// If clear is false, pending commands can be received at the next Drain.
//
//   - clear: if true, it empties the pending commands and immediate write targets.
//
// It returns an error if not initialized or already closed.
// It returns ErrCommandMailboxUninitialized if it was not created with newCommandMailbox, and
// ErrCommandMailboxClosed if it is already closed.
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

// Clear empties the pending regular commands, the quota counts, and the immediate write targets.
// It keeps the open/closed state, the pending stop command, and the FD notification.
//
// It returns an error if not initialized.
// It returns ErrCommandMailboxUninitialized if it was not created with newCommandMailbox.
func (this *commandMailbox) Clear() error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()
	this.clear()
	return nil
}

// clear erases the references in the enqueue array and the counts,
// and does not touch the array that has been returned and is being processed.
// It must be called within accessMutex.
func (this *commandMailbox) clear() {
	clear(this.commands)
	this.commands = this.commands[:0]
	this.registerCommandCount = 0
	clear(this.handlerCommandCounts)
	clear(this.immediateWrites)
}

// EnqueueRegisterHandler enqueues a Handler registration command.
// It uses the Register registration quota and does not use the per-connection quota.
// It does not check the validity of the Handler.
//
//   - handler: Handler to register.
//
// It returns an error if closed, if the quota is exceeded, or if the notification fails.
func (this *commandMailbox) EnqueueRegisterHandler(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandRegisterHandler,
		handler:     handler,
		expiresAt:   time.Time{},
	})
}

// EnqueueSetTimeout enqueues a timer set command.
// It uses the per-connection quota of the Handler.
//
//   - handler: target Handler.
//   - timerKey: timer key.
//   - expiresAt: absolute expiration time.
//
// It returns an error if closed, if the Handler is nil, if the quota is exceeded, or if the
// notification fails.
func (this *commandMailbox) EnqueueSetTimeout(handler ClientHandler, timerKey uint64, expiresAt time.Time) error {
	return this.enqueue(command{
		commandType: commandSetTimeout,
		handler:     handler,
		timerKey:    timerKey,
		expiresAt:   expiresAt,
	})
}

// EnqueueUnsetTimeout enqueues a timer unset command.
// It uses the per-connection quota of the Handler.
//
//   - handler: target Handler.
//   - timerKey: timer key.
//
// It returns an error if closed, if the Handler is nil, if the quota is exceeded, or if the
// notification fails.
func (this *commandMailbox) EnqueueUnsetTimeout(handler ClientHandler, timerKey uint64) error {
	return this.enqueue(command{
		commandType: commandUnsetTimeout,
		handler:     handler,
		timerKey:    timerKey,
		expiresAt:   time.Time{},
	})
}

// EnqueueWrite enqueues a send command.
// It uses the per-connection quota of the Handler.
// When immediate write (BeginImmediateWrites~EndImmediateWrites) is set,
// it is enqueued as an immediate write target without the command queue and FD notification,
// and in that case, the per-connection quota is not applied and it is held only once per Handler.
//
//   - handler: Handler whose data is to be sent.
//
// It returns an error if closed, if the Handler is nil, if the quota is exceeded, or if the
// notification fails.
func (this *commandMailbox) EnqueueWrite(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandWrite,
		handler:     handler,
		expiresAt:   time.Time{},
	})
}

// EnqueueUserEvent enqueues a user event command.
// It uses the per-connection quota of the Handler and does not deep-copy the data.
//
//   - handler: target Handler.
//   - userEventData: user event data.
//
// It returns an error if closed, if the Handler is nil, if the quota is exceeded, or if the
// notification fails.
func (this *commandMailbox) EnqueueUserEvent(handler ClientHandler, userEventData any) error {
	return this.enqueue(command{
		commandType:   commandUser,
		handler:       handler,
		userEventData: userEventData,
	})
}

// EnqueueClose enqueues a normal close command for the Handler.
// It is not subject to the per-connection quota and keeps the enqueue order with other commands.
//
//   - handler: Handler to close.
//
// It returns an error if closed, if the Handler is nil, or if the notification fails.
func (this *commandMailbox) EnqueueClose(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandClose,
		handler:     handler,
	})
}

// EnqueueAbortiveClose enqueues a forced (RST) close command for the Handler.
// It is not subject to the per-connection quota and keeps the enqueue order with other commands.
//
//   - handler: Handler to close.
//
// It returns an error if closed, if the Handler is nil, or if the notification fails.
func (this *commandMailbox) EnqueueAbortiveClose(handler ClientHandler) error {
	return this.enqueue(command{
		commandType: commandAbortiveClose,
		handler:     handler,
	})
}

// EnqueueStop closes enqueuing of new commands and reserves a stop command.
// The stop command is separate from the quotas and is delivered last, after the pending regular
// commands, at the next Drain.
// It notifies the FD only when there are no pending regular commands.
// If the notification fails, it reverts only the stop command and keeps the closed state.
//
// It returns an error if not initialized or if the notification fails.
// It returns ErrCommandMailboxUninitialized if it was not created with newCommandMailbox,
// and ErrCommandMailboxClosed if it is already closed.
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

// enqueue checks the closed state within accessMutex,
// and divides enqueuing into registration, immediate write, and per-connection commands according
// to the command type.
//
//   - command: command to enqueue.
//
// It returns the closed error, the unknown command error, or the error of each enqueue step.
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
	case commandWrite:
		// If Register is processing events, it is enqueued as an immediate write without a command
		// and FD notification.
		if this.immediateWriteEnabled == true {
			return this.enqueueImmediateWrite(command.handler)
		}
		return this.enqueueHandlerCommand(command)
	case commandSetTimeout, commandUnsetTimeout, commandUser:
		return this.enqueueHandlerCommand(command)
	case commandClose, commandAbortiveClose:
		// Close commands are not counted in the per-connection quota. HandlerContext enqueues at
		// most one per connection.
		if command.handler == nil {
			return ErrCommandMailboxNilHandler
		}
		return this.appendCommand(command)
	default:
		return ErrCommandMailboxInvalidCommand
	}
}

// enqueueImmediateWrite holds each immediate write target only once per Handler.
// It does not go through the command queue and FD notification. It does not apply the
// per-connection quota.
// It is called within accessMutex.
//
//   - handler: Handler whose data is to be sent.
//
// It returns an error if the Handler is nil.
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

// BeginImmediateWrites starts an immediate write section.
// Transmissions enqueued until EndImmediateWrites do not go through the command queue and eventfd
// and are collected as immediate write targets.
//
// It returns an error if not initialized.
// It returns ErrCommandMailboxUninitialized if it was not created with newCommandMailbox.
func (this *commandMailbox) BeginImmediateWrites() error {
	if this.handlerCommandCounts == nil {
		return ErrCommandMailboxUninitialized
	}

	this.accessMutex.Lock()
	defer this.accessMutex.Unlock()
	this.immediateWriteEnabled = true
	return nil
}

// EndImmediateWrites ends the immediate write section and passes the collected transmission targets
// to writer.
// writer is called once per Handler outside accessMutex, and the call order is not guaranteed.
// Transmissions requested inside writer are enqueued as commands, not as immediate writes.
// It is called by the same single consumer as Drain.
//
//   - writer: function that processes the transmission target Handler.
//
// It returns an error if not initialized.
// It returns ErrCommandMailboxUninitialized if it was not created with newCommandMailbox.
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

	this.immediateWrites = this.immediateWritesSpare[:0] // Array already emptied after its previous use
	this.immediateWritesSpare = handlers[:0]
	this.accessMutex.Unlock()

	// Callbacks inside writer may acquire accessMutex, so it is called outside the lock.
	for _, handler := range handlers {
		writer(handler)
	}

	clear(handlers)
	return nil
}

// enqueueRegisterCommand checks the quota of registration commands and increments the count if
// enqueuing succeeds.
// The count is reset in Drain and clear.
// It is called within accessMutex.
//
//   - command: registration command to enqueue.
//
// It returns an error if the quota is exceeded or if the notification fails.
func (this *commandMailbox) enqueueRegisterCommand(command command) error {
	if this.registerCommandCount >= this.registerCommandQuota {
		return ErrCommandMailboxRegisterQuotaExceeded
	}

	if err := this.appendCommand(command); err != nil {
		return err
	}

	this.registerCommandCount++
	return nil
}

// enqueueHandlerCommand checks the input of the Handler command and the per-connection quota,
// and increments the count of the Handler if enqueuing succeeds. The quota is read with
// GetCommandQuota of the Handler.
// It is called within accessMutex.
//
//   - command: Handler command to enqueue.
//
// It returns an error if the Handler is nil, if the quota is exceeded, or if the notification
// fails.
func (this *commandMailbox) enqueueHandlerCommand(command command) error {
	if command.handler == nil {
		return ErrCommandMailboxNilHandler
	}

	handler := command.handler

	if this.handlerCommandCounts[handler] >= handler.GetCommandQuota() {
		return ErrCommandMailboxHandlerQuotaExceeded
	}

	if err := this.appendCommand(command); err != nil {
		return err
	}

	this.handlerCommandCounts[handler]++
	return nil
}

// appendCommand stores commands in input order.
// It notifies the FD only when there are neither pending commands nor a stop command. If the
// notification fails, it reverts the added command.
// It is called within accessMutex.
//
//   - command: command to store.
//
// It returns an error if the notification fails.
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

// Drain consumes the FD notification and returns the pending regular commands in enqueue order,
// appending the stop command at the end if there is one.
// It swaps the enqueue array and resets the quota counts.
// It is called by a single consumer.
// The returned array is reused for enqueuing at the next Drain, so it must be processed and cleared
// before the next Drain, and it is not kept or extended.
//
//   - errorCallback: function that receives notification consumption errors, and it is skipped if
//     nil.
//
// It returns nil if there are no pending commands.
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
	clear(this.handlerCommandCounts)

	return commands
}

// notifyFD notifies the eventfd of command arrival.
// It retries on EINTR, and treats EAGAIN from a full counter as success because a notification
// already exists.
//
// It returns the system call error if the notification fails.
func (this *commandMailbox) notifyFD() error {
	notification := [8]byte{}
	binary.NativeEndian.PutUint64(notification[:], 1)

	for {
		_, writeError := syscall.Write(int(this.commandFD), notification[:])
		if writeError == syscall.EINTR {
			continue
		}

		// If the counter is full, a read notification already exists.
		if writeError == nil || writeError == syscall.EAGAIN {
			return nil
		}

		return os.NewSyscallError("write", writeError)
	}
}

// consumeFD consumes the eventfd notification.
// It retries on EINTR, and treats EAGAIN with no notification as success.
//
// It returns the system call error if consumption fails.
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
