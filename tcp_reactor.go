package erio

import (
	"errors"
	"github.com/netiogreem/erio/internal"
	"math"
	"os"
	"sync/atomic"
	"syscall"
	"time"
)

// TCPReactorError represents state, command, and connection handling errors of TCPReactor.
type TCPReactorError string

// Error returns the error string.
//
// It is the TCPReactor error message.
func (this TCPReactorError) Error() string {
	return string(this)
}

// Lifecycle states of TCPReactor.
const (
	TCPReactorStateInit    internal.LifecycleStateType = iota // Ready-to-run state right after resource creation or after Init recreates the resources.
	TCPReactorStateStart                                      // State in which StartWithCounter starts the event loop and waits for preRun.
	TCPReactorStateRun                                        // State in which the event loop is running.
	TCPReactorStateStop                                       // State set after WaitStop finishes the shutdown cleanup, or when preRun fails.
	TCPReactorStateCleanup                                    // State set by Cleanup when releasing resources.
)

// Errors returned by TCPReactor.
const (
	ErrTCPReactorInvalidFileDescriptor       TCPReactorError = "erio: invalid tcp reactor handler file descriptor"
	ErrTCPReactorCommand                     TCPReactorError = "erio: invalid tcp reactor command"
	ErrTCPReactorNilHandler                  TCPReactorError = "erio: tcp reactor handler is nil"
	ErrTCPReactorNilHandlerContext           TCPReactorError = "erio: tcp reactor handler context is nil"
	ErrTCPReactorNilHandlerCounter           TCPReactorError = "erio: tcp reactor handler counter is nil"
	ErrTCPReactorDuplicateFD                 TCPReactorError = "erio: tcp reactor file descriptor is already registered"
	ErrTCPReactorHangup                      TCPReactorError = "erio: client connection is closed"
	ErrTCPReactorReadHangup                  TCPReactorError = "erio: client read side is closed"
	ErrTCPReactorStopped                     TCPReactorError = "erio: tcp reactor is stopped"
	ErrTCPReactorStateNotInit                TCPReactorError = "erio: tcp reactor is not in the Init state"
	ErrTCPReactorStateNotStart               TCPReactorError = "erio: tcp reactor is not in the Start state"
	ErrTCPReactorStateNotInitOrStopOrCleanup TCPReactorError = "erio: tcp reactor is not in the Init or Stop or Cleanup state"
	ErrTCPReactorStateNotRun                 TCPReactorError = "erio: tcp reactor is not in the Run state"
	ErrTCPReactorStateNotStop                TCPReactorError = "erio: tcp reactor is not in the Stop state"
	ErrTCPReactorStateNotCleanup             TCPReactorError = "erio: tcp reactor is not in the Cleanup state"
	ErrTCPReactorUninitialized               TCPReactorError = "erio: tcp reactor is uninitialized"
	ErrTCPReactorInvalidReadBufferSize       TCPReactorError = "erio: invalid tcp reactor read buffer size"
)

// TCPReactor sequentially processes commands, send/receive, and timer callbacks of TCP connections
// in a single event loop.
// It must be created with NewTCPReactor, and on a zero value, methods other than HandlerCount
// return ErrTCPReactorUninitialized.
// RegisterHandler and RequestStop can be called from other goroutines.
type TCPReactor struct {
	// epoll
	epoller              *internal.Epoller                         // Monitors events of the command FD and client sockets.
	eventBatchSize       uint32                                    // Maximum number of events to receive in one epoll wait.
	commandFD            internal.FileDescriptor                   // eventfd that wakes the epoll wait when a command is enqueued.
	commands             *commandMailbox                           // Command queue that passes external requests to the event loop.
	commandsReserveSize  uint32                                    // Initial reserved count of the command arrays.
	registerCommandQuota uint32                                    // Maximum number of pending registration commands of the Reactor.
	readBuffer           []byte                                    // Receive buffer reused for every read.
	handlers             map[internal.FileDescriptor]ClientHandler // Per-FD Handlers registered with this Reactor.
	handlerCounter       *atomic.Uint32                            // Handler counter that increases and decreases on client registration and removal and can be shared with other Reactors.
	clientTimer          *clientTimer                              // Manages the nearest timer expiration time for each Handler.
	lifecycleState       *internal.LifecycleState                  // Protects the state transitions of the run and resource management APIs.
	started              internal.SignalVal[error]                 // Delivers the start preparation result of the event loop.
	stopped              internal.SignalVal[error]                 // Delivers the result of unregistering command FD monitoring and updating the stop state.
	errorCallback        func(error)                               // Delivers Reactor errors such as event wait and resource cleanup errors.
}

// NewTCPReactor creates epoll, the command eventfd, and the Mailbox, and returns a Reactor in the
// ready-to-run state.
//
//   - eventBatchSize: maximum number of events to receive in one epoll wait.
//   - commandsReserveSize: initial reserved count for each of the two command arrays, and it must
//     be 1 or more.
//   - registerCommandQuota: maximum number of RegisterHandler requests.
//   - readBufferSize: size in bytes of the receive buffer of the Reactor, and it must be 1 or more.
//   - ErrorCallback: callback that delivers Reactor errors, and notification is skipped if it is
//     nil.
//
// It returns the created Reactor and the creation error.
// It returns ErrTCPReactorInvalidReadBufferSize if readBufferSize is 0 or too large.
func NewTCPReactor(
	eventBatchSize uint32,
	commandsReserveSize uint32,
	registerCommandQuota uint32,
	readBufferSize uint32,
	ErrorCallback func(error)) (*TCPReactor, error) {
	if readBufferSize == 0 || uint64(readBufferSize) > uint64(^uint(0)>>1) {
		return nil, ErrTCPReactorInvalidReadBufferSize
	}

	epoller, err := internal.NewEpoller(eventBatchSize)
	if err != nil {
		return nil, err
	}

	commandFD, _, errNo := syscall.Syscall(syscall.SYS_EVENTFD2, 0, syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if errNo != 0 {
		return nil, errors.Join(os.NewSyscallError("eventfd2", errNo), epoller.Close())
	}

	reactor := &TCPReactor{
		epoller:              epoller,
		eventBatchSize:       eventBatchSize,
		commandsReserveSize:  commandsReserveSize,
		registerCommandQuota: registerCommandQuota,
		commandFD:            internal.FileDescriptor(commandFD),
		commands:             nil,
		readBuffer:           make([]byte, int(readBufferSize)),
		handlers:             make(map[internal.FileDescriptor]ClientHandler),
		handlerCounter:       &atomic.Uint32{},
		clientTimer:          newClientTimer(),
		lifecycleState:       internal.NewLifecycleState(TCPReactorStateInit),
		errorCallback:        ErrorCallback,
	}

	commands, err := newCommandMailbox(reactor.commandFD, commandsReserveSize, registerCommandQuota)
	if err != nil {
		return nil, errors.Join(err, os.NewSyscallError("close", syscall.Close(int(reactor.commandFD))), epoller.Close())
	}

	reactor.commands = commands

	return reactor, nil
}

// Init releases the existing resources in the Init, Stop, or Cleanup state, recreates them with the
// same settings, and transitions to Init.
// If recreation fails, it remains in the Cleanup state and Init can be called again.
//
// It returns an error if resource cleanup or recreation fails.
// It returns ErrTCPReactorStateNotCleanup if the state right after Cleanup is not Cleanup.
func (this *TCPReactor) Init() error {
	if err := this.Cleanup(); err != nil {
		return err
	}

	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPReactorStateCleanup {
			return ErrTCPReactorStateNotCleanup
		}

		reactor, err := NewTCPReactor(this.eventBatchSize, this.commandsReserveSize, this.registerCommandQuota, uint32(len(this.readBuffer)), this.errorCallback)
		if err != nil {
			return err
		}

		// The state lock, run signals, shared counter, and receive buffer are kept, and the newly
		// created resources are taken over.
		this.epoller = reactor.epoller
		this.commands = reactor.commands
		this.commandFD = reactor.commandFD
		this.handlers = reactor.handlers
		this.clientTimer = reactor.clientTimer
		*state = TCPReactorStateInit
		return nil
	})
}

// Cleanup releases the command FD and epoll resources in the Init, Stop, or Cleanup state and
// transitions to the Cleanup state.
// It does not close an already released command FD and epoll again.
//
// It returns an error if the state check or resource release fails.
// It returns ErrTCPReactorUninitialized if it was not created with NewTCPReactor,
// and ErrTCPReactorStateNotInitOrStopOrCleanup if it is not in the Init, Stop, or Cleanup state.
func (this *TCPReactor) Cleanup() error {
	if this.lifecycleState == nil {
		return ErrTCPReactorUninitialized
	}

	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPReactorStateInit && *state != TCPReactorStateStop && *state != TCPReactorStateCleanup {
			return ErrTCPReactorStateNotInitOrStopOrCleanup
		}

		var closeErr error
		if this.commandFD >= 0 {
			closeErr = os.NewSyscallError("close", syscall.Close(int(this.commandFD)))
			this.commandFD = -1
		}

		*state = TCPReactorStateCleanup
		return errors.Join(closeErr, this.epoller.Close())
	})
}

// Start starts the event loop with an independent Handler counter and waits for the start
// preparation result.
//
// It returns an error if the Init state check or start preparation fails.
func (this *TCPReactor) Start() error {
	return this.StartWithCounter(&atomic.Uint32{})
}

// StartWithCounter connects a shared Handler counter, starts the event loop, and waits for the
// start preparation result.
//
//   - handlerCounter: shared atomic counter that reflects registration and removal counts.
//
// It returns an error if the Init state check or start preparation fails.
// It returns ErrTCPReactorUninitialized if it was not created with NewTCPReactor,
// ErrTCPReactorNilHandlerCounter if handlerCounter is nil,
// and ErrTCPReactorStateNotInit if it is not in the Init state.
func (this *TCPReactor) StartWithCounter(handlerCounter *atomic.Uint32) error {
	if this.lifecycleState == nil {
		return ErrTCPReactorUninitialized
	}

	if handlerCounter == nil {
		return ErrTCPReactorNilHandlerCounter
	}
	if err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPReactorStateInit {
			return ErrTCPReactorStateNotInit
		}

		this.started.Reset()
		this.stopped.Reset()
		this.handlerCounter = handlerCounter
		*state = TCPReactorStateStart
		return nil
	}); err != nil {
		return err
	}

	go this.run()
	if err := this.started.Wait(); err != nil {
		return err
	}

	return nil
}

// preRun prepares the command Mailbox and read monitoring in the Start state, transitions to Run,
// and delivers the result to the start waiter.
// If the preparation fails, it transitions to Stop.
//
// It returns an error if the state check or start preparation fails.
func (this *TCPReactor) preRun() error {
	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) (err error) {
		defer func() {
			this.started.BroadcastWith(err)
		}()

		if *state != TCPReactorStateStart {
			return ErrTCPReactorStateNotStart
		}

		if err := this.commands.Open(true); err != nil {
			*state = TCPReactorStateStop
			return err
		}

		if err := this.epoller.RegisterRead(this.commandFD); err != nil {
			*state = TCPReactorStateStop
			return err
		}

		*state = TCPReactorStateRun
		return nil
	})
}

// run processes timer, command, and socket events in the event loop, and when stopping, cleans up
// command enqueuing and registered connections.
//
// It returns the start preparation error or the shutdown cleanup result of postRun.
func (this *TCPReactor) run() (err error) {
	if err := this.preRun(); err != nil {
		return err
	}

	defer func() {
		err = this.postRun()
	}()

	isStop := false
	for {
		timeout := this.handleTimeouts()

		events, waitError := this.epoller.Wait(timeout)
		if waitError != nil {
			this.handleError(waitError)
			return waitError
		}

		this.commands.BeginImmediateWrites()
		for _, event := range events {
			fd := internal.FileDescriptor(event.Fd)

			if fd == this.commandFD {
				isStop = this.handleCommands()
			}

			this.handleEvent(fd, event)

			if isStop {
				this.commands.EndImmediateWrites(this.writeImmediately)
				return ErrTCPReactorStopped
			}
		}

		// Transmissions enqueued as immediate writes are performed before the next wait.
		this.commands.EndImmediateWrites(this.writeImmediately)
	}
}

// postRun closes command enqueuing, cleans up pending commands and registered connections, and then
// delivers the result of unregistering command FD monitoring to the stop waiter.
// It does not change the state.
//
// It returns the error from closing command enqueuing.
func (this *TCPReactor) postRun() (err error) {
	stopError := this.commands.Stop(false)
	if stopError != nil && !errors.Is(stopError, ErrCommandMailboxClosed) {
		err = errors.Join(err, stopError)
	}

	this.discardCommands()
	for _, handler := range this.handlers {
		this.removeHandler(handler, ErrTCPReactorStopped, true)
	}

	this.stopped.BroadcastWith(this.epoller.Unregister(this.commandFD))
	return err
}

// RequestStop closes command enqueuing of the running Reactor and requests a stop command.
//
// It returns an error if the run state check or enqueuing the stop command fails.
// It returns ErrTCPReactorUninitialized if it was not created with NewTCPReactor, and
// ErrTCPReactorStateNotRun if it is not in the Run state.
func (this *TCPReactor) RequestStop() (err error) {
	if this.lifecycleState == nil {
		return ErrTCPReactorUninitialized
	}

	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPReactorStateRun {
			return ErrTCPReactorStateNotRun
		}

		return this.commands.EnqueueStop()
	})
}

// WaitStop waits for the shutdown cleanup of the event loop in the Run state and then transitions
// to Stop.
// It does not send a stop command.
//
// It returns the error from unregistering command FD monitoring during shutdown cleanup and the
// state check error.
// It returns ErrTCPReactorUninitialized if it was not created with NewTCPReactor, and
// ErrTCPReactorStateNotRun if it is not in the Run state.
func (this *TCPReactor) WaitStop() error {
	if this.lifecycleState == nil {
		return ErrTCPReactorUninitialized
	}

	if err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPReactorStateRun {
			return ErrTCPReactorStateNotRun
		}

		return nil
	}); err != nil {
		return err
	}

	err1 := this.stopped.Wait()
	err2 := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		*state = TCPReactorStateStop
		return nil
	})

	return errors.Join(err1, err2)
}

// RegisterHandler enqueues a Handler registration command, and the actual registration is processed
// in the event loop.
//
//   - handler: Handler whose connection ownership is handed over to the Reactor when the
//     registration command is enqueued successfully.
//
// It returns an error if enqueuing fails, and after enqueuing, the registration result is delivered
// through Handler callbacks.
// It returns ErrTCPReactorUninitialized if it was not created with NewTCPReactor,
// ErrTCPReactorNilHandler if handler is nil,
// ErrTCPReactorNilHandlerContext if HandlerContext is nil, ErrHandlerContextAlreadyBound if the
// Handler is already registered,
// ErrTCPReactorInvalidFileDescriptor if the FD is negative, and ErrTCPReactorStopped if it is not
// in the Run state.
// It returns ErrCommandMailboxClosed after RequestStop and ErrTCPReactorStopped after WaitStop,
// and ErrCommandMailboxRegisterQuotaExceeded if pending registration commands reach registerCommandQuota.
func (this *TCPReactor) RegisterHandler(handler ClientHandler) error {
	if this.lifecycleState == nil {
		return ErrTCPReactorUninitialized
	}

	if handler == nil {
		return ErrTCPReactorNilHandler
	}

	if handler.context() == nil {
		return ErrTCPReactorNilHandlerContext
	}

	if handler.context().isBound() {
		return ErrHandlerContextAlreadyBound
	}

	fd := handler.context().fileDescriptor()
	if fd < 0 {
		return ErrTCPReactorInvalidFileDescriptor
	}

	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPReactorStateRun {
			return ErrTCPReactorStopped
		}

		return this.commands.EnqueueRegisterHandler(handler)
	})
}

// HandlerCount returns the number of registered Handlers recorded in the shared counter.
//
// If multiple Reactors share the counter, it returns the summed count, and it is 0 if there is no
// counter.
func (this *TCPReactor) HandlerCount() uint32 {
	if this.handlerCounter == nil {
		return 0
	}

	return this.handlerCounter.Load()
}

// handleTimeouts processes expired timers and updates the next expiration time of each Handler.
// If there are expired timers, Writes requested in OnTimeout are processed as immediate writes.
//
// It is the epoll wait time, -1 if there is no timer, with an upper limit of MaxInt32 milliseconds.
func (this *TCPReactor) handleTimeouts() time.Duration {
	if expired := this.clientTimer.PopExpired(); len(expired) > 0 {
		this.commands.BeginImmediateWrites()
		for _, handler := range expired {
			nextExpiration, hasTimer := handler.context().onTimeout()
			this.updateClientTimer(handler, nextExpiration, hasTimer)
		}
		this.commands.EndImmediateWrites(this.writeImmediately)
	}

	timeout, hasTimer := this.clientTimer.NextTimeout()
	if !hasTimer {
		return -1
	}

	if timeout > time.Duration(math.MaxInt32)*time.Millisecond {
		return time.Duration(math.MaxInt32) * time.Millisecond
	}

	return timeout
}

// writeImmediately performs the transmission enqueued as an immediate write, and registers write
// monitoring if unsent data remains.
//
//   - handler: target of the transmission, and it is ignored if it differs from the currently
//     registered object.
func (this *TCPReactor) writeImmediately(handler ClientHandler) {
	if handler == nil {
		return
	}

	fd := handler.context().fileDescriptor()
	if registered, contains := this.handlers[fd]; !contains || registered != handler {
		return
	}

	if handler.context().onWritable() == false {
		return
	}

	if err := this.epoller.RegisterWrite(fd); err != nil {
		handler.OnError(handler.context(), err)
	}
}

// handleError delivers the Reactor error if the error callback is set.
//
//   - err: error to deliver to the callback.
func (this *TCPReactor) handleError(err error) {
	if this.errorCallback == nil {
		return
	}

	this.errorCallback(err)
}

// discardCommands discards pending commands and cleans up each not-yet-registered connection once
// per FD.
// Already registered FDs and the command FD are excluded from cleanup.
func (this *TCPReactor) discardCommands() {
	descriptors := make(map[internal.FileDescriptor]struct{})
	commands := this.commands.Drain(this.errorCallback)
	for _, command := range commands {
		if command.commandType != commandRegisterHandler || command.handler == nil {
			continue
		}

		fd := command.handler.context().fileDescriptor()
		if _, registered := this.handlers[fd]; registered || fd == this.commandFD || fd < 0 {
			continue
		}

		if _, discarded := descriptors[fd]; discarded {
			continue
		}

		descriptors[fd] = struct{}{}
		this.rejectHandler(command.handler, ErrTCPReactorStopped)
	}

	clear(commands)
}
