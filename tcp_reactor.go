package erio

import (
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/netiogreem/erio/internal"
)

// TCPReactorError is the error type returned by [TCPReactor] methods and passed to handler callbacks.
// It is a string so that each error can be declared as a comparable constant.
type TCPReactorError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this TCPReactorError) Error() string {
	return string(this)
}

const (
	TCPReactorStateStopped  internal.StateGuardType = iota // Event loop is not running. The initial state and the state after cleanup.
	TCPReactorStateStart                                   // Start succeeded and the event loop goroutine is starting.
	TCPReactorStateRunning                                 // Event loop is processing events and commands.
	TCPReactorStateStopping                                // Stop was requested or cleanup is in progress.
)

const (
	ErrTCPReactorInvalidFileDescriptor TCPReactorError = "erio: invalid tcp reactor handler file descriptor"       // Returned by [TCPReactor.RegisterHandler], or passed to [ClientHandler.OnError], when the handler's file descriptor is negative.
	ErrTCPReactorCommand               TCPReactorError = "erio: invalid tcp reactor command"                       // Passed to errorCallback when a drained command has an unknown type.
	ErrTCPReactorNilHandler            TCPReactorError = "erio: tcp reactor handler is nil"                        // Returned by [TCPReactor.RegisterHandler] when handler is nil, or passed to errorCallback when a drained command has no handler.
	ErrTCPReactorNilHandlerContext     TCPReactorError = "erio: tcp reactor handler context is nil"                // Returned by [TCPReactor.RegisterHandler] when the handler's context is nil.
	ErrTCPReactorNilHandlerCounter     TCPReactorError = "erio: tcp reactor handler counter is nil"                // Returned by [TCPReactor.StartWithCounter] when handlerCounter is nil.
	ErrTCPReactorDuplicateFD           TCPReactorError = "erio: tcp reactor file descriptor is already registered" // Passed to [ClientHandler.OnError] when the handler's file descriptor is already registered or is the reactor's own eventfd.
	ErrTCPReactorCloseRequested        TCPReactorError = "erio: connection closed by Close"                        // Close reason passed to [ClientHandler.OnClose] after a graceful close finished sending pending data.
	ErrTCPReactorAbortiveClose         TCPReactorError = "erio: connection closed by AbortiveClose"                // Close reason passed to [ClientHandler.OnClose] after an abortive close.
	ErrTCPReactorAbortiveCloseTimeout  TCPReactorError = "erio: connection closed by abortive close timeout"       // Close reason passed to [ClientHandler.OnClose] when a graceful close did not finish before its deadline.
	ErrTCPReactorHangup                TCPReactorError = "erio: client connection is closed"                       // Close reason passed to [ClientHandler.OnClose] when epoll reports EPOLLHUP on the connection.
	ErrTCPReactorStopped               TCPReactorError = "erio: tcp reactor is stopped"                            // Close reason passed to [ClientHandler.OnClose], or error passed to [ClientHandler.OnError] for pending registrations, when the reactor stops.
	ErrTCPReactorAlreadyStopped        TCPReactorError = "erio: tcp reactor is already stopped"                    // Returned by [TCPReactor.WaitStopped] when the reactor was never started.
	ErrTCPReactorStateNotStopped       TCPReactorError = "erio: tcp reactor is not in the Stopped state"           // Returned by [TCPReactor.Start] and [TCPReactor.StartWithCounter] when the reactor is already started.
	ErrTCPReactorStateNotStart         TCPReactorError = "erio: tcp reactor is not in the Start state"             // Returned by [TCPReactor.Start] when the event loop goroutine finds the reactor in an unexpected state.
	ErrTCPReactorStateNotRunning       TCPReactorError = "erio: tcp reactor is not in the Running state"           // Returned by [TCPReactor.Stop] when the reactor is not running, including before the first [TCPReactor.Start].
	ErrTCPReactorUninitialized         TCPReactorError = "erio: tcp reactor is uninitialized"                      // Returned when the reactor was not created with [NewTCPReactor].
	ErrTCPReactorInvalidReadBufferSize TCPReactorError = "erio: invalid tcp reactor read buffer size"              // Returned by [TCPReactor.Start] when readBufferSize is 0 or exceeds the maximum slice capacity.
)

// TCPReactor runs a single-threaded epoll event loop that reads from, writes to, and times
// out registered [ClientHandler] connections.
//
// Notes:
//   - The reactor can be started again after [TCPReactor.WaitStopped] returns.
//   - All handler callbacks run on the event loop goroutine.
//   - Methods are safe to call from multiple goroutines.
type TCPReactor struct {
	startMutex           sync.Mutex                   // Serializes Start calls.
	holder               *internal.Holder[tcpReactor] // Current event loop instance, nil before the first Start.
	eventBatchSize       uint32                       // Maximum epoll events handled per wait.
	commandsReserveSize  uint32                       // Initial capacity of the command queue.
	registerCommandQuota uint32                       // Maximum register commands queued per event loop iteration.
	readBufferSize       uint32                       // Size of the read buffer shared by all handlers.
	errorCallback        func(error)                  // Receives errors not tied to a handler callback. nil drops them.
}

// tcpReactor is one run of the event loop owned by a [TCPReactor].
type tcpReactor struct {
	epoller        *internal.Epoller                         // epoll instance for connection and eventfd events.
	commandFD      internal.FileDescriptor                   // eventfd that wakes the loop when a command is queued; -1 after cleanup.
	commands       *commandMailbox                           // Command queue from other goroutines to the loop.
	readBuffer     []byte                                    // Buffer reused for every read.
	handlers       map[internal.FileDescriptor]ClientHandler // Registered handlers keyed by their socket descriptor.
	handlerCounter *atomic.Uint32                            // Number of registered handlers; may be shared across reactors.
	clientTimer    *clientTimer                              // Earliest timer expiration per handler.
	state          *internal.StateGuard                      // Current lifecycle state.
	started        internal.SignalVal[error]                 // Signaled when the loop goroutine is running, with its start error.
	stopped        internal.SignalVal[error]                 // Signaled when cleanup finished, with the epoll and eventfd release error.
	errorCallback  func(error)                               // Receives errors not tied to a handler callback. nil drops them.
}

// NewTCPReactor creates [TCPReactor].
//
// Parameters:
//   - eventBatchSize is the maximum number of epoll events handled per wait. Must be between 1 and
//     math.MaxInt32 divided by the size of syscall.EpollEvent.
//   - commandsReserveSize is the initial capacity of the command queue. Must be within the range
//     accepted by [newCommandMailbox].
//   - registerCommandQuota is the maximum number of register commands queued per event loop
//     iteration. Must be within the range accepted by [newCommandMailbox].
//   - readBufferSize is the size of the read buffer shared by all handlers. Must be between 1 and
//     the maximum slice capacity.
//   - ErrorCallback receives errors that are not tied to a handler callback, such as epoll failures.
//     nil drops them.
//
// Returns:
//   - The stopped reactor.
//
// Notes:
//   - The parameters are validated at [TCPReactor.Start], not here.
func NewTCPReactor(
	eventBatchSize uint32,
	commandsReserveSize uint32,
	registerCommandQuota uint32,
	readBufferSize uint32,
	ErrorCallback func(error)) *TCPReactor {
	return &TCPReactor{
		startMutex:           sync.Mutex{},
		holder:               &internal.Holder[tcpReactor]{},
		eventBatchSize:       eventBatchSize,
		commandsReserveSize:  commandsReserveSize,
		registerCommandQuota: registerCommandQuota,
		readBufferSize:       readBufferSize,
		errorCallback:        ErrorCallback,
	}
}

// Start starts the reactor.
//
// Returns:
//   - The values of [TCPReactor.StartWithCounter] except [ErrTCPReactorNilHandlerCounter].
func (this *TCPReactor) Start() error {
	return this.StartWithCounter(&atomic.Uint32{})
}

// StartWithCounter starts the reactor and counts its registered handlers in handlerCounter,
// so several reactors can share one count.
//
// Parameters:
//   - handlerCounter is incremented when a handler is registered and decremented when it is removed.
//     Pass the same counter to several reactors to count their handlers together. Must not be nil.
//
// Returns:
//   - nil when the event loop is running.
//   - [ErrTCPReactorUninitialized] when the reactor was not created with [NewTCPReactor].
//   - [ErrTCPReactorNilHandlerCounter] when handlerCounter is nil.
//   - [ErrTCPReactorStateNotStopped] when the previous event loop has not reached the stopped state.
//   - [ErrTCPReactorInvalidReadBufferSize] when readBufferSize given to NewTCPReactor is out of range.
//   - An error wrapping [syscall.EINVAL] when eventBatchSize given to NewTCPReactor is out of range.
//   - [ErrCommandMailboxInvalidReserveSize] or [ErrCommandMailboxInvalidRegisterCommandQuota] when the
//     matching size given to NewTCPReactor is out of range.
//   - A system call error when creating the epoll instance or eventfd fails.
//
// Notes:
//   - Returns after the reactor is ready to accept [TCPReactor.RegisterHandler].
//   - Call [TCPReactor.WaitStopped] before starting again; [TCPReactor.Stop] alone leaves the reactor in the stopping state.
func (this *TCPReactor) StartWithCounter(handlerCounter *atomic.Uint32) error {
	this.startMutex.Lock()
	defer this.startMutex.Unlock()

	if this.holder == nil {
		return ErrTCPReactorUninitialized
	}

	if handlerCounter == nil {
		return ErrTCPReactorNilHandlerCounter
	}

	if current := this.holder.Load(); current != nil {
		if current.IsStopped() == false {
			return ErrTCPReactorStateNotStopped
		}
	}

	reactor, err := newTCPReactor(this.eventBatchSize,
		this.commandsReserveSize,
		this.registerCommandQuota,
		this.readBufferSize,
		this.errorCallback)
	if err != nil {
		return err
	}

	if err := reactor.StartWithCounter(handlerCounter); err != nil {
		return err
	}

	this.holder.Store(reactor)
	return nil
}

// Stop stops the reactor. After every request submitted before Stop has been processed, each
// registered handler receives [ClientHandler.OnClose] with [ErrTCPReactorStopped] as the close reason.
//
// Returns:
//   - nil when the stop was requested.
//   - [ErrTCPReactorUninitialized] when the reactor was not created with [NewTCPReactor].
//   - [ErrTCPReactorStateNotRunning] when the reactor is not running, including before the first
//     [TCPReactor.Start] and after a previous Stop.
//   - A system call error when waking the reactor fails.
//
// Notes:
//   - Returns without waiting. Call [TCPReactor.WaitStopped] to wait until the reactor has stopped.
func (this *TCPReactor) Stop() error {
	if this.holder == nil {
		return ErrTCPReactorUninitialized
	}

	reactor := this.holder.Load()
	if reactor == nil {
		return ErrTCPReactorStateNotRunning
	}

	return reactor.Stop()
}

// WaitStopped waits until the reactor has stopped and released its epoll instance and eventfd.
//
// Returns:
//   - nil when the reactor stopped and released its resources without error.
//   - [ErrTCPReactorUninitialized] when the reactor was not created with [NewTCPReactor].
//   - [ErrTCPReactorAlreadyStopped] when the reactor was never started.
//   - The joined errors from releasing the epoll instance and eventfd.
func (this *TCPReactor) WaitStopped() error {
	if this.holder == nil {
		return ErrTCPReactorUninitialized
	}

	reactor := this.holder.Load()
	if reactor == nil {
		return ErrTCPReactorAlreadyStopped
	}

	return reactor.WaitStopped()
}

// RegisterHandler requests registration of handler with the reactor. Once registered, the reactor
// delivers the connection's events to the handler's callbacks, starting with [ClientHandler.OnConnect].
//
// Parameters:
//   - handler owns an open, not yet registered socket and must not be nil.
//
// Returns:
//   - nil when the handler was accepted. Registration finishes asynchronously; failures after this
//     point are reported through [ClientHandler.OnError] only, and OnConnect and OnClose are not called.
//   - [ErrTCPReactorUninitialized] when the reactor was not created with [NewTCPReactor].
//   - [ErrCommandMailboxClosed] when the reactor is not started or is stopping.
//   - [ErrTCPReactorNilHandler] when handler is nil.
//   - [ErrTCPReactorNilHandlerContext] when the handler's context is nil.
//   - [ErrHandlerContextAlreadyBound] when the handler is already registered with a reactor.
//   - [ErrTCPReactorInvalidFileDescriptor] when the handler's file descriptor is negative.
//   - [ErrCommandMailboxRegisterQuotaExceeded] when more than registerCommandQuota handlers are registered at a time.
//   - A system call error when waking the reactor fails.
//
// Notes:
//   - When an error is returned, no [ClientHandler] callback is called.
func (this *TCPReactor) RegisterHandler(handler ClientHandler) error {
	if this.holder == nil {
		return ErrTCPReactorUninitialized
	}

	reactor := this.holder.Load()
	if reactor == nil {
		return ErrCommandMailboxClosed
	}

	return reactor.RegisterHandler(handler)
}

// HandlerCount returns the number of connected clients registered with this reactor, or the shared
// count when started with [TCPReactor.StartWithCounter].
//
// Returns:
//   - The number of registered handlers.
//   - 0 when the reactor was never started or is a zero value.
func (this *TCPReactor) HandlerCount() uint32 {
	if this.holder == nil {
		return 0
	}

	reactor := this.holder.Load()
	if reactor == nil {
		return 0
	}

	return reactor.HandlerCount()
}

// newTCPReactor creates a tcpReactor in [TCPReactorStateStopped] with its epoller, commandFD (an eventfd),
// readBuffer, and commands mailbox.
//
// Parameters:
//   - All parameters have the same meaning and constraints as those of [NewTCPReactor].
//
// Returns:
//   - The tcpReactor and nil when [internal.NewEpoller], eventfd2, and [newCommandMailbox] all succeeded.
//   - [ErrTCPReactorInvalidReadBufferSize] when readBufferSize is 0 or exceeds the maximum slice capacity.
//   - The [internal.NewEpoller] error when eventBatchSize is out of range or epoll_create1 fails.
//   - The eventfd2 system call error, joined with the epoller.Close error.
//   - The [newCommandMailbox] error, joined with the syscall.Close(commandFD) and epoller.Close errors.
//
// Notes:
//   - When eventfd2 or newCommandMailbox fails, epoller.Close is called, and commandFD is closed when it was
//     created. Their errors are joined to the returned error.
func newTCPReactor(
	eventBatchSize uint32,
	commandsReserveSize uint32,
	registerCommandQuota uint32,
	readBufferSize uint32,
	errorCallback func(error)) (*tcpReactor, error) {
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

	reactor := &tcpReactor{
		epoller:        epoller,
		commandFD:      internal.FileDescriptor(commandFD),
		commands:       nil,
		readBuffer:     make([]byte, int(readBufferSize)),
		handlers:       make(map[internal.FileDescriptor]ClientHandler),
		handlerCounter: &atomic.Uint32{},
		clientTimer:    newClientTimer(),
		state:          internal.NewStateGuard(TCPReactorStateStopped),
		errorCallback:  errorCallback,
	}

	commands, err := newCommandMailbox(reactor.commandFD, commandsReserveSize, registerCommandQuota)
	if err != nil {
		return nil, errors.Join(err, os.NewSyscallError("close", syscall.Close(int(reactor.commandFD))), epoller.Close())
	}

	reactor.commands = commands

	return reactor, nil
}

// StartWithCounter calls epoller.RegisterRead(commandFD) and commands.Open(true), sets state to
// [TCPReactorStateStart], starts [tcpReactor.run] as a goroutine, and returns started.Wait().
//
// Parameters:
//   - handlerCounter replaces the handlerCounter created by [newTCPReactor]. The caller guarantees it is not nil.
//
// Returns:
//   - nil when run broadcast nil through started, that is, when it moved state to [TCPReactorStateRunning].
//   - [ErrTCPReactorStateNotStopped] when state is not [TCPReactorStateStopped].
//   - The epoller.RegisterRead error, joined with the syscall.Close(commandFD) and epoller.Close errors.
//   - The commands.Open error, joined with the epoller.Unregister(commandFD), syscall.Close(commandFD), and
//     epoller.Close errors.
//   - [ErrTCPReactorStateNotStart] when run found state changed before it ran.
//
// Notes:
//   - After a RegisterRead or Open error, commandFD and epoller are already closed, so this tcpReactor
//     cannot be started again. [TCPReactor.StartWithCounter] creates a new one on the next call.
func (this *tcpReactor) StartWithCounter(handlerCounter *atomic.Uint32) error {
	if err := this.state.Access(func(state *internal.StateGuardType) error {
		if *state != TCPReactorStateStopped {
			return ErrTCPReactorStateNotStopped
		}

		if err := this.epoller.RegisterRead(this.commandFD); err != nil {
			return errors.Join(err,
				os.NewSyscallError("close", syscall.Close(int(this.commandFD))),
				this.epoller.Close())
		}

		if err := this.commands.Open(true); err != nil {
			return errors.Join(err,
				this.epoller.Unregister(this.commandFD),
				os.NewSyscallError("close", syscall.Close(int(this.commandFD))),
				this.epoller.Close())
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
	return this.started.Wait()
}

// IsStopped reports whether the state is Stopped.
//
// Returns:
//   - true when the state is [TCPReactorStateStopped].
//   - false in every other state.
func (this *tcpReactor) IsStopped() bool {
	stopped := true
	this.state.Access(func(state *internal.StateGuardType) error {
		if *state != TCPReactorStateStopped {
			stopped = false
		}
		return nil
	})
	return stopped
}

// State returns the current lifecycle state.
//
// Returns:
//   - One of [TCPReactorStateStopped], [TCPReactorStateStart], [TCPReactorStateRunning], or [TCPReactorStateStopping].
func (this *tcpReactor) State() internal.StateGuardType {
	curr := TCPReactorStateStopped
	this.state.Access(func(state *internal.StateGuardType) error {
		curr = *state
		return nil
	})

	return curr
}

// run is the event loop goroutine. Each iteration calls [tcpReactor.handleTimeouts], epoller.Wait,
// [tcpReactor.handleCommands] when the event is for commandFD, [tcpReactor.handleEvent] for every event, and
// commands.EndImmediateWrites([tcpReactor.writeImmediately]) at the end.
//
// Returns:
//   - [ErrTCPReactorStateNotStart] when state is not [TCPReactorStateStart] on entry. [tcpReactor.cleanup]
//     does not run in this case.
//   - The [tcpReactor.cleanup] return value otherwise. The deferred cleanup overwrites the return value after
//     the loop ends, either because handleCommands returned isStop true or because epoller.Wait returned an error.
//
// Notes:
//   - Started as "go this.run()" by [tcpReactor.StartWithCounter], which discards the return value.
//   - The state check result is broadcast through started.BroadcastWith before the loop begins.
//   - An epoller.Wait error is passed to [tcpReactor.handleError] and ends the loop.
func (this *tcpReactor) run() (err error) {
	if err := this.state.Access(func(state *internal.StateGuardType) (err error) {
		defer func() {
			this.started.BroadcastWith(err)
		}()

		if *state != TCPReactorStateStart {
			return ErrTCPReactorStateNotStart
		}

		*state = TCPReactorStateRunning
		return nil
	}); err != nil {
		return err
	}

	defer func() {
		err = this.cleanup()
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

		this.commands.EndImmediateWrites(this.writeImmediately)
	}
}

// cleanup sets state to [TCPReactorStateStopping], calls commands.Stop(false) and
// [tcpReactor.discardCommands], calls [tcpReactor.removeHandler] for every handler in handlers with
// [ErrTCPReactorStopped] and soLinger false, releases commandFD and epoller, and sets state to
// [TCPReactorStateStopped].
//
// Returns:
//   - nil when commands.Stop returned nil or [ErrCommandMailboxClosed].
//   - The commands.Stop error otherwise.
//
// Notes:
//   - The epoller.Unregister(commandFD), syscall.Close(commandFD), and epoller.Close errors are joined and
//     broadcast through stopped.BroadcastWith, so [tcpReactor.WaitStopped] returns them. They are not returned here.
//   - commandFD is set to -1 after it is closed.
func (this *tcpReactor) cleanup() (err error) {
	this.state.Access(func(state *internal.StateGuardType) error {
		*state = TCPReactorStateStopping
		return nil
	})

	stopError := this.commands.Stop(false)
	if stopError != nil && !errors.Is(stopError, ErrCommandMailboxClosed) {
		err = errors.Join(err, stopError)
	}

	this.discardCommands()
	for _, handler := range this.handlers {
		this.removeHandler(handler, ErrTCPReactorStopped, false)
	}

	releaseError := errors.Join(this.epoller.Unregister(this.commandFD),
		os.NewSyscallError("close", syscall.Close(int(this.commandFD))),
		this.epoller.Close())

	this.commandFD = -1

	return this.state.Access(func(state *internal.StateGuardType) error {
		*state = TCPReactorStateStopped
		this.stopped.BroadcastWith(releaseError)
		return err
	})
}

// Stop calls commands.EnqueueStop and sets state to [TCPReactorStateStopping].
//
// Returns:
//   - nil when EnqueueStop returned nil.
//   - [ErrTCPReactorStateNotRunning] when state is not [TCPReactorStateRunning].
//   - The [commandMailbox.EnqueueStop] error.
//
// Notes:
//   - On any error state is unchanged.
func (this *tcpReactor) Stop() (err error) {
	return this.state.Access(func(state *internal.StateGuardType) error {
		if *state != TCPReactorStateRunning {
			return ErrTCPReactorStateNotRunning
		}

		if err := this.commands.EnqueueStop(); err != nil {
			return err
		}

		*state = TCPReactorStateStopping
		return nil
	})
}

// WaitStopped returns stopped.Wait(), which blocks until [tcpReactor.cleanup] has broadcast.
//
// Returns:
//   - nil when epoller.Unregister(commandFD), syscall.Close(commandFD), and epoller.Close all succeeded in cleanup.
//   - Those errors joined otherwise.
func (this *tcpReactor) WaitStopped() error {
	return this.stopped.Wait()
}

// RegisterHandler validates handler and calls commands.EnqueueRegisterHandler(handler).
//
// Parameters:
//   - handler is the handler to register. It must not be nil, handler.context() must not be nil, and
//     handler.context().isBound() must be false.
//
// Returns:
//   - nil when EnqueueRegisterHandler returned nil.
//   - [ErrTCPReactorNilHandler] when handler is nil.
//   - [ErrTCPReactorNilHandlerContext] when handler.context() is nil.
//   - [ErrHandlerContextAlreadyBound] when handler.context().isBound() is true.
//   - [ErrTCPReactorInvalidFileDescriptor] when handler.context().fileDescriptor() is negative.
//   - The [commandMailbox.EnqueueRegisterHandler] error.
func (this *tcpReactor) RegisterHandler(handler ClientHandler) error {
	if handler == nil {
		return ErrTCPReactorNilHandler
	}

	if handler.context() == nil {
		return ErrTCPReactorNilHandlerContext
	}

	if handler.context().isBound() {
		return ErrHandlerContextAlreadyBound
	}

	if handler.context().fileDescriptor() < 0 {
		return ErrTCPReactorInvalidFileDescriptor
	}

	return this.commands.EnqueueRegisterHandler(handler)
}

// HandlerCount returns handlerCounter.Load().
//
// Returns:
//   - The number of registered handlers. The count covers every reactor that shares this handlerCounter.
func (this *tcpReactor) HandlerCount() uint32 {
	return this.handlerCounter.Load()
}

// handleTimeouts calls handler.context().onTimeout for every handler returned by clientTimer.PopExpired,
// then returns the timeout for epoller.Wait from clientTimer.NextTimeout.
//
// Returns:
//   - -1 when clientTimer.NextTimeout reports no timer.
//   - The clientTimer.NextTimeout value, capped at math.MaxInt32 milliseconds.
//
// Notes:
//   - clientTimer holds one time per handler. That time comes from [HandlerContext.nextExpiration], which is
//     the earlier of timer.NextExpiration and abortiveCloseDeadline. abortiveCloseDeadline is set by
//     [HandlerContext.setAbortiveCloseDeadline] from [tcpReactor.handleCommandClose]. handleTimeouts cannot
//     tell which one expired, so it calls [HandlerContext.onTimeout], which calls [ClientHandler.OnTimeout]
//     for each expired timerKey and returns aborted true when abortiveCloseDeadline has passed.
//   - When aborted is true, commands.EnqueueUnregisterHandler(handler, [ErrTCPReactorAbortiveCloseTimeout],
//     true) is called. The loop later runs [tcpReactor.handleCommandUnregisterHandler], which calls
//     [tcpReactor.removeHandler] with soLinger true: the socket is closed with RST and
//     [ClientHandler.OnClose] receives [ErrTCPReactorAbortiveCloseTimeout].
//   - When EnqueueUnregisterHandler returns an error, handler.OnError receives it and removeHandler is
//     called directly. That is acceptable here because the deadline already means writeBuffer data and
//     queued commands are discarded.
//   - When aborted is false, [tcpReactor.updateClientTimer] is called with the nextExpiration and hasTimer
//     returned by onTimeout.
//   - The expired handlers are processed between commands.BeginImmediateWrites and
//     commands.EndImmediateWrites([tcpReactor.writeImmediately]), so [HandlerContext.Write] calls made in
//     OnTimeout are written before returning.
func (this *tcpReactor) handleTimeouts() time.Duration {
	if expired := this.clientTimer.PopExpired(); len(expired) > 0 {
		this.commands.BeginImmediateWrites()
		for _, handler := range expired {
			nextExpiration, hasTimer, aborted := handler.context().onTimeout()
			if aborted == true {
				if err := this.commands.EnqueueUnregisterHandler(handler, ErrTCPReactorAbortiveCloseTimeout, true); err != nil {
					handler.OnError(handler.context(), err)
					this.removeHandler(handler, ErrTCPReactorAbortiveCloseTimeout, true)
				}
				continue
			}
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

// writeImmediately is the writer passed to commands.EndImmediateWrites. It writes the writeBuffer of a
// handler that called [HandlerContext.Write] during the current iteration.
//
// Parameters:
//   - handler is the handler recorded by [commandMailbox.enqueueImmediateWrite]. nil is ignored.
//
// Notes:
//   - Returns when handlers[fd] is missing or holds a different handler, where fd is
//     handler.context().fileDescriptor().
//   - Calls [HandlerContext.onWritable], which writes writeBuffer to the socket until it is empty or write
//     returns EAGAIN.
//   - When onWritable returns true (data remains), epoller.RegisterWrite(fd) arms EPOLLOUT so
//     [tcpReactor.handleEventWritable] continues the write. A RegisterWrite error is passed to handler.OnError.
//   - When onWritable returns false and [HandlerContext.isDrainedForClose] is true,
//     [tcpReactor.removeHandler] is not called here. Instead commands.EnqueueUnregisterHandler(handler,
//     [ErrTCPReactorCloseRequested], handler.context().IsAbortiveClosed()) is called. The loop later drains
//     that command in [tcpReactor.handleCommands] and runs [tcpReactor.handleCommandUnregisterHandler], which
//     calls removeHandler: the socket is closed and [ClientHandler.OnClose] receives
//     [ErrTCPReactorCloseRequested]. Commands for this handler that were queued earlier are drained first.
//   - The EnqueueUnregisterHandler error is ignored. Calling removeHandler here would skip those earlier
//     commands. The handler is removed later by [tcpReactor.handleEventHangup], by
//     [tcpReactor.handleTimeouts] when abortiveCloseDeadline passes, or by [tcpReactor.cleanup].
func (this *tcpReactor) writeImmediately(handler ClientHandler) {
	if handler == nil {
		return
	}

	fd := handler.context().fileDescriptor()
	if registered, contains := this.handlers[fd]; !contains || registered != handler {
		return
	}

	if handler.context().onWritable() == false {
		if handler.context().isDrainedForClose() == true {
			this.commands.EnqueueUnregisterHandler(handler,
				ErrTCPReactorCloseRequested,
				handler.context().IsAbortiveClosed())
		}
		return
	}

	if err := this.epoller.RegisterWrite(fd); err != nil {
		handler.OnError(handler.context(), err)
	}
}

// handleError passes err to errorCallback.
//
// Parameters:
//   - err is the error not tied to a handler callback.
//
// Notes:
//   - Does nothing when errorCallback is nil.
func (this *tcpReactor) handleError(err error) {
	if this.errorCallback == nil {
		return
	}

	this.errorCallback(err)
}

// discardCommands calls commands.Drain(errorCallback) during [tcpReactor.cleanup] and rejects the
// commandRegisterHandler commands whose handler was never added to handlers.
//
// Notes:
//   - For each commandRegisterHandler whose command.handler.context().fileDescriptor() is not in handlers,
//     not commandFD, and not negative, [tcpReactor.rejectHandler](command.handler, [ErrTCPReactorStopped])
//     is called once per file descriptor. rejectHandler aborts the socket and passes the error to
//     [ClientHandler.OnError].
//   - Every other command, including commandRegisterHandler with a nil handler, is skipped.
func (this *tcpReactor) discardCommands() {
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
