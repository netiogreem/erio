package erio

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"reflect"
	"sync/atomic"
	"syscall"

	"github.com/netiogreem/erio/internal"
)

// AcceptorError is the error type returned by [Acceptor] methods.
// It is a string so that each error can be declared as a comparable constant.
type AcceptorError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this AcceptorError) Error() string {
	return string(this)
}

const (
	ErrAcceptorInvalidAddress      AcceptorError = "erio: invalid acceptor listen address"               // Returned by [NewAcceptor], wrapped with the parse error, when listenAddress is not a valid "host:port".
	ErrAcceptorUninitialized       AcceptorError = "erio: acceptor is uninitialized"                     // Returned when the acceptor was not created with [NewAcceptor].
	ErrAcceptorNilFactory          AcceptorError = "erio: acceptor factory is nil"                       // Returned by [NewAcceptor] when factory is nil.
	ErrAcceptorNilAcceptedCallback AcceptorError = "erio: acceptor accepted callback is nil"             // Reported to errorCallback for every accepted connection when acceptedCallback is nil.
	ErrAcceptorInvalidHandler      AcceptorError = "erio: acceptor factory returned a typed nil handler" // Reported to errorCallback when factory returns a nil pointer wrapped in a non-nil interface.
	ErrAcceptorStateNotRunning     AcceptorError = "erio: acceptor is not in the Running state"          // Returned by [Acceptor.Stop] when the acceptor is not running.
	ErrAcceptorStateNotWaitRunning AcceptorError = "erio: acceptor is not in the WaitRunning state"      // Reported to errorCallback when the accept goroutine finds the acceptor in an unexpected state.
	ErrAcceptorStateNotStopped     AcceptorError = "erio: acceptor is not in the Stopped state"          // Returned by [Acceptor.Start] when the acceptor is already started.
	ErrAcceptorStateNotWaitStopped AcceptorError = "erio: acceptor is not in the WaitStop state"         // Returned by [Acceptor.WaitStopped] when [Acceptor.Stop] has not been called.
)

const (
	AcceptorStateStopped     internal.StateGuardType = iota // Not listening. The initial state and the state after [Acceptor.WaitStopped].
	AcceptorStateWaitRunning                                // Listener is open and the accept goroutine is starting.
	AcceptorStateRunning                                    // Accepting connections.
	AcceptorStateWaitStopped                                // Listener is closed and the accept goroutine is exiting.
)

// Acceptor listens on a TCP address and hands each accepted connection to a
// [ClientHandler] created by a [ClientHandlerFactory].
//
// Notes:
//   - Lifecycle is [Acceptor.Start] -> [Acceptor.Stop] -> [Acceptor.WaitStopped], and the
//     acceptor can be started again after WaitStopped.
//   - Connections are accepted on a dedicated goroutine; factory and callbacks run on that goroutine.
//   - Methods are safe to call from multiple goroutines.
type Acceptor struct {
	listener            *net.TCPListener               // Open listener while running, nil before the first Start.
	listenAddrPortParam netip.AddrPort                 // Address given to NewAcceptor; port may be 0.
	listenAddrPort      atomic.Pointer[netip.AddrPort] // Address actually bound, updated at Start so ListenAddrPort is lock-free.
	factory             ClientHandlerFactory           // Creates a handler for each accepted connection.
	acceptedCallback    func(ClientHandler) error      // Receives each created handler. nil rejects every connection.
	errorCallback       func(error)                    // Receives accept and rejection errors. nil drops them.
	state               *internal.StateGuard           // Current lifecycle state; nil for a zero-value Acceptor.
	started             *internal.Signal               // Signaled when the accept goroutine is running.
	stopped             *internal.Signal               // Signaled when the accept goroutine has exited.
}

// NewAcceptor creates an [Acceptor] for listenAddress. Listening starts at [Acceptor.Start].
//
// Parameters:
//   - listenAddress is an "IP:port" literal; host names are rejected. Port 0 picks a free port at [Acceptor.Start].
//   - factory creates the handler for each accepted connection and must not be nil.
//   - acceptedCallback receives each created handler, typically to register it with a reactor. Its error
//     rejects the connection. nil rejects every connection with [ErrAcceptorNilAcceptedCallback].
//   - errorCallback receives accept errors and rejection reasons. nil drops them.
//
// Returns:
//   - The stopped acceptor and nil.
//   - [ErrAcceptorInvalidAddress] wrapped with the parse error when listenAddress is not an "IP:port" literal.
//   - [ErrAcceptorNilFactory] when factory is nil.
//
// Caution:
//   - When factory returns an error or a nil handler, the acceptor closes fd. factory must not close it.
func NewAcceptor(listenAddress string, factory ClientHandlerFactory, acceptedCallback func(ClientHandler) error, errorCallback func(error)) (*Acceptor, error) {
	listenAddrPort, err := netip.ParseAddrPort(listenAddress)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAcceptorInvalidAddress, err)
	}

	if factory == nil {
		return nil, ErrAcceptorNilFactory
	}

	acceptor := &Acceptor{
		listener:            nil,
		listenAddrPortParam: listenAddrPort,
		listenAddrPort:      atomic.Pointer[netip.AddrPort]{},
		factory:             factory,
		acceptedCallback:    acceptedCallback,
		errorCallback:       errorCallback,
		state:               internal.NewStateGuard(AcceptorStateStopped),
		started:             internal.NewSignal(),
		stopped:             internal.NewSignal(),
	}

	acceptor.listenAddrPort.Store(&listenAddrPort)
	return acceptor, nil
}

// Start begins accepting connections on the listen address.
//
// Returns:
//   - nil when the acceptor is running.
//   - [ErrAcceptorUninitialized] when the acceptor was not created with [NewAcceptor].
//   - [ErrAcceptorStateNotStopped] when the acceptor is not in the stopped state.
//   - The error from [net.ListenTCP] when binding fails; the acceptor stays stopped.
//
// Notes:
//   - Blocks until the accept goroutine is running, so [Acceptor.ListenAddrPort] returns
//     the bound address as soon as Start returns.
func (this *Acceptor) Start() error {
	if this.state == nil {
		return ErrAcceptorUninitialized
	}

	if err := this.state.Access(func(state *internal.StateGuardType) error {
		if *state != AcceptorStateStopped {
			return ErrAcceptorStateNotStopped
		}

		this.started = internal.NewSignal()
		this.stopped = internal.NewSignal()

		if this.listener != nil {
			this.listener.Close()
		}

		listener, listenError := net.ListenTCP("tcp", net.TCPAddrFromAddrPort(this.listenAddrPortParam))
		if listenError != nil {
			return listenError
		}

		this.listener = listener
		listenAddrPort := listener.Addr().(*net.TCPAddr).AddrPort()
		this.listenAddrPort.Store(&listenAddrPort)

		*state = AcceptorStateWaitRunning
		return nil
	}); err != nil {
		return err
	}

	go this.run()
	this.started.Wait()
	return nil
}

// run is the accept loop goroutine. It sets state to [AcceptorStateRunning], calls started.Signal(), and
// loops on listener.AcceptTCP() until [Acceptor.Stop] closes listener.
//
// Notes:
//   - Started as "go this.run()" by [Acceptor.Start].
//   - When state is not [AcceptorStateWaitRunning] on entry, [ErrAcceptorStateNotWaitRunning] is passed to
//     [Acceptor.reportError] and run returns without calling started.Signal or stopped.Signal.
//   - When AcceptTCP returns an error for which errors.Is(err, [net.ErrClosed]) is true, run returns.
//     Any other AcceptTCP error is passed to reportError and the loop continues.
//   - Each accepted *net.TCPConn is passed to [Acceptor.acceptedConnection] on this goroutine.
//   - stopped.Signal() runs on return through defer.
func (this *Acceptor) run() {
	if err := this.state.Access(func(state *internal.StateGuardType) error {
		if *state != AcceptorStateWaitRunning {
			return ErrAcceptorStateNotWaitRunning
		}

		*state = AcceptorStateRunning
		return nil
	}); err != nil {
		this.reportError(err)
		return
	}

	defer this.stopped.Signal()
	this.started.Signal()

	listenAddrPort := this.listener.Addr().(*net.TCPAddr).AddrPort()
	for {
		netTCPConn, err := this.listener.AcceptTCP()
		if errors.Is(err, net.ErrClosed) {
			return
		}

		if err != nil {

			this.reportError(err)
			continue
		}

		this.acceptedConnection(netTCPConn, listenAddrPort)
	}
}

// Stop stops accepting connections.
//
// Returns:
//   - nil when the listener was closed.
//   - [ErrAcceptorUninitialized] when the acceptor was not created with [NewAcceptor].
//   - [ErrAcceptorStateNotRunning] when the acceptor is not running.
//   - The error from closing the listener; the state still moves to WaitStopped.
//
// Notes:
//   - Returns without waiting for the accept goroutine. Call [Acceptor.WaitStopped] to wait
//     and return to the stopped state.
func (this *Acceptor) Stop() error {
	if this.state == nil {
		return ErrAcceptorUninitialized
	}

	return this.state.Access(func(state *internal.StateGuardType) error {
		if *state != AcceptorStateRunning {
			return ErrAcceptorStateNotRunning
		}

		err := this.listener.Close()
		*state = AcceptorStateWaitStopped
		return err
	})
}

// WaitStopped blocks until the accept goroutine has exited after [Acceptor.Stop], then
// moves the acceptor to the stopped state so it can be started again.
//
// Returns:
//   - nil when the acceptor is stopped.
//   - [ErrAcceptorUninitialized] when the acceptor was not created with [NewAcceptor].
//   - [ErrAcceptorStateNotWaitStopped] when [Acceptor.Stop] has not been called, or WaitStopped already returned.
func (this *Acceptor) WaitStopped() error {
	if this.state == nil {
		return ErrAcceptorUninitialized
	}

	if err := this.state.Access(func(state *internal.StateGuardType) error {
		if *state != AcceptorStateWaitStopped {
			return ErrAcceptorStateNotWaitStopped
		}
		return nil
	}); err != nil {
		return err
	}

	this.stopped.Wait()
	return this.state.Access(func(state *internal.StateGuardType) error {
		*state = AcceptorStateStopped
		return nil
	})
}

// ListenAddrPort returns the listen address.
//
// Returns:
//   - Before the first [Acceptor.Start], the address passed to [NewAcceptor].
//   - After a successful Start, the address actually bound, with port 0 replaced by the assigned port.
//   - A zero [netip.AddrPort] for a zero-value Acceptor.
//
// Notes:
//   - Safe to call from any goroutine without blocking.
func (this *Acceptor) ListenAddrPort() netip.AddrPort {
	listenAddrPortPtr := this.listenAddrPort.Load()
	if listenAddrPortPtr == nil {
		return netip.AddrPort{}
	}

	return *listenAddrPortPtr
}

// netTCPConnToFileDescriptor duplicates the socket of netTCPConn with fcntl(F_DUPFD_CLOEXEC) through
// netTCPConn.SyscallConn().Control, so the socket stays open after netTCPConn.Close.
//
// Parameters:
//   - netTCPConn is the connection returned by listener.AcceptTCP.
//
// Returns:
//   - The duplicated descriptor and nil.
//   - -1 and the SyscallConn error, or the Control error.
//   - -1 and os.NewSyscallError("fcntl", errno) when the fcntl call fails.
//
// Notes:
//   - The duplicate has FD_CLOEXEC set and shares the non-blocking mode the net package set on the socket.
func netTCPConnToFileDescriptor(netTCPConn *net.TCPConn) (FileDescriptor, error) {
	rawConn, rawError := netTCPConn.SyscallConn()
	if rawError != nil {
		return -1, rawError
	}

	var duplicated uintptr
	var duplicateErrno syscall.Errno
	if controlError := rawConn.Control(func(fd uintptr) {
		duplicated, _, duplicateErrno = syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_DUPFD_CLOEXEC, 0)
	}); controlError != nil {
		return -1, controlError
	}

	if duplicateErrno != 0 {
		return -1, os.NewSyscallError("fcntl", duplicateErrno)
	}

	return FileDescriptor(duplicated), nil
}

// acceptedConnection calls [netTCPConnToFileDescriptor], factory(fd, listenAddrPort), and
// acceptedCallback(handler) for one accepted connection.
//
// Parameters:
//   - netTCPConn is the connection from listener.AcceptTCP. netTCPConn.Close() runs on return through defer.
//   - listenAddrPort is the address bound by listener, passed to factory.
//
// Notes:
//   - On success fd belongs to the handler's [HandlerContext]; only netTCPConn is closed.
//   - When netTCPConnToFileDescriptor or factory returns an error, [Acceptor.rejectConnection](netTCPConn,
//     fd, err) closes fd and reports err.
//   - When factory returns a nil interface, rejectConnection is called with reason nil, so nothing is
//     reported unless closing fails. When it returns a typed nil pointer, detected with reflect,
//     rejectConnection is called with [ErrAcceptorInvalidHandler].
//   - When acceptedCallback is nil, [Acceptor.rejectAcceptedCallback] is called with
//     [ErrAcceptorNilAcceptedCallback]. When acceptedCallback returns an error, rejectAcceptedCallback is
//     called with that error.
func (this *Acceptor) acceptedConnection(netTCPConn *net.TCPConn, listenAddrPort netip.AddrPort) {
	defer func() {
		netTCPConn.Close()
	}()

	fd, err := netTCPConnToFileDescriptor(netTCPConn)
	if err != nil {
		this.rejectConnection(netTCPConn, fd, err)
		return
	}

	handler, err := this.factory(fd, listenAddrPort)

	if err != nil {
		this.rejectConnection(netTCPConn, fd, err)
		return
	}

	if handler == nil {
		this.rejectConnection(netTCPConn, fd, nil)
		return
	}

	value := reflect.ValueOf(handler)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		this.rejectConnection(netTCPConn, fd, ErrAcceptorInvalidHandler)
		return
	}

	if this.acceptedCallback == nil {
		this.rejectAcceptedCallback(netTCPConn, fd, handler, ErrAcceptorNilAcceptedCallback)
		return
	}

	if err := this.acceptedCallback(handler); err != nil {
		this.rejectAcceptedCallback(netTCPConn, fd, handler, err)
	}
}

// rejectAcceptedCallback drops a connection whose handler was created by factory but not accepted by
// acceptedCallback.
//
// Parameters:
//   - netTCPConn is the accepted connection.
//   - fd is the descriptor given to factory.
//   - handler is the handler returned by factory. Must not be nil.
//   - reason is the acceptedCallback error or [ErrAcceptorNilAcceptedCallback].
//
// Notes:
//   - When handler.context() is not nil and handler.context().fileDescriptor() >= 0, the context owns fd:
//     [HandlerContext.abortConnection] closes it with RST and errors.Join(reason, abort error) is passed to
//     [Acceptor.reportError].
//   - Otherwise [Acceptor.rejectConnection](netTCPConn, fd, reason) closes fd and netTCPConn.
func (this *Acceptor) rejectAcceptedCallback(netTCPConn *net.TCPConn, fd FileDescriptor, handler ClientHandler, reason error) {
	context := handler.context()
	if context == nil || context.fileDescriptor() < 0 {
		this.rejectConnection(netTCPConn, fd, reason)
		return
	}

	this.reportError(errors.Join(reason, context.abortConnection()))
}

// rejectConnection calls syscall.Close(fd) when fd >= 0, then netTCPConn.SetLinger(0) and netTCPConn.Close(),
// so the peer receives RST.
//
// Parameters:
//   - netTCPConn is the accepted connection.
//   - fd is the duplicated descriptor. Negative means [netTCPConnToFileDescriptor] failed and there is none.
//   - reason is the rejection cause. nil means nothing is reported unless a close fails.
//
// Notes:
//   - errors.Join(reason, SetLinger error, Close error, syscall.Close error) is passed to
//     [Acceptor.reportError], which drops it when every element is nil.
func (this *Acceptor) rejectConnection(netTCPConn *net.TCPConn, fd FileDescriptor, reason error) {
	var closeError error
	if fd >= 0 {
		closeError = os.NewSyscallError("close", syscall.Close(int(fd)))
	}

	this.reportError(errors.Join(reason, netTCPConn.SetLinger(0), netTCPConn.Close(), closeError))
}

// reportError calls errorCallback(err).
//
// Parameters:
//   - err is the AcceptTCP error or the rejection error.
//
// Notes:
//   - Returns without calling when err is nil or errorCallback is nil.
func (this *Acceptor) reportError(err error) {
	if this.errorCallback == nil || err == nil {
		return
	}

	this.errorCallback(err)
}
