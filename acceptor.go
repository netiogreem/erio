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

// AcceptorError represents an error that Acceptor returns or passes to the error callback.
type AcceptorError string

// Error returns the error string.
//
// It returns the Acceptor error message.
func (this AcceptorError) Error() string {
	return string(this)
}

// Errors returned by Acceptor.
const (
	ErrAcceptorInvalidAddress      AcceptorError = "erio: invalid acceptor listen address"
	ErrAcceptorUninitialized       AcceptorError = "erio: acceptor is uninitialized"
	ErrAcceptorNilFactory          AcceptorError = "erio: acceptor factory is nil"
	ErrAcceptorNilAcceptedCallback AcceptorError = "erio: acceptor accepted callback is nil"
	ErrAcceptorInvalidHandler      AcceptorError = "erio: acceptor factory returned a typed nil handler"
	ErrAcceptorStateNotRun         AcceptorError = "erio: acceptor is not in the Run state"
	ErrAcceptorStateNotWaitRun     AcceptorError = "erio: acceptor is not in the WaitRun state"
	ErrAcceptorStateNotStop        AcceptorError = "erio: acceptor is not in the Stop state"
	ErrAcceptorStateNotWaitStop    AcceptorError = "erio: acceptor is not in the WaitStop state"
)

// Lifecycle states of Acceptor.
// It transitions in the order Stop -> WaitRun -> Run -> WaitStop -> Stop.
const (
	AcceptorStateStop     internal.LifecycleStateType = iota // State right after creation or after WaitStop completes, which allows only Start.
	AcceptorStateWaitRun                                     // State in which Start creates the listener and waits for the accept loop to start.
	AcceptorStateRun                                         // State in which the accept loop is running, which allows only RequestStop.
	AcceptorStateWaitStop                                    // State after RequestStop, waiting for WaitStop.
)

// Acceptor creates TCP connections on a single listen address.
// For a created connection, it calls the factory registered as a user function and receives the
// returned ClientHandler.
// It accepts connections, creates a ClientHandler for each connection, and requests registration.
// If Acceptor receives an error or nil from the factory function during the connection process, it
// releases the connection.
// Acceptor calls the registered user function errorCallback with its errors.
// It must be created with NewAcceptor, and Start, RequestStop, and WaitStop on a zero value return
// ErrAcceptorUninitialized.
type Acceptor struct {
	listener            *net.TCPListener               // TCP listener created in Start and closed in WaitStop
	listenAddrPortParam netip.AddrPort                 // Listen address received by NewAcceptor, bound on every Start
	listenAddrPort      atomic.Pointer[netip.AddrPort] // Listen address, updated to the actual bound address when Start succeeds
	factory             ClientHandlerFactory           // Function that creates a ClientHandler for each connection
	acceptedCallback    func(ClientHandler) error      // Function that requests registration of the created Handler
	errorCallback       func(error)                    // Callback that delivers accept, registration request, and rejection errors, nil allowed
	lifecycleState      *internal.LifecycleState       // Lock that protects the lifecycle state and its transitions
	startedSignal       *internal.Signal               // Signal that notifies the start of the accept loop
	stoppedSignal       *internal.Signal               // Signal that notifies the end of the accept loop
}

// NewAcceptor validates the listen address and factory and creates an Acceptor in the Stop state.
// It does not create the listener, and actual listening starts in Start.
// It does not check whether acceptedCallback and errorCallback are nil.
// If acceptedCallback is nil, accepted connections are rejected with
// ErrAcceptorNilAcceptedCallback.
//
//   - listenAddress: listen address in "IP:port" format, and if the port is 0, it is assigned
//     automatically in Start
//   - factory: function that creates a ClientHandler for each connection
//   - acceptedCallback: function that requests registration of the created Handler
//   - errorCallback: callback that receives accept, registration request, and rejection errors
//
// It returns ErrAcceptorInvalidAddress if the address format is invalid, and ErrAcceptorNilFactory
// if factory is nil.
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
		lifecycleState:      internal.NewLifecycleState(AcceptorStateStop),
		startedSignal:       internal.NewSignal(),
		stoppedSignal:       internal.NewSignal(),
	}

	acceptor.listenAddrPort.Store(&listenAddrPort)
	return acceptor, nil
}

// Start creates the listener, starts the accept loop, and then waits for the loop to start.
// It starts only in the Stop state and creates new start and stop signals.
// Every Start binds to the address received by NewAcceptor, so with port 0, a new port is assigned
// on every restart.
// Even for an Acceptor created with port 0, the actual port can be checked with ListenAddrPort
// after return.
// If creating the listener fails, it stays in the Stop state.
//
// It returns ErrAcceptorUninitialized if not initialized, ErrAcceptorStateNotStop if not in the
// Stop state, and the error itself if creating the listener fails.
func (this *Acceptor) Start() error {
	if this.lifecycleState == nil {
		return ErrAcceptorUninitialized
	}

	if err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != AcceptorStateStop {
			return ErrAcceptorStateNotStop
		}

		this.startedSignal = internal.NewSignal()
		this.stoppedSignal = internal.NewSignal()

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

		*state = AcceptorStateWaitRun
		return nil
	}); err != nil {
		return err
	}

	go this.run()
	this.startedSignal.Wait()
	return nil
}

// run is the accept loop that accepts connections and requests registration through
// acceptedConnection.
// It transitions from WaitRun to Run and then sends the start signal.
// Errors other than net.ErrClosed are passed to reportError, and the loop continues.
// When the listener is closed and it receives net.ErrClosed, it sends the stop signal and returns.
func (this *Acceptor) run() {
	if err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != AcceptorStateWaitRun {
			return ErrAcceptorStateNotWaitRun
		}

		*state = AcceptorStateRun
		return nil
	}); err != nil {
		this.reportError(err)
		return
	}

	defer this.stoppedSignal.Signal()
	this.startedSignal.Signal()

	listenAddrPort := this.listener.Addr().(*net.TCPAddr).AddrPort()
	for {
		netTCPConn, err := this.listener.AcceptTCP()
		if errors.Is(err, net.ErrClosed) {
			return
		}

		if err != nil {
			// AcceptTcp errors such as exceeding the FD limit are passed on, and it continues.
			this.reportError(err)
			continue
		}

		this.acceptedConnection(netTCPConn, listenAddrPort)
	}
}

// RequestStop transitions the Run state to WaitStop.
// It closes the listener to request that the accept loop end.
//
// It returns ErrAcceptorUninitialized if not initialized, ErrAcceptorStateNotRun if not in the Run
// state,
// and the listener close error if closing the listener fails.
func (this *Acceptor) RequestStop() error {
	if this.lifecycleState == nil {
		return ErrAcceptorUninitialized
	}

	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != AcceptorStateRun {
			return ErrAcceptorStateNotRun
		}

		err := this.listener.Close()
		*state = AcceptorStateWaitStop
		return err
	})
}

// WaitStop waits for termination in the WaitStop state.
//
// It returns ErrAcceptorUninitialized if not initialized, and ErrAcceptorStateNotWaitStop if not in
// the WaitStop state.
func (this *Acceptor) WaitStop() error {
	if this.lifecycleState == nil {
		return ErrAcceptorUninitialized
	}

	if err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != AcceptorStateWaitStop {
			return ErrAcceptorStateNotWaitStop
		}
		return nil
	}); err != nil {
		return err
	}

	this.stoppedSignal.Wait()
	err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		*state = AcceptorStateStop
		return nil
	})

	return err
}

// ListenAddrPort returns the stored listen address.
// After Start succeeds, it returns the actual bound address.
//
// It returns the listen address and port.
func (this *Acceptor) ListenAddrPort() netip.AddrPort {
	listenAddrPortPtr := this.listenAddrPort.Load()
	if listenAddrPortPtr == nil {
		return netip.AddrPort{}
	}

	return *listenAddrPortPtr
}

// netTCPConnToFileDescriptor duplicates the socket FD of the connection with F_DUPFD_CLOEXEC and
// returns it.
// The original connection and the duplicated FD must each be closed.
//
//   - netTCPConn: accepted TCP connection
//
// It returns the duplicated FD on success, and -1 and an error on failure.
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

// acceptedConnection duplicates the FD of the accepted connection, creates a Handler with factory,
// and calls acceptedCallback.
// It always closes the original connection when returning. The socket itself is not closed.
// FD duplication failure, factory errors, and nil or typed nil Handlers are rejected with
// rejectConnection.
// If acceptedCallback is nil or returns an error, it is rejected with rejectAcceptedCallback.
//
//   - netTCPConn: accepted TCP connection
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

// rejectAcceptedCallback handles the rejection.
// If there is a HandlerContext and the FD is valid, it closes the duplicated FD with the configured
// SO_LINGER value through abortConnection.
// If HandlerContext is nil or the FD is negative, it rejects with rejectConnection.
// The rejection reason and cleanup errors are passed to the error callback.
//
//   - netTCPConn: accepted original connection
//   - fd: duplicated FD
//   - handler: Handler to reject
//   - reason: rejection reason
func (this *Acceptor) rejectAcceptedCallback(netTCPConn *net.TCPConn, fd FileDescriptor, handler ClientHandler, reason error) {
	context := handler.context()
	if context == nil || context.fileDescriptor() < 0 {
		this.rejectConnection(netTCPConn, fd, reason)
		return
	}

	this.reportError(errors.Join(reason, context.abortConnection()))
}

// rejectConnection sets Linger of the original connection to 0 and closes it, and closes the
// duplicated FD if it is valid.
// The rejection reason and cleanup errors are passed to the error callback.
//
//   - netTCPConn: accepted original connection
//   - fd: duplicated FD, which is -1 and is not closed if duplication failed
//   - reason: rejection reason, nil allowed
func (this *Acceptor) rejectConnection(netTCPConn *net.TCPConn, fd FileDescriptor, reason error) {
	var closeError error
	if fd >= 0 {
		closeError = os.NewSyscallError("close", syscall.Close(int(fd)))
	}

	this.reportError(errors.Join(reason, netTCPConn.SetLinger(0), netTCPConn.Close(), closeError))
}

// reportError passes err to the error callback if the callback exists and err is not nil.
//
//   - err: error to pass
func (this *Acceptor) reportError(err error) {
	if this.errorCallback == nil || err == nil {
		return
	}

	this.errorCallback(err)
}
