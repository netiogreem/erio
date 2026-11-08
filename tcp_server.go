package erio

import (
	"errors"
	"net/netip"
	"sync/atomic"

	"github.com/netiogreem/erio/internal"
)

// TCPServerError is the error type returned by [TCPServer] methods and [NewTCPServer].
// It is a string so that each error can be declared as a comparable constant.
type TCPServerError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this TCPServerError) Error() string {
	return string(this)
}

const (
	ErrTCPServerUninitialized    TCPServerError = "erio: tcp server is uninitialized"               // Returned by [TCPServer.Start] and [TCPServer.Stop] when the server was not created with [NewTCPServer].
	ErrTCPServerNilAcceptors     TCPServerError = "erio: tcp server acceptors is nil"               // Returned by [NewTCPServer] when acceptors is nil.
	ErrTCPServerEmptyReactors    TCPServerError = "erio: empty tcp server reactor list"             // Returned by [NewTCPServer] when no reactor is given.
	ErrTCPServerNilReactor       TCPServerError = "erio: tcp server reactor is nil"                 // Returned by [NewTCPServer] when a reactor is nil.
	ErrTCPServerDuplicateReactor TCPServerError = "erio: duplicate tcp server reactor"              // Returned by [NewTCPServer] when the same reactor is given more than once.
	ErrTCPServerStateNotStopped  TCPServerError = "erio: tcp server is not in the Not Stpped state" // Returned by [TCPServer.Start] when the server is not stopped.
	ErrTCPServerStateNotRunning  TCPServerError = "erio: tcp server is not in the Running state"    // Returned by [TCPServer.Stop] when the server is not running.
)

const (
	TCPServerStateStopped     internal.StateGuardType = iota // Nothing is running. The initial state, the state after [TCPServer.Stop], and the state after a failed [TCPServer.Start].
	TCPServerStateWaitRunning                                // [TCPServer.Start] is starting the reactors and acceptors.
	TCPServerStateRunning                                    // All reactors and acceptors are running.
	TCPServerStateWaitStopped                                // [TCPServer.Stop] is stopping the acceptors and reactors.
)

// TCPServer starts and stops a group of [TCPReactor] values and an [Acceptors] group in the right
// order, and counts the handlers registered across all its reactors.
//
// Notes:
//   - Start runs the reactors first and then the acceptors; Stop runs them in the reverse order.
//   - The server can be started again after [TCPServer.Stop] returns.
//   - Start, Stop, and HandlerCount are safe to call from multiple goroutines.
type TCPServer struct {
	acceptors      *Acceptors           // Acceptor group that hands accepted connections to the reactors.
	reactors       []*TCPReactor        // Reactors in the order given to NewTCPServer.
	state          *internal.StateGuard // Current lifecycle state; nil for a zero-value TCPServer.
	handlerCounter atomic.Uint32        // Number of registered handlers, shared by all reactors.
}

// NewTCPServer creates a [TCPServer]. The reactors and acceptors start at [TCPServer.Start].
//
// Parameters:
//   - acceptors is the acceptor group to start and stop with the server. Must not be nil.
//   - reactors are the reactors to start and stop with the server. At least one is required, and
//     none may be nil or given twice.
//
// Returns:
//   - The stopped server and nil.
//   - [ErrTCPServerNilAcceptors] when acceptors is nil.
//   - [ErrTCPServerEmptyReactors] when no reactor is given.
//   - [ErrTCPServerNilReactor] when a reactor is nil.
//   - [ErrTCPServerDuplicateReactor] when the same reactor is given more than once.
//
// Notes:
//   - The reactor slice is copied, so changing the caller's slice afterwards has no effect.
//   - It is not checked whether the acceptors register their connections with these reactors.
func NewTCPServer(acceptors *Acceptors, reactors ...*TCPReactor) (*TCPServer, error) {
	if acceptors == nil {
		return nil, ErrTCPServerNilAcceptors
	}

	if len(reactors) == 0 {
		return nil, ErrTCPServerEmptyReactors
	}

	seen := make(map[*TCPReactor]struct{}, len(reactors))
	for _, reactor := range reactors {
		if reactor == nil {
			return nil, ErrTCPServerNilReactor
		}

		if _, exists := seen[reactor]; exists {
			return nil, ErrTCPServerDuplicateReactor
		}

		seen[reactor] = struct{}{}
	}

	return &TCPServer{
		acceptors:      acceptors,
		reactors:       append([]*TCPReactor(nil), reactors...),
		state:          internal.NewStateGuard(TCPServerStateStopped),
		handlerCounter: atomic.Uint32{},
	}, nil
}

// Start starts every reactor with the shared handler counter, then starts the acceptors.
//
// Returns:
//   - nil when all reactors and acceptors are running.
//   - [ErrTCPServerUninitialized] when the server was not created with [NewTCPServer].
//   - [ErrTCPServerStateNotStopped] when the server is not stopped.
//   - The [TCPReactor.StartWithCounter] error of the first reactor that fails to start.
//   - The [Acceptors.Start] error when the reactors started but the acceptors did not.
//
// Notes:
//   - On a start error the reactors already started are stopped again and the server returns to
//     the stopped state, so Start can be retried.
//   - Returns after every acceptor is accepting connections.
func (this *TCPServer) Start() error {
	if this.state == nil {
		return ErrTCPServerUninitialized
	}

	if err := this.state.Access(func(state *internal.StateGuardType) error {
		if *state != TCPServerStateStopped {
			return ErrTCPServerStateNotStopped
		}

		*state = TCPServerStateWaitRunning
		return nil
	}); err != nil {
		return err
	}

	var errs error
	for _, reactor := range this.reactors {
		if err := reactor.StartWithCounter(&this.handlerCounter); err != nil {
			errs = errors.Join(errs, err)
			break
		}
	}

	if errs == nil {
		errs = errors.Join(errs, this.acceptors.Start())
	}

	if errs != nil {
		this.stopReactors()

		return this.state.Access(func(state *internal.StateGuardType) error {
			*state = TCPServerStateStopped
			return errs
		})
	}

	return this.state.Access(func(state *internal.StateGuardType) error {
		*state = TCPServerStateRunning
		return nil
	})
}

// Stop stops accepting connections, then stops every reactor and waits until they have released
// their epoll instances and eventfds.
//
// Returns:
//   - nil when the server is stopped.
//   - [ErrTCPServerUninitialized] when the server was not created with [NewTCPServer].
//   - [ErrTCPServerStateNotRunning] when the server is not running.
//
// Notes:
//   - Every registered handler receives [ClientHandler.OnClose] with [ErrTCPReactorStopped] before Stop returns.
//   - Errors from [Acceptors.Stop], [TCPReactor.Stop], and [TCPReactor.WaitStopped] are dropped; the
//     server moves to the stopped state regardless.
func (this *TCPServer) Stop() error {
	if this.state == nil {
		return ErrTCPServerUninitialized
	}

	if err := this.state.Access(func(state *internal.StateGuardType) error {
		if *state != TCPServerStateRunning {
			return ErrTCPServerStateNotRunning
		}

		*state = TCPServerStateWaitStopped
		return nil
	}); err != nil {
		return err
	}

	this.acceptors.Stop()
	this.stopReactors()

	this.state.Access(func(state *internal.StateGuardType) error {
		*state = TCPServerStateStopped
		return nil
	})

	return nil
}

// HandlerCount returns the number of handlers registered across all reactors of the server.
//
// Returns:
//   - The shared handler count. 0 before the first [TCPServer.Start] and after every handler is closed.
func (this *TCPServer) HandlerCount() uint32 {
	return this.handlerCounter.Load()
}

// ListenAddrPorts returns the listen address of each acceptor.
//
// Returns:
//   - The addresses in the order the acceptors were given to [NewAcceptors]. After a successful
//     [TCPServer.Start] these are the bound addresses, with port 0 replaced by the assigned port.
//
// Caution:
//   - Panics on a zero-value TCPServer because acceptors is nil.
func (this *TCPServer) ListenAddrPorts() []netip.AddrPort {
	return this.acceptors.ListenAddrPorts()
}

// stopReactors calls [TCPReactor.Stop] on every element of reactors, then [TCPReactor.WaitStopped] on every element.
//
// Notes:
//   - All Stop calls run before any WaitStopped, so the event loops shut down concurrently.
//   - The return values of both calls are discarded. A reactor that was never started returns
//     [ErrTCPReactorStateNotRunning] from Stop and [ErrTCPReactorAlreadyStopped] from WaitStopped, and
//     that is how a failed [TCPServer.Start] skips the reactors it did not reach.
func (this *TCPServer) stopReactors() {
	for _, reactor := range this.reactors {
		reactor.Stop()
	}

	for _, reactor := range this.reactors {
		reactor.WaitStopped()
	}
}
