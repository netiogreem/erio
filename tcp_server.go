package erio

import (
	"errors"
	"net/netip"
	"sync/atomic"

	"github.com/netiogreem/erio/internal"
)

// TCPServerError represents an error returned by TCPServer.
type TCPServerError string

// Error returns the error string.
//
// It returns the TCPServer error message.
func (this TCPServerError) Error() string {
	return string(this)
}

// Errors returned by TCPServer.
const (
	ErrTCPServerUninitialized      TCPServerError = "erio: tcp server is uninitialized"
	ErrTCPServerNilAcceptors       TCPServerError = "erio: tcp server acceptors is nil"
	ErrTCPServerEmptyReactors      TCPServerError = "erio: empty tcp server reactor list"
	ErrTCPServerNilReactor         TCPServerError = "erio: tcp server reactor is nil"
	ErrTCPServerDuplicateReactor   TCPServerError = "erio: duplicate tcp server reactor"
	ErrTCPServerStateNotInitOrStop TCPServerError = "erio: tcp server is not in the Init or Stop state"
	ErrTCPServerStateNotRun        TCPServerError = "erio: tcp server is not in the Run state"
)

// Lifecycle states of TCPServer.
// It transitions in the order Init -> WaitRun -> Run -> WaitStop -> Stop, and Start can be called
// again from Stop.
// If Start fails, it returns to Stop.
const (
	TCPServerStateInit     internal.LifecycleStateType = iota // State right after creation, which allows only Start.
	TCPServerStateStop                                        // Normally stopped state, which allows only Start.
	TCPServerStateWaitRun                                     // State in which Start is starting the Reactors and Acceptors.
	TCPServerStateRun                                         // State after Start succeeded, which allows only Stop.
	TCPServerStateWaitStop                                    // State in which Stop is stopping the Acceptors and Reactors.
)

// TCPServer manages the start and stop order of Acceptors and TCPReactors and the number of
// registered Handlers for the whole server.
// Start starts all Reactors and then starts accepting connections,
// and Stop first stops accepting connections and then stops the Reactors.
// Stop also calls Cleanup of the Reactors, so it releases the epoll and eventfd resources as well.
// It must be created with Builder or NewTCPServer, and Start and Stop on a zero value return
// ErrTCPServerUninitialized.
// Start, Stop, and HandlerCount can be called from multiple goroutines, but ListenAddrPorts must
// not be called concurrently with Start.
type TCPServer struct {
	acceptors      *Acceptors               // Acceptors responsible for accepting connections
	reactors       []*TCPReactor            // Copy of the Reactor pointer list received by the constructor
	lifecycleState *internal.LifecycleState // Lock that protects the lifecycle state of the server and its transitions
	handlerCounter atomic.Uint32            // Number of registered Handlers shared by all Reactors
}

// NewTCPServer validates the Acceptors and the Reactor list and creates a TCPServer in the Init
// state.
// It copies the Reactor pointer list, but the Reactor objects are shared with the caller.
// It does not check the Reactor states, or whether the Reactors that Acceptors assign connections
// to match this list.
//
//   - acceptors: Acceptors responsible for accepting connections
//   - reactors: list of Reactors whose start and stop are managed
//
// It returns ErrTCPServerNilAcceptors if acceptors is nil, ErrTCPServerEmptyReactors if the list is
// empty,
// ErrTCPServerNilReactor if it contains nil, and ErrTCPServerDuplicateReactor if it contains
// duplicate pointers.
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
		lifecycleState: internal.NewLifecycleState(TCPServerStateInit),
		handlerCounter: atomic.Uint32{},
	}, nil
}

// Start starts all Reactors with the shared Handler counter and then starts the Acceptors.
// It starts only in the Init or Stop state. When starting from the Stop state, it reinitializes all
// Reactors with Init.
// If everything succeeds, it transitions to Run, and if starting the Reactors or Acceptors fails,
// it stops and releases all Reactors with stopReactors and transitions to Stop.
//
// It returns ErrTCPServerUninitialized if not initialized, ErrTCPServerStateNotInitOrStop if not in
// the Init or Stop state,
// the error itself if Reactor initialization fails, and the start error joined with the state
// transition error if starting fails.
func (this *TCPServer) Start() error {
	if this.lifecycleState == nil {
		return ErrTCPServerUninitialized
	}

	if err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPServerStateInit && *state != TCPServerStateStop {
			return ErrTCPServerStateNotInitOrStop
		}

		if *state == TCPServerStateStop {
			for _, reactor := range this.reactors {
				if err := reactor.Init(); err != nil {
					return err
				}
			}
		}

		*state = TCPServerStateWaitRun
		return nil
	}); err != nil {
		return err
	}

	var startError error
	for _, reactor := range this.reactors {
		if err := reactor.StartWithCounter(&this.handlerCounter); err != nil {
			startError = err
			break
		}
	}

	if startError == nil {
		startError = this.acceptors.Start()
	}

	if startError != nil {
		this.stopReactors()
		stateError := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
			*state = TCPServerStateStop
			return nil
		})

		return errors.Join(startError, stateError)
	}

	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		*state = TCPServerStateRun
		return nil
	})
}

// Stop stops only in the Run state.
// After stopping accepting connections, it stops and releases all Reactors with stopReactors, and
// transitions to Stop regardless of the stop results.
//
// It returns ErrTCPServerUninitialized if not initialized and ErrTCPServerStateNotRun if not in the
// Run state,
// and otherwise returns nil regardless of the stop results of the Acceptors and Reactors.
func (this *TCPServer) Stop() error {
	if this.lifecycleState == nil {
		return ErrTCPServerUninitialized
	}

	if err := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != TCPServerStateRun {
			return ErrTCPServerStateNotRun
		}

		*state = TCPServerStateWaitStop
		return nil
	}); err != nil {
		return err
	}

	this.acceptors.Stop()
	this.stopReactors()

	this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		*state = TCPServerStateStop
		return nil
	})

	return nil
}

// HandlerCount returns the number of registered Handlers shared by all Reactors.
//
// It returns the number of registered Handlers for the whole server.
func (this *TCPServer) HandlerCount() uint32 {
	return this.handlerCounter.Load()
}

// ListenAddrPorts returns the list of listen addresses in registration order.
// After Start succeeds, addresses registered with port 0 are also returned with the actually
// assigned port.
//
// It returns the list of listen addresses.
func (this *TCPServer) ListenAddrPorts() []netip.AddrPort {
	return this.acceptors.ListenAddrPorts()
}

// stopReactors requests RequestStop on all Reactors, waits for WaitStop of all Reactors,
// and then calls Cleanup on all Reactors.
// It does not check the result of each call.
func (this *TCPServer) stopReactors() {
	for _, reactor := range this.reactors {
		reactor.RequestStop()
	}

	for _, reactor := range this.reactors {
		reactor.WaitStop()
	}

	for _, reactor := range this.reactors {
		reactor.Cleanup()
	}
}
