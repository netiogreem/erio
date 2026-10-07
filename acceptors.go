package erio

import (
	"errors"
	"net/netip"

	"github.com/netiogreem/erio/internal"
)

// AcceptorsError represents an error returned by Acceptors.
type AcceptorsError string

// Error returns the error string.
//
// It returns the Acceptors error message.
func (this AcceptorsError) Error() string {
	return string(this)
}

// Errors returned by Acceptors.
const (
	ErrAcceptorsEmpty            AcceptorsError = "erio: empty acceptor list"
	ErrAcceptorsNilAcceptor      AcceptorsError = "erio: acceptor is nil"
	ErrAcceptorsDuplicate        AcceptorsError = "erio: duplicate acceptor"
	ErrAcceptorsDuplicateAddress AcceptorsError = "erio: duplicate listen address"
	ErrAcceptorsUninitialized    AcceptorsError = "erio: acceptors are uninitialized"
	ErrAcceptorsStateNotStop     AcceptorsError = "erio: acceptors are not in the Stop state"
	ErrAcceptorsStateNotRun      AcceptorsError = "erio: acceptors are not in the Run state"
)

// Lifecycle states of Acceptors.
// It transitions in the order Stop -> WaitRun -> Run -> WaitStop -> Stop,
// and if Start fails, it returns from WaitRun to Stop.
const (
	AcceptorsStateStop     internal.LifecycleStateType = iota // State right after creation, after a Start failure, or after Stop completes.
	AcceptorsStateWaitRun                                     // State in which Start is starting the Acceptors one by one.
	AcceptorsStateRun                                         // State in which all Acceptors have finished starting.
	AcceptorsStateWaitStop                                    // State in which Stop is stopping the Acceptors.
)

// Acceptors manages the start, stop, and listen address lookup of multiple Acceptors together.
// It must be created with NewAcceptors, and Start and Stop on a zero value return
// ErrAcceptorsUninitialized.
type Acceptors struct {
	acceptors      []*Acceptor              // Copy of the Acceptor pointer list received by the constructor
	lifecycleState *internal.LifecycleState // Lock that protects the lifecycle state of the group and its transitions
}

// NewAcceptors validates the Acceptor list and creates Acceptors in the Stop state.
// Duplicate listen addresses are checked with ListenAddrPort at creation time,
// and addresses with port 0 are excluded from the duplicate check because each is assigned a port
// when started.
//
//   - acceptors: list of Acceptors to group
//
// It returns ErrAcceptorsEmpty if the list is empty, ErrAcceptorsNilAcceptor if it contains nil,
// ErrAcceptorsDuplicate if it contains duplicate pointers, and ErrAcceptorsDuplicateAddress if it
// contains duplicate addresses.
func NewAcceptors(acceptors ...*Acceptor) (*Acceptors, error) {
	if len(acceptors) == 0 {
		return nil, ErrAcceptorsEmpty
	}

	seen := make(map[*Acceptor]struct{}, len(acceptors))
	for _, acceptor := range acceptors {
		if acceptor == nil {
			return nil, ErrAcceptorsNilAcceptor
		}

		if _, exists := seen[acceptor]; exists {
			return nil, ErrAcceptorsDuplicate
		}

		seen[acceptor] = struct{}{}
	}

	addresses := make(map[netip.AddrPort]struct{}, len(acceptors))
	for _, acceptor := range acceptors {
		address := acceptor.ListenAddrPort()
		// Port 0 is excluded from the address duplicate check because each is assigned its own port
		// at start.
		if address.Port() == 0 {
			continue
		}

		if _, exists := addresses[address]; exists {
			return nil, ErrAcceptorsDuplicateAddress
		}

		addresses[address] = struct{}{}
	}

	return &Acceptors{
		acceptors:      append([]*Acceptor(nil), acceptors...),
		lifecycleState: internal.NewLifecycleState(AcceptorsStateStop),
	}, nil
}

// Start starts all Acceptors in registration order.
// It starts only in the Stop state and transitions to Run when all have started.
// If any of them fails to start, it stops the Acceptors started before it and returns to Stop.
//
// It returns ErrAcceptorsUninitialized if not initialized, ErrAcceptorsStateNotStop if not in the
// Stop state,
// and the start error joined with the stop errors if starting fails.
func (this *Acceptors) Start() error {
	if err := this.beginTransition(AcceptorsStateStop, AcceptorsStateWaitRun, ErrAcceptorsStateNotStop); err != nil {
		return err
	}

	startError := this.startAcceptors()
	stateError := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if startError != nil {
			*state = AcceptorsStateStop
			return nil
		}

		*state = AcceptorsStateRun
		return nil
	})

	return errors.Join(startError, stateError)
}

// Stop stops all Acceptors and transitions to Stop.
// It stops only in the Run state,
// and first requests RequestStop on all Acceptors, then waits for WaitStop of the Acceptors whose
// request succeeded.
// Even if stopping an individual Acceptor fails, it continues stopping the rest, and the group
// transitions to Stop.
//
// It returns ErrAcceptorsUninitialized if not initialized, ErrAcceptorsStateNotRun if not in the
// Run state,
// and the individual stop errors joined together.
func (this *Acceptors) Stop() error {
	if err := this.beginTransition(AcceptorsStateRun, AcceptorsStateWaitStop, ErrAcceptorsStateNotRun); err != nil {
		return err
	}

	stopError := stopAcceptors(this.acceptors)
	stateError := this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		*state = AcceptorsStateStop
		return nil
	})

	return errors.Join(stopError, stateError)
}

// ListenAddrPorts collects and returns the listen address of each Acceptor in registration order.
// After Start succeeds, addresses registered with port 0 are also returned with the actually
// assigned port.
//
// It returns the list of listen addresses, and nil if not initialized.
func (this *Acceptors) ListenAddrPorts() (listenAddrPorts []netip.AddrPort) {
	if this.lifecycleState == nil {
		return nil
	}

	for _, acceptor := range this.acceptors {
		listenAddrPorts = append(listenAddrPorts, acceptor.ListenAddrPort())
	}

	return listenAddrPorts
}

// beginTransition transitions to next within the lock if the current state is expected.
//
//   - expected: state expected before the transition
//   - next: state to transition to
//   - stateError: error to return when the current state is not expected
//
// It returns ErrAcceptorsUninitialized if not initialized, and stateError if the state differs.
func (this *Acceptors) beginTransition(expected internal.LifecycleStateType, next internal.LifecycleStateType, stateError error) error {
	if this.lifecycleState == nil {
		return ErrAcceptorsUninitialized
	}

	return this.lifecycleState.Access(func(state *internal.LifecycleStateType) error {
		if *state != expected {
			return stateError
		}

		*state = next
		return nil
	})
}

// startAcceptors starts the Acceptors in registration order.
// It stops at the first Acceptor that fails and stops the Acceptors started before it with
// stopAcceptors.
//
// It returns the start error joined with the stop errors.
func (this *Acceptors) startAcceptors() error {
	for index, acceptor := range this.acceptors {
		if err := acceptor.Start(); err != nil {
			return errors.Join(err, stopAcceptors(this.acceptors[:index]))
		}
	}

	return nil
}

// stopAcceptors requests RequestStop on all Acceptors in the list, then waits for WaitStop of the
// Acceptors whose request succeeded.
// Even if an individual Acceptor fails, it continues processing the rest.
//
//   - acceptors: list of Acceptors to stop
//
// It returns the individual errors joined together.
func stopAcceptors(acceptors []*Acceptor) error {
	waiting := make([]*Acceptor, 0, len(acceptors))

	var stopErrors []error
	for _, acceptor := range acceptors {
		if err := acceptor.RequestStop(); err != nil {
			stopErrors = append(stopErrors, err)
			continue
		}

		waiting = append(waiting, acceptor)
	}

	for _, acceptor := range waiting {
		if err := acceptor.WaitStop(); err != nil {
			stopErrors = append(stopErrors, err)
		}
	}

	return errors.Join(stopErrors...)
}
