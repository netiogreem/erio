package erio

import (
	"errors"
	"net/netip"

	"github.com/netiogreem/erio/internal"
)

// AcceptorsError is the error type returned by [Acceptors] methods.
// It is a string so that each error can be declared as a comparable constant.
type AcceptorsError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this AcceptorsError) Error() string {
	return string(this)
}

const (
	ErrAcceptorsEmpty            AcceptorsError = "erio: empty acceptor list"                 // Returned by [NewAcceptors] when no acceptor is given.
	ErrAcceptorsNilAcceptor      AcceptorsError = "erio: acceptor is nil"                     // Returned by [NewAcceptors] when an acceptor is nil.
	ErrAcceptorsDuplicate        AcceptorsError = "erio: duplicate acceptor"                  // Returned by [NewAcceptors] when the same [Acceptor] is given more than once.
	ErrAcceptorsDuplicateAddress AcceptorsError = "erio: duplicate listen address"            // Returned by [NewAcceptors] when two acceptors have the same non-zero-port listen address.
	ErrAcceptorsUninitialized    AcceptorsError = "erio: acceptors are uninitialized"         // Returned when the group was not created with [NewAcceptors].
	ErrAcceptorsStateNotStop     AcceptorsError = "erio: acceptors are not in the Stop state" // Returned by [Acceptors.Start] when the group is not stopped.
	ErrAcceptorsStateNotRun      AcceptorsError = "erio: acceptors are not in the Run state"  // Returned by [Acceptors.Stop] when the group is not running.
)

const (
	AcceptorsStateStopped     internal.StateGuardType = iota // No acceptor is running. The initial state and the state after [Acceptors.Stop].
	AcceptorsStateWaitRunning                                // [Acceptors.Start] is starting the acceptors.
	AcceptorsStateRunning                                    // All acceptors are running.
	AcceptorsStateWaitStopped                                // [Acceptors.Stop] is stopping the acceptors.
)

// Acceptors starts and stops a group of [Acceptor] values together.
//
// Notes:
//   - Start and Stop are all-or-nothing: when one acceptor fails to start, the ones already
//     started are stopped again.
//   - Methods are safe to call from multiple goroutines.
type Acceptors struct {
	acceptors []*Acceptor          // Acceptors in the order given to NewAcceptors.
	state     *internal.StateGuard // Current lifecycle state; nil for a zero-value Acceptors.
}

// NewAcceptors creates an [Acceptors] group. The acceptors start listening at [Acceptors.Start].
//
// Parameters:
//   - acceptors are the acceptors to group, started and stopped in this order. At least one is
//     required, none may be nil or given twice, and no two may listen on the same address with a
//     non-zero port.
//
// Returns:
//   - The stopped group and nil.
//   - [ErrAcceptorsEmpty] when no acceptor is given.
//   - [ErrAcceptorsNilAcceptor] when an acceptor is nil.
//   - [ErrAcceptorsDuplicate] when the same acceptor is given more than once.
//   - [ErrAcceptorsDuplicateAddress] when two acceptors have the same listen address with a non-zero port.
//
// Notes:
//   - The slice is copied, so changing the caller's slice afterwards has no effect.
//   - Addresses are compared using [Acceptor.ListenAddrPort] at construction time.
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

		if address.Port() == 0 {
			continue
		}

		if _, exists := addresses[address]; exists {
			return nil, ErrAcceptorsDuplicateAddress
		}

		addresses[address] = struct{}{}
	}

	return &Acceptors{
		acceptors: append([]*Acceptor(nil), acceptors...),
		state:     internal.NewStateGuard(AcceptorsStateStopped),
	}, nil
}

// Start starts every acceptor in order.
//
// Returns:
//   - nil when all acceptors are running.
//   - [ErrAcceptorsUninitialized] when the group was not created with [NewAcceptors].
//   - [ErrAcceptorsStateNotStop] when the group is not stopped.
//   - The [Acceptor.Start] error joined with any errors from stopping the acceptors started
//     before it; the group returns to the stopped state.
func (this *Acceptors) Start() error {
	if err := this.beginTransition(AcceptorsStateStopped, AcceptorsStateWaitRunning, ErrAcceptorsStateNotStop); err != nil {
		return err
	}

	err := this.startAcceptors()
	return this.state.Access(func(state *internal.StateGuardType) error {
		if err != nil {
			*state = AcceptorsStateStopped
			return err
		}

		*state = AcceptorsStateRunning
		return nil
	})
}

// Stop stops every acceptor and waits until their accept goroutines have exited.
//
// Returns:
//   - nil when all acceptors are stopped.
//   - [ErrAcceptorsUninitialized] when the group was not created with [NewAcceptors].
//   - [ErrAcceptorsStateNotRun] when the group is not running.
//   - The joined errors from [Acceptor.Stop] and [Acceptor.WaitStopped]; the group still
//     moves to the stopped state.
func (this *Acceptors) Stop() error {
	if err := this.beginTransition(AcceptorsStateRunning, AcceptorsStateWaitStopped, ErrAcceptorsStateNotRun); err != nil {
		return err
	}

	stopError := stopAcceptors(this.acceptors)
	stateError := this.state.Access(func(state *internal.StateGuardType) error {
		*state = AcceptorsStateStopped
		return nil
	})

	return errors.Join(stopError, stateError)
}

// ListenAddrPorts returns the listen address of each acceptor.
//
// Returns:
//   - The addresses in the order the acceptors were given to [NewAcceptors]. After a successful
//     [Acceptors.Start] these are the bound addresses, with port 0 replaced by the assigned port.
//   - nil for a zero-value Acceptors.
func (this *Acceptors) ListenAddrPorts() (listenAddrPorts []netip.AddrPort) {
	if this.state == nil {
		return nil
	}

	for _, acceptor := range this.acceptors {
		listenAddrPorts = append(listenAddrPorts, acceptor.ListenAddrPort())
	}

	return listenAddrPorts
}

// beginTransition moves state from expected to next inside state.Access.
//
// Parameters:
//   - expected is the state the group must be in. [Acceptors.Start] passes [AcceptorsStateStopped],
//     [Acceptors.Stop] passes [AcceptorsStateRunning].
//   - next is the state stored when the current state equals expected.
//   - stateError is returned when the current state differs from expected.
//
// Returns:
//   - nil when state is now next.
//   - [ErrAcceptorsUninitialized] when state is nil.
//   - stateError when the current state differs from expected. state is unchanged.
func (this *Acceptors) beginTransition(expected internal.StateGuardType, next internal.StateGuardType, stateError error) error {
	if this.state == nil {
		return ErrAcceptorsUninitialized
	}

	return this.state.Access(func(state *internal.StateGuardType) error {
		if *state != expected {
			return stateError
		}

		*state = next
		return nil
	})
}

// startAcceptors calls [Acceptor.Start] on each element of acceptors in order and stops at the first error.
//
// Returns:
//   - nil when every Start returned nil.
//   - errors.Join(Start error, [stopAcceptors](acceptors[:index]) error) for the first acceptor whose Start
//     failed, where index is its position.
//
// Notes:
//   - After an error no acceptor of the group is running, because stopAcceptors stopped the ones before index.
func (this *Acceptors) startAcceptors() error {
	for index, acceptor := range this.acceptors {
		if err := acceptor.Start(); err != nil {
			return errors.Join(err, stopAcceptors(this.acceptors[:index]))
		}
	}

	return nil
}

// stopAcceptors calls [Acceptor.Stop] on every acceptor, then [Acceptor.WaitStopped] on those whose Stop
// returned nil.
//
// Parameters:
//   - acceptors are the acceptors to stop. May be empty.
//
// Returns:
//   - nil when every Stop and WaitStopped returned nil.
//   - errors.Join of every Stop and WaitStopped error otherwise.
//
// Notes:
//   - All Stop calls run before any WaitStopped, so the accept goroutines exit concurrently instead of
//     one after another.
//   - An acceptor whose Stop returned an error is not passed to WaitStopped.
func stopAcceptors(acceptors []*Acceptor) error {
	waiting := make([]*Acceptor, 0, len(acceptors))

	var stopErrors []error
	for _, acceptor := range acceptors {
		if err := acceptor.Stop(); err != nil {
			stopErrors = append(stopErrors, err)
			continue
		}

		waiting = append(waiting, acceptor)
	}

	for _, acceptor := range waiting {
		if err := acceptor.WaitStopped(); err != nil {
			stopErrors = append(stopErrors, err)
		}
	}

	return errors.Join(stopErrors...)
}
