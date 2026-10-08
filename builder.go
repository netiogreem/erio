package erio

import (
	"errors"
	"runtime"
	"sync/atomic"
)

// TCPServerBuilderError represents an error returned by Builder.
type TCPServerBuilderError string

// Error returns the error string.
//
// Returns the Builder error message.
func (this TCPServerBuilderError) Error() string {
	return string(this)
}

// Configuration errors returned by Build.
const (
	ErrBuilderInvalidEventBatchSize       TCPServerBuilderError = "erio: tcp reactor event batch size must be positive"
	ErrBuilderInvalidRegisterCommandQuota TCPServerBuilderError = "erio: tcp reactor command quota must be positive"
	ErrBuilderInvalidCommandReserveSize   TCPServerBuilderError = "erio: tcp reactor command reserve size must be positive"
	ErrBuilderInvalidReadBufferSize       TCPServerBuilderError = "erio: tcp reactor read buffer size must be positive"
)

// ReactorParam holds the settings for the TCPReactors that Builder will create.
// The same settings apply to all Reactors.
// Only Count uses a default value when set to 0;
// Build returns an error if any other numeric field is 0.
type ReactorParam struct {
	Count                uint32      // Number of Reactors; 0 defaults to GOMAXPROCS-1 (at least 1).
	EventBatchSize       uint32      // Maximum number of events received per epoll wait for each Reactor.
	CommandReserveSize   uint32      // Preallocates memory for each Reactor's command array. Double buffering uses twice the reserved size to minimize locking.
	RegisterCommandQuota uint32      // The maximum number of RegisterHandler commands each Reactor can accept at a time. Capacity becomes available again as commands are processed internally.
	ReadBufferSize       uint32      // Size in bytes of the receive buffer of each Reactor.
	ErrorCallback        func(error) // Reactor error callback; nil disables notifications.
}

// Builder stores the TCPServer configuration and creates TCPReactor, Acceptor, Acceptors, and TCPServer instances during Build.
type Builder struct {
	reactorParam        ReactorParam         // Reactor settings specified by WithReactor.
	listenAddresses     []string             // Listening addresses collected in the order of WithListenAddress calls.
	acceptErrorCallback func(error)          // Acceptor error callback.
	factory             ClientHandlerFactory // Function that creates a ClientHandler for each accepted connection.
}

// NewBuilder creates a Builder with an empty configuration.
// Listening addresses and ClientHandlerFactory should be set separately.
//
// Returns the created Builder.
func NewBuilder() *Builder {
	return &Builder{
		reactorParam:        ReactorParam{},
		listenAddresses:     nil,
		acceptErrorCallback: nil,
		factory:             nil,
	}
}

// WithReactor stores the Reactor settings.
// Calling it again replaces the previous settings.
// Build applies the default value for Count and validates the values.
//
//   - param: Reactor settings
//
// Returns the Builder itself for method chaining.
func (this *Builder) WithReactor(param ReactorParam) *Builder {
	this.reactorParam = param
	return this
}

// WithListenAddress adds a listening address.
// Repeated calls register all addresses in call order;
// Build resolves the addresses.
//
//   - listenAddress: Listening address in "IP:port" format; if the port is 0, a port is assigned automatically during Start
//
// Returns the Builder itself for method chaining.
func (this *Builder) WithListenAddress(listenAddress string) *Builder {
	this.listenAddresses = append(this.listenAddresses, listenAddress)
	return this
}

// WithAcceptErrorCallback sets the callback for errors that occur while accepting
// connections, creating Handlers, or requesting Reactor registration.
// The callback is invoked in each Acceptor's accept loop goroutine.
//
//   - acceptErrorCallback: Error callback; nil disables notifications
//
// Returns the Builder itself for method chaining.
func (this *Builder) WithAcceptErrorCallback(acceptErrorCallback func(error)) *Builder {
	this.acceptErrorCallback = acceptErrorCallback
	return this
}

// WithClientHandlerFactory sets the function that creates a ClientHandler for each accepted connection.
//
//   - factory: Function that creates a ClientHandler
//
// Returns the Builder itself for method chaining.
func (this *Builder) WithClientHandlerFactory(factory ClientHandlerFactory) *Builder {
	this.factory = factory
	return this
}

// Build validates the configuration and creates instances in the order Reactor -> Acceptors -> TCPServer.
// It creates the Reactor's epoll and eventfd resources but does not start the server;
// TCPServer.Start creates the listeners.
// If it fails after creating Reactors, it calls Cleanup on all created Reactors and returns the error combined with any cleanup errors.
//
// Returns a TCPServer in the Init state on success, or an error on failure.
// Returns ErrAcceptorsEmpty if there are no listening addresses, or ErrAcceptorNilFactory if factory is nil.
func (this *Builder) Build() (server *TCPServer, err error) {
	param, err := this.validateParam()
	if err != nil {
		return nil, err
	}

	reactors, err := this.buildReactors(param)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err == nil {
			return
		}

		for _, reactor := range reactors {
			if cleanupErr := reactor.Cleanup(); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()

	acceptors, err := this.buildAcceptors(reactors)
	if err != nil {
		return nil, err
	}

	return NewTCPServer(acceptors, reactors...)
}

// validateParam validates and returns a copy of reactorParam.
// If Count is 0, it sets Count to GOMAXPROCS-1 (at least 1).
// It does not modify the stored reactorParam.
//
// If EventBatchSize, RegisterCommandQuota, CommandReserveSize, or ReadBufferSize is 0, it returns an empty ReactorParam
// and the corresponding error: ErrBuilderInvalidEventBatchSize, ErrBuilderInvalidRegisterCommandQuota,
// ErrBuilderInvalidCommandReserveSize, or ErrBuilderInvalidReadBufferSize.
func (this *Builder) validateParam() (ReactorParam, error) {
	param := this.reactorParam
	if param.Count == 0 {
		if param.Count = uint32(max(runtime.GOMAXPROCS(0)-1, 1)); param.Count == 0 {
			param.Count = 1
		}
	}

	if param.EventBatchSize == 0 {
		return ReactorParam{}, ErrBuilderInvalidEventBatchSize
	}

	if param.RegisterCommandQuota == 0 {
		return ReactorParam{}, ErrBuilderInvalidRegisterCommandQuota
	}

	if param.CommandReserveSize == 0 {
		return ReactorParam{}, ErrBuilderInvalidCommandReserveSize
	}

	if param.ReadBufferSize == 0 {
		return ReactorParam{}, ErrBuilderInvalidReadBufferSize
	}

	return param, nil
}

// buildReactors creates param.Count TCPReactors.
// If creation fails partway through, it calls Cleanup on all previously created Reactors.
//
//   - param: Validated Reactor settings
//
// Returns the list of created Reactors and, if creation fails, the creation and cleanup errors.
func (this *Builder) buildReactors(param ReactorParam) (retReactors []*TCPReactor, err error) {
	reactors := make([]*TCPReactor, 0)

	defer func() {
		if err == nil {
			return
		}

		for _, reactor := range reactors {
			if cleanupErr := reactor.Cleanup(); cleanupErr != nil {
				err = errors.Join(err, cleanupErr)
			}
		}
	}()

	for range param.Count {
		reactor, err := NewTCPReactor(
			param.EventBatchSize,
			param.CommandReserveSize,
			param.RegisterCommandQuota,
			param.ReadBufferSize,
			param.ErrorCallback,
		)

		if err != nil {
			return reactors, err
		}

		reactors = append(reactors, reactor)
	}

	return reactors, nil
}

// buildAcceptors creates an Acceptor for each listening address and groups them into Acceptors.
// All Acceptors share a single sequence counter to assign accepted connections to Reactors in round-robin order;
// a connection is not reassigned to another Reactor if registration with the assigned Reactor fails.
//
//   - reactors: List of Reactors to assign connections to
//
// Returns the created Acceptors, or an address error, factory error, or Acceptors creation error.
func (this *Builder) buildAcceptors(reactors []*TCPReactor) (*Acceptors, error) {
	nextReactor := atomic.Uint32{}
	nextReactor.Store(0)
	acceptedCallback := func(handler ClientHandler) error {
		// Assign requests from multiple accept loops using a shared sequence counter and return failures to the caller without reassignment.
		index := (nextReactor.Add(1) - 1) % uint32(len(reactors))
		return reactors[index].RegisterHandler(handler)
	}

	acceptors := make([]*Acceptor, 0, len(this.listenAddresses))
	for _, address := range this.listenAddresses {
		acceptor, err := NewAcceptor(address, this.factory, acceptedCallback, this.acceptErrorCallback)
		if err != nil {
			return nil, err
		}

		acceptors = append(acceptors, acceptor)
	}

	return NewAcceptors(acceptors...)
}
