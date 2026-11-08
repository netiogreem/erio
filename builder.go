package erio

import (
	"runtime"
	"sync/atomic"
)

// TCPServerBuilderError is the error type returned by [Builder.Build].
// It is a string so that each error can be declared as a comparable constant.
type TCPServerBuilderError string

// Error returns the error message.
//
// Returns:
//   - The message text of the error constant.
func (this TCPServerBuilderError) Error() string {
	return string(this)
}

const (
	ErrBuilderInvalidEventBatchSize       TCPServerBuilderError = "erio: tcp reactor event batch size must be positive"     // Returned by [Builder.Build] when [ReactorParam.EventBatchSize] is 0.
	ErrBuilderInvalidRegisterCommandQuota TCPServerBuilderError = "erio: tcp reactor command quota must be positive"        // Returned by [Builder.Build] when [ReactorParam.RegisterCommandQuota] is 0.
	ErrBuilderInvalidCommandReserveSize   TCPServerBuilderError = "erio: tcp reactor command reserve size must be positive" // Returned by [Builder.Build] when [ReactorParam.CommandReserveSize] is 0.
	ErrBuilderInvalidReadBufferSize       TCPServerBuilderError = "erio: tcp reactor read buffer size must be positive"     // Returned by [Builder.Build] when [ReactorParam.ReadBufferSize] is 0.
)

// ReactorParam holds the settings [Builder.Build] passes to [NewTCPReactor] for every reactor of the server.
type ReactorParam struct {
	Count                uint32      // Number of reactors. 0 uses GOMAXPROCS - 1, at least 1.
	EventBatchSize       uint32      // Maximum epoll events handled per wait. Must be positive.
	CommandReserveSize   uint32      // Initial capacity of each reactor's command queue. Must be positive.
	RegisterCommandQuota uint32      // Maximum register commands queued per event loop iteration. Must be positive.
	ReadBufferSize       uint32      // Size of the read buffer shared by the handlers of one reactor. Must be positive.
	ErrorCallback        func(error) // Receives reactor errors not tied to a handler callback. nil drops them.
}

// Builder collects the reactor settings, listen addresses, and callbacks of a [TCPServer] and
// creates the server with [Builder.Build].
type Builder struct {
	reactorParam        ReactorParam         // Settings for every reactor.
	listenAddresses     []string             // "IP:port" literals, one acceptor each, in the order added.
	acceptErrorCallback func(error)          // Receives accept and rejection errors from every acceptor. nil drops them.
	factory             ClientHandlerFactory // Creates the handler for each accepted connection.
}

// NewBuilder creates an empty [Builder].
//
// Returns:
//   - A builder with no listen address, no factory, and zero reactor settings.
func NewBuilder() *Builder {
	return &Builder{
		reactorParam:        ReactorParam{},
		listenAddresses:     nil,
		acceptErrorCallback: nil,
		factory:             nil,
	}
}

// WithReactor sets the reactor settings.
//
// Parameters:
//   - param replaces the settings of a previous call. It is validated at [Builder.Build].
//
// Returns:
//   - The builder, for chaining.
func (this *Builder) WithReactor(param ReactorParam) *Builder {
	this.reactorParam = param
	return this
}

// WithListenAddress adds a listen address. Each call adds one acceptor.
//
// Parameters:
//   - listenAddress is an "IP:port" literal as accepted by [NewAcceptor]. Port 0 picks a free port
//     when the server starts.
//
// Returns:
//   - The builder, for chaining.
func (this *Builder) WithListenAddress(listenAddress string) *Builder {
	this.listenAddresses = append(this.listenAddresses, listenAddress)
	return this
}

// WithAcceptErrorCallback sets the callback that receives accept and rejection errors from every acceptor.
//
// Parameters:
//   - acceptErrorCallback replaces the callback of a previous call. nil drops the errors.
//
// Returns:
//   - The builder, for chaining.
func (this *Builder) WithAcceptErrorCallback(acceptErrorCallback func(error)) *Builder {
	this.acceptErrorCallback = acceptErrorCallback
	return this
}

// WithClientHandlerFactory sets the factory that creates a handler for each accepted connection.
//
// Parameters:
//   - factory replaces the factory of a previous call. Must not be nil at [Builder.Build].
//
// Returns:
//   - The builder, for chaining.
func (this *Builder) WithClientHandlerFactory(factory ClientHandlerFactory) *Builder {
	this.factory = factory
	return this
}

// Build creates the reactors and acceptors from the collected settings and returns a [TCPServer] that owns them.
//
// Returns:
//   - The server and nil when every setting is valid.
//   - [ErrBuilderInvalidEventBatchSize], [ErrBuilderInvalidRegisterCommandQuota],
//     [ErrBuilderInvalidCommandReserveSize], or [ErrBuilderInvalidReadBufferSize] when the matching
//     [ReactorParam] field is 0.
//   - [ErrAcceptorInvalidAddress] when a listen address is not an "IP:port" literal.
//   - [ErrAcceptorNilFactory] when no factory was set.
//   - [ErrAcceptorsEmpty] when no listen address was added.
//   - [ErrAcceptorsDuplicateAddress] when two listen addresses with a non-zero port are equal.
//   - The [NewTCPServer] error.
//
// Notes:
//   - Accepted connections are registered with the reactors in round-robin order.
//   - The other [ReactorParam] limits are checked when the server starts the reactors, not here.
func (this *Builder) Build() (server *TCPServer, err error) {
	param, err := this.validateParam()
	if err != nil {
		return nil, err
	}

	reactors := this.buildReactors(param)

	acceptors, err := this.buildAcceptors(reactors)
	if err != nil {
		return nil, err
	}

	return NewTCPServer(acceptors, reactors...)
}

// validateParam copies reactorParam, fills in Count when it is 0, and checks the other fields for 0.
//
// Returns:
//   - The copy with Count filled in, and nil.
//   - An empty [ReactorParam] and [ErrBuilderInvalidEventBatchSize], [ErrBuilderInvalidRegisterCommandQuota],
//     [ErrBuilderInvalidCommandReserveSize], or [ErrBuilderInvalidReadBufferSize] when the matching field is 0.
//
// Notes:
//   - Count 0 becomes uint32(max(runtime.GOMAXPROCS(0)-1, 1)).
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

// buildReactors calls [NewTCPReactor] param.Count times with param.EventBatchSize, param.CommandReserveSize,
// param.RegisterCommandQuota, param.ReadBufferSize, and param.ErrorCallback.
//
// Parameters:
//   - param is the [ReactorParam] returned by [Builder.validateParam].
//
// Returns:
//   - The reactors in creation order.
func (this *Builder) buildReactors(param ReactorParam) []*TCPReactor {
	reactors := make([]*TCPReactor, 0)

	for range param.Count {
		reactor := NewTCPReactor(
			param.EventBatchSize,
			param.CommandReserveSize,
			param.RegisterCommandQuota,
			param.ReadBufferSize,
			param.ErrorCallback,
		)

		reactors = append(reactors, reactor)
	}

	return reactors
}

// buildAcceptors calls [NewAcceptor] for each element of listenAddresses and passes the result to [NewAcceptors].
//
// Parameters:
//   - reactors are the reactors from [Builder.buildReactors]. Must not be empty.
//
// Returns:
//   - The [Acceptors] and nil.
//   - The NewAcceptor error for the first address that fails.
//   - The NewAcceptors error, including [ErrAcceptorsEmpty] when listenAddresses is empty.
//
// Notes:
//   - Every acceptor gets the same acceptedCallback, which computes
//     index := (nextReactor.Add(1) - 1) % uint32(len(reactors)) on one shared atomic.Uint32 and calls
//     reactors[index].RegisterHandler(handler). This is the round-robin distribution.
//   - factory and acceptErrorCallback are passed to NewAcceptor as given to the builder.
func (this *Builder) buildAcceptors(reactors []*TCPReactor) (*Acceptors, error) {
	nextReactor := atomic.Uint32{}
	nextReactor.Store(0)
	acceptedCallback := func(handler ClientHandler) error {
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
