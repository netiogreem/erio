package erio_test

import (
	"errors"
	"net"
	"net/netip"
	"slices"
	"syscall"
	"testing"

	"github.com/netiogreem/erio"
)

func newTCPServerTestReactor(test testing.TB) *erio.TCPReactor {
	test.Helper()
	reactor, createError := erio.NewTCPReactor(8, 8, 8, 64, func(reactorError error) {
		test.Errorf("reactor error=%v", reactorError)
	})
	if createError != nil {
		test.Fatal(createError)
	}

	return reactor
}

// Creates one Reactor and one Acceptor per listening address and combines them with NewTCPServer.
// A connection accepted on the i-th address is registered with the i-th Reactor, and Handler callbacks are recorded in the returned events.
func newTCPServerTest(test testing.TB, listenAddresses ...string) (*erio.TCPServer, *handlerContextFixture) {
	test.Helper()
	events := &handlerContextFixture{events: make(chan handlerContextTestEvent, 16)}
	factory := func(fd erio.FileDescriptor, listenAddress netip.AddrPort) (erio.ClientHandler, error) {
		context, createError := erio.NewHandlerContext(fd, listenAddress, 64, 8)
		if createError != nil {
			return nil, createError
		}

		return &handlerContextTestHandler{HandlerContext: context, events: events.events}, nil
	}

	// Registers the Reactor release first so that the server stop runs first.
	var reactors []*erio.TCPReactor
	test.Cleanup(func() {
		for _, reactor := range reactors {
			reactor.Cleanup()
		}
	})

	acceptors := make([]*erio.Acceptor, 0, len(listenAddresses))
	for _, listenAddress := range listenAddresses {
		reactor := newTCPServerTestReactor(test)
		reactors = append(reactors, reactor)
		acceptor, acceptorError := erio.NewAcceptor(listenAddress, factory, reactor.RegisterHandler, func(acceptError error) {
			test.Errorf("accept error=%v", acceptError)
		})
		if acceptorError != nil {
			test.Fatal(acceptorError)
		}

		acceptors = append(acceptors, acceptor)
	}

	group, groupError := erio.NewAcceptors(acceptors...)
	if groupError != nil {
		test.Fatal(groupError)
	}

	server, serverError := erio.NewTCPServer(group, reactors...)
	if serverError != nil {
		test.Fatal(serverError)
	}

	// Stop returns a state error immediately when the server is not in the Run state.
	test.Cleanup(func() { server.Stop() })
	return server, events
}

func TestNewTCPServerRejectsInvalidArguments(test *testing.T) {
	reactor := newTCPServerTestReactor(test)
	test.Cleanup(func() { reactor.Cleanup() })
	acceptor, acceptorError := erio.NewAcceptor("127.0.0.1:0", rejectBuilderTestConnection, nil, nil)
	if acceptorError != nil {
		test.Fatal(acceptorError)
	}

	acceptors, acceptorsError := erio.NewAcceptors(acceptor)
	if acceptorsError != nil {
		test.Fatal(acceptorsError)
	}

	for _, rejectCase := range []struct {
		name      string
		acceptors *erio.Acceptors
		reactors  []*erio.TCPReactor
		wantError error
	}{
		{name: "nil_acceptors", acceptors: nil, reactors: []*erio.TCPReactor{reactor}, wantError: erio.ErrTCPServerNilAcceptors},
		{name: "empty_reactors", acceptors: acceptors, reactors: nil, wantError: erio.ErrTCPServerEmptyReactors},
		{name: "nil_reactor", acceptors: acceptors, reactors: []*erio.TCPReactor{reactor, nil}, wantError: erio.ErrTCPServerNilReactor},
		{name: "duplicate_reactor", acceptors: acceptors, reactors: []*erio.TCPReactor{reactor, reactor}, wantError: erio.ErrTCPServerDuplicateReactor},
	} {
		server, createError := erio.NewTCPServer(rejectCase.acceptors, rejectCase.reactors...)
		if server != nil || !errors.Is(createError, rejectCase.wantError) {
			test.Errorf("%s: server=%v, error=%v, want %v", rejectCase.name, server, createError, rejectCase.wantError)
		}
	}
}

func TestTCPServerZeroValue(test *testing.T) {
	server := &erio.TCPServer{}
	if startError := server.Start(); !errors.Is(startError, erio.ErrTCPServerUninitialized) {
		test.Errorf("Start=%v, want %v", startError, erio.ErrTCPServerUninitialized)
	}

	if stopError := server.Stop(); !errors.Is(stopError, erio.ErrTCPServerUninitialized) {
		test.Errorf("Stop=%v, want %v", stopError, erio.ErrTCPServerUninitialized)
	}

	if count := server.HandlerCount(); count != 0 {
		test.Errorf("HandlerCount=%d, want 0", count)
	}
}

func TestTCPServerStateTransitions(test *testing.T) {
	server, events := newTCPServerTest(test, "127.0.0.1:0")

	// Init state
	if stopError := server.Stop(); !errors.Is(stopError, erio.ErrTCPServerStateNotRun) {
		test.Fatalf("Stop in Init=%v, want %v", stopError, erio.ErrTCPServerStateNotRun)
	}

	// Run state
	if startError := server.Start(); startError != nil {
		test.Fatal(startError)
	}

	if startError := server.Start(); !errors.Is(startError, erio.ErrTCPServerStateNotInitOrStop) {
		test.Fatalf("Start in Run=%v, want %v", startError, erio.ErrTCPServerStateNotInitOrStop)
	}

	// Stop state
	listenAddress := server.ListenAddrPorts()[0]
	if stopError := server.Stop(); stopError != nil {
		test.Fatal(stopError)
	}

	if stopError := server.Stop(); !errors.Is(stopError, erio.ErrTCPServerStateNotRun) {
		test.Fatalf("Stop in Stop=%v, want %v", stopError, erio.ErrTCPServerStateNotRun)
	}

	assertTestAcceptorRefused(test, listenAddress)

	// Starting again from the Stop state reinitializes the Reactor and accepts connections.
	if startError := server.Start(); startError != nil {
		test.Fatalf("Start in Stop=%v", startError)
	}

	dialTestAcceptor(test, server.ListenAddrPorts()[0])
	events.waitEvent(test, handlerContextTestOnConnect)
}

func TestTCPServerStartRollsBackOnAcceptorFailure(test *testing.T) {
	occupier, listenError := net.ListenTCP("tcp", net.TCPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if listenError != nil {
		test.Fatal(listenError)
	}

	test.Cleanup(func() {
		if closeError := occupier.Close(); closeError != nil && !errors.Is(closeError, net.ErrClosed) {
			test.Error(closeError)
		}
	})

	occupiedAddress := occupier.Addr().(*net.TCPAddr).AddrPort()
	server, events := newTCPServerTest(test, "127.0.0.1:0", occupiedAddress.String())
	if startError := server.Start(); !errors.Is(startError, syscall.EADDRINUSE) {
		test.Fatalf("Start with occupied address=%v, want %v", startError, syscall.EADDRINUSE)
	}

	// A failed Start rolls back the Acceptors that were started earlier and enters the Stop state.
	assertTestAcceptorRefused(test, server.ListenAddrPorts()[0])
	if stopError := server.Stop(); !errors.Is(stopError, erio.ErrTCPServerStateNotRun) {
		test.Fatalf("Stop after failed Start=%v, want %v", stopError, erio.ErrTCPServerStateNotRun)
	}

	if closeError := occupier.Close(); closeError != nil {
		test.Fatal(closeError)
	}

	// Reinitializes the released Reactors and accepts connections on all addresses.
	if startError := server.Start(); startError != nil {
		test.Fatalf("Start after release=%v", startError)
	}

	for _, listenAddress := range server.ListenAddrPorts() {
		dialTestAcceptor(test, listenAddress)
		events.waitEvent(test, handlerContextTestOnConnect)
	}
}

func TestTCPServerSharesHandlerCount(test *testing.T) {
	server, events := newTCPServerTest(test, "127.0.0.1:0", "127.0.0.2:0")
	wantBeforeStart := []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:0"), netip.MustParseAddrPort("127.0.0.2:0")}
	if listenAddresses := server.ListenAddrPorts(); !slices.Equal(listenAddresses, wantBeforeStart) {
		test.Fatalf("ListenAddrPorts before Start=%v, want %v", listenAddresses, wantBeforeStart)
	}

	if startError := server.Start(); startError != nil {
		test.Fatal(startError)
	}

	// Connection counts registered with different Reactors are summed across the whole server.
	var clients []*net.TCPConn
	for _, listenAddress := range server.ListenAddrPorts() {
		clients = append(clients, dialTestAcceptor(test, listenAddress))
		events.waitEvent(test, handlerContextTestOnConnect)
	}

	if count := server.HandlerCount(); count != 2 {
		test.Fatalf("HandlerCount=%d, want 2", count)
	}

	if closeError := clients[0].Close(); closeError != nil {
		test.Fatal(closeError)
	}

	events.waitEvent(test, handlerContextTestOnClose)
	if count := server.HandlerCount(); count != 1 {
		test.Fatalf("HandlerCount after client close=%d, want 1", count)
	}

	if stopError := server.Stop(); stopError != nil {
		test.Fatal(stopError)
	}

	if closed := events.waitEvent(test, handlerContextTestOnClose); !errors.Is(closed.eventError, erio.ErrTCPReactorStopped) {
		test.Fatalf("OnClose reason=%v, want %v", closed.eventError, erio.ErrTCPReactorStopped)
	}

	if count := server.HandlerCount(); count != 0 {
		test.Fatalf("HandlerCount after Stop=%d, want 0", count)
	}
}

// The least significant bit of each operation byte represents Start or Stop.
// Predicts the error returned by each call with a model of whether the server is running, and compares it.
func FuzzTCPServerLifecycle(fuzz *testing.F) {
	fuzz.Add([]byte{0, 1, 0, 1})
	fuzz.Add([]byte{1, 0, 0, 1, 1, 0})
	fuzz.Fuzz(func(test *testing.T, operations []byte) {
		server, _ := newTCPServerTest(test, "127.0.0.1:0")
		running := false
		for index, operation := range operations {
			var actualError, expectedError error
			if operation&1 == 0 {
				actualError = server.Start()
				if running {
					expectedError = erio.ErrTCPServerStateNotInitOrStop
				} else {
					running = true
				}
			} else {
				actualError = server.Stop()
				if running {
					running = false
				} else {
					expectedError = erio.ErrTCPServerStateNotRun
				}
			}

			if !errors.Is(actualError, expectedError) {
				test.Fatalf("op %d running=%v: error=%v, want %v", index, running, actualError, expectedError)
			}
		}
	})
}
