# erio

Epoll Reactor-based TCP I/O framework

## Introduction

erio is a framework designed for the **invariance of session connection state** and the **serialized processing of session events, guaranteeing** that each session is always processed on the same goroutine.

It implements the Reactor pattern with Linux epoll for I/O multiplexing and runs in an event-driven manner without creating a goroutine per connection.

erio is a good fit for the following use cases:

* When you need to maintain sessions and want to modify a session's data from within that session without locks (e.g., sync.Mutex).
* When you need independent per-session timers (e.g., idle connection detection, authentication timeouts) without locks or complex code.
* When you need safe asynchronous event delivery from external goroutines to the session-processing goroutine (via PostUserEvent).

## Requirements

- **Linux**: Uses epoll and eventfd.
- **Go 1.26.0 or later**: The version currently specified in go.mod.
- **External dependency**: Uses github.com/emirpasic/gods v1.18.1 for timer management, managed as a Go module.

## Key Features

* **Serialized processing of session events**
  * Connection, receive, write/write-completion, multi-timer, and close events are processed serially on the same Reactor goroutine, so event callbacks of the same session never run concurrently.
* **Multiple listen addresses and connection distribution**
  * You can register multiple addresses and ports, and assign accepted connections to multiple Reactors in round-robin order.
* **Multi-session handling without per-connection goroutines**
  * Instead of using send/receive goroutines per connection, each Reactor receives events through epoll, identifies the ClientHandler of the connection where the event occurred, and processes it.
* **Write completion timing**
  * On the same goroutine that handles session events, you can use OnWritten(streamID) to confirm that data submitted through Write has been fully handed off to the kernel, then continue with the next write.
* **Multiple timers per session**
  * On the same goroutine that handles session events, you can use timer keys with SetTimeout(timerKey, timeout) and OnTimeout(timerKey) to manage response wait times, idle-connection timeouts, and authentication time limits separately.
* **Session control from other goroutines**
  * HandlerContext's Write, SetTimeout, UnsetTimeout, Close, AbortiveClose, and PostUserEvent can be called from other goroutines without any extra work (such as synchronization).
* **Asynchronous delivery of user data to a target session**
  * User-defined data can be sent asynchronously from another goroutine to any session and received in that session on the goroutine the session runs on.

## Benchmarks

Both erio and gnet use their public APIs to send one response per received packet (one 'Write()' call per received packet).

* AMD Ryzen™ 9 7945HX (32 logical cores), CPU clock 2.85GHz to 3.7GHz
* 6 cores (1 Acceptor, 5 Reactors) / Clients: 25 separate processes

### 8-byte response

After receiving data from a client, the test server sends an 8-byte acknowledgment for each packet.

#### erio

| Client<br />message <br />size | Pipelined<br />requests<br /> per client |       **TPS** | Avg TPS<br /> per Reactor | Receive<br /> throughput | Messages<br /> processed | Elapsed<br /> time |         CPU / RSS |
| -----------------------------: | ---------------------------------------: | ------------------: | ------------------------: | -----------------------: | -----------------------: | -----------------: | -----------------: |
|                      300 bytes |                                      100 | **1,980,159** |         **396,032** |              594.05 MB/s |               59,409,288 |           30.002 s | 499.9% / 14.27 MiB |
|                      300 bytes |                                       10 | **1,129,571** |         **225,914** |              338.87 MB/s |               33,889,523 |           30.002 s | 501.7% / 14.04 MiB |
|                      300 bytes |                                        1 |   **313,880** |          **62,776** |               94.16 MB/s |                9,416,825 |           30.001 s | 501.7% / 14.10 MiB |
|                    1,000 bytes |                                      100 | **1,959,974** |         **391,995** |            1,959.97 MB/s |               58,800,927 |           30.001 s | 499.9% / 15.25 MiB |
|                    1,000 bytes |                                       10 | **1,120,103** |         **224,021** |            1,120.10 MB/s |               33,603,945 |           30.001 s | 501.8% / 13.92 MiB |
|                    1,000 bytes |                                        1 |   **313,613** |          **62,723** |              313.61 MB/s |                9,408,830 |           30.001 s | 501.8% / 13.14 MiB |

> Send, receive, and response-check counts all matched, with 0 errors. CPU usage counts one logical CPU as 100%.

#### gnet v2.10.0

| Client<br />message <br />size | Pipelined<br /> requests<br /> per client |     **TPS** | Avg TPS<br /> per Reactor | Receive<br /> throughput | Messages<br /> processed | Elapsed<br /> time |         CPU / RSS |
| -----------------------------: | ----------------------------------------: | ----------------: | ------------------------: | -----------------------: | -----------------------: | -----------------: | -----------------: |
|                    1,000 bytes |                                       100 | **530,792** |         **106,158** |              530.79 MB/s |               15,926,220 |           30.005 s | 502.5% / 11.50 MiB |
|                    1,000 bytes |                                         1 | **311,798** |          **62,360** |              311.80 MB/s |                9,354,398 |           30.001 s | 503.4% / 14.56 MiB |

### 65536-byte response

After receiving data from a client, the test server sends a 65536-byte acknowledgment for each packet.

#### erio

| Client<br />message <br />size | Pipelined<br />requests<br /> per client |     **TPS** | Avg TPS<br /> per Reactor | Receive<br /> throughput | Messages<br /> processed | Elapsed<br /> time |           CPU / RSS |
| -----------------------------: | ---------------------------------------: | ----------------: | ------------------------: | -----------------------: | -----------------------: | -----------------: | ------------------: |
|                    1,000 bytes |                                      100 | **123,300** |          **24,660** |              123.30 MB/s |                3,701,594 |           30.021 s | 501.9% / 266.00 MiB |
|                    1,000 bytes |                                        1 | **143,270** |          **28,654** |              143.27 MB/s |                4,298,296 |           30.001 s |  501.7% / 21.25 MiB |

> The high RSS(266.00MiB) at 1,000 bytes with 100 pipelined requests is because the per-session write buffer was set to 65536*100 instead of making use of OnWritten.

#### gnet v2.10.0

| Client<br />message <br />size | Pipelined<br /> requests<br /> per client |     **TPS** | Avg TPS<br /> per Reactor | Receive<br /> throughput | Messages<br /> processed | Elapsed<br /> time |          CPU / RSS |
| -----------------------------: | ----------------------------------------: | ----------------: | ------------------------: | -----------------------: | -----------------------: | -----------------: | -----------------: |
|                    1,000 bytes |                                       100 | **149,553** |          **29,911** |              149.55 MB/s |                4,489,290 |           30.018 s | 509.1% / 16.25 MiB |
|                    1,000 bytes |                                         1 | **148,704** |          **29,741** |              148.70 MB/s |                4,461,289 |           30.001 s | 503.1% / 10.25 MiB |

## Quick Start

> You can use it as a template when building your own server. The code below is the same as examples/echo_server/main.go.

erio **avoids implementation approaches that depend only on callbacks, such as the On{Event}(**onComplete func(...)**) pattern**.

Instead, it is designed so that users implement the interface's event callbacks themselves, and effort has gone into making the design hard to **misuse**.

> APIs of the form **On{Event}(onComplete func(...))**, which take a completion callback as an argument, leave additional concurrency problems for the user to solve.
>
> Therefore, unlike other frameworks, erio does not provide **default/empty method implementations or methods that take a callback function per event as an argument**.
>
> - OnUserEvent, OnTimeout, and OnReadClosed are exceptions, as they are optional for the user.

### Echo Server Example

The following is a TCP echo server that sends received data back as is.

Embed HandlerContext, implement the event callbacks, and then register the Handler factory function with the Builder.

```go
// erio echo server example: sends received data back and closes the connection if nothing is received for 10 seconds.
package main

import (
	"context"
	"log"
	"net/netip"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/netiogreem/erio"
)

// Alias for HandlerContext.
type HandlerContext = erio.HandlerContext

// Per-connection handler.
// All EchoHandler(ClientHandler) callbacks are called on the same goroutine, so avoid long-running work.
type EchoHandler struct {
	*HandlerContext
	streamID int32 // ID that tells which Write completed in OnWritten
}

var _ erio.ClientHandler = (*EchoHandler)(nil)

// Timer key for idle monitoring. Timers are managed per connection.
const ALIVE_TIMER_KEY uint64 = 10

// Called each time the Acceptor accepts a connection.
// See: client_handler.go::ClientHandlerFactory
func EchoHandlerFactory(fd erio.FileDescriptor, listenAddr netip.AddrPort) (erio.ClientHandler, error) {
	// Send buffer 64KiB, command quota 64
	// closePendingWriteTimeout 0 (Close sends all remaining data with no time limit, then closes the connection.)
	// See: handler_context.go::NewHandlerContext
	handler, err := erio.NewHandlerContext(fd, listenAddr, 65536, 64, 0)
	if err != nil {
		return nil, err
	}

	return &EchoHandler{HandlerContext: handler, streamID: 0}, nil
}

// Called once after the connection is registered with the Reactor.
func (this *EchoHandler) OnConnect(context *HandlerContext) {
	log.Printf("%s connected, active clients: %d", context.PeerAddrPort(), context.Count())
	// Timers are one-shot; setting the same key again replaces the expiration time.
	if err := context.SetTimeout(ALIVE_TIMER_KEY, 10*time.Second); err != nil {
		log.Printf("%s timer setup error: %v", context.PeerAddrPort(), err)
	}
}

// receivedData changes after the callback returns, so copy it if you need to keep it.
func (this *EchoHandler) OnRead(context *HandlerContext, receivedData []byte) {
	if err := context.SetTimeout(ALIVE_TIMER_KEY, 10*time.Second); err != nil {
		log.Printf("%s timer setup error: %v", context.PeerAddrPort(), err)
	}

	log.Printf("%s received: %d", context.PeerAddrPort(), len(receivedData))
	this.streamID++
	// Copies the data into the send buffer and returns immediately. Write completion is reported through OnWritten with the same streamID.
	if err := context.Write(this.streamID, receivedData); err != nil {
		log.Printf("%s send error: %v", context.PeerAddrPort(), err)
		return
	}

	log.Printf("%s sent: %d, streamID:%d", context.PeerAddrPort(), len(receivedData), this.streamID)
}

// Called for each streamID whose data passed to Write has been successfully written to the kernel send buffer.
func (this *EchoHandler) OnWritten(context *HandlerContext, streamID int32, writtenBytes uint32) {
	log.Printf("%s write completed: streamID %d, written bytes %d", context.PeerAddrPort(), streamID, writtenBytes)
}

// Called when a timer expires.
func (this *EchoHandler) OnTimeout(context *HandlerContext, timerKey uint64) {
	log.Printf("%s timeout: timerKey %d", context.PeerAddrPort(), timerKey)
	// Only requests the close and returns. OnClose is called after the connection is cleaned up.
	if err := context.Close(); err != nil {
		log.Printf("%s close request error: %v", context.PeerAddrPort(), err)
	}
}

// Called when an error occurs while receiving, sending, or processing commands.
func (this *EchoHandler) OnError(context *HandlerContext, clientError error) {
	log.Printf("%s error: %v", context.PeerAddrPort(), clientError)
}

// Called once when the connection is removed.
// closeReason: ErrTCPReactorCloseRequested, ErrTCPReactorHangup, ErrTCPReactorStopped, etc.
func (this *EchoHandler) OnClose(context *HandlerContext, closeReason error) {
	log.Printf("%s closed: %v", context.PeerAddrPort(), closeReason)
	log.Printf("active clients: %d", context.Count())
}

func main() {
	// Shuts down the server on Ctrl+C or SIGTERM.
	shutdown, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	reactorError := func(err error) { log.Print(err) }
	acceptError := func(err error) { log.Print(err) }

	server, err := erio.NewBuilder().
		WithReactor(erio.ReactorParam{Count: 3,
			EventBatchSize:       128,
			RegisterCommandQuota: 1024,
			CommandReserveSize:   4096,
			ReadBufferSize:       32 * 1024, // Receive buffer of each Reactor.
			ErrorCallback:        reactorError}).
		// Accepts connections on two addresses.
		WithListenAddress("127.0.0.1:2000").
		WithListenAddress("127.0.0.1:3000").
		WithAcceptErrorCallback(acceptError).
		WithClientHandlerFactory(EchoHandlerFactory).
		Build()

	if err != nil {
		log.Fatal(err)
	}

	if err := server.Start(); err != nil {
		log.Fatal(err)
	}

	log.Print("Listening on 127.0.0.1:2000, 127.0.0.1:3000. Press Ctrl+C to exit.")
	<-shutdown.Done()
	if err := server.Stop(); err != nil {
		log.Fatal(err)
	}
}
```

Save the code above to erio/examples/echo_server/main.go and run it from the erio/ directory.

```bash
go run ./examples/echo_server
```

### User Data Delivery Example

If you define an identifier and data in the command and deliver it with HandlerContext.PostUserEvent, you can receive the data in ClientHandler.OnUserEvent.

```Go
// User-defined data
type TaskCompleted struct {
	taskID uint64
	result string
}

// User handler
type MyHandler struct {
	*erio.HandlerContext
	taskID uint64
	result string
  ...
}

// From another goroutine, you can deliver a user-defined ID and data as follows.
// handler is the *erio.HandlerContext of the target session.
// PostUserEvent delivers the content asynchronously.
if err := handler.PostUserEvent(TaskCompleted{
	taskID: 123,
	result: "completed",
}); err != nil {
	log.Printf("failed to submit task completion command: %v", err)
}

// Handle the delivered data in the user handler as follows.
func (this *MyHandler) OnUserEvent(handler *erio.HandlerContext, userEventData any) {
	switch received := userEventData.(type) {
	case TaskCompleted:
		this.taskID = received.taskID
		this.result = received.result
		log.Printf("session=%s task=%d result=%s", handler.PeerAddrPort(), this.taskID, this.result)
	}
}
```

---

## API

### Builder

* Builder is implemented as a fluent interface, so you can configure the server by chaining method calls.
  * If the configuration is valid, you get a TCPServer.
* WithReactor
  * Sets the number of Reactors, the per-Reactor event batch size, command queue capacity, and read buffer size, and the error callback.
  * A Reactor is an instance that processes Epoll events; due to the nature of syscall.Epoll, it occupies one thread.
  * ReactorParam
    * Count: The number of Reactors.
    * EventBatchSize: The maximum number of events received per epoll wait.
    * RegisterCommandQuota: The maximum number of RegisterHandler commands each Reactor can accept at a time.
    * CommandReserveSize: The reserved size of the command queue.
    * ReadBufferSize: The receive buffer size of each Reactor.
    * ErrorCallback: Reactor error callback
* WithListenAddress(listenAddress string)
  * Registers a listen address. (e.g., 172.30.10.10:30010)
  * Call it again to add more listen addresses.
* WithClientHandlerFactory
  * Registers a Factory function that returns an implementation of the ClientHandler interface.
    * Factory function signature: func(fd FileDescriptor, listenAddress netip.AddrPort) (ClientHandler, error)
  * When a client connects, the Acceptor calls the Factory function, and the connection is attached to a Reactor.

```go
// ReactorParam is the configuration for the TCPReactor created by the Builder.
type ReactorParam struct {
	Count                uint32      // Number of Reactors (if 0, GOMAXPROCS-1, minimum 1)
	EventBatchSize       uint32      // Maximum number of events received per epoll wait
	RegisterCommandQuota uint32      // Register command queue limit
	CommandReserveSize   uint32      // Reserved size of the command queue
	ReadBufferSize       uint32      // Receive buffer size of each Reactor
	ErrorCallback        func(error) // Reactor error callback
}

// Creates a Builder with an empty configuration.
func NewBuilder() *Builder

// Sets the number of Reactors, the per-Reactor event batch size, command queue capacity, and read buffer size, and the error callback.
func (this *Builder) WithReactor(param ReactorParam) *Builder

// Adds a listen address. If the port is 0, it is assigned automatically at start.
func (this *Builder) WithListenAddress(listenAddress string) *Builder

// Sets the error callback for accepting connections, creating Handlers, and requesting Reactor registration.
func (this *Builder) WithAcceptErrorCallback(acceptErrorCallback func(error)) *Builder

// Sets the Factory that creates a ClientHandler for each accepted connection.
func (this *Builder) WithClientHandlerFactory(factory ClientHandlerFactory) *Builder

// Creates a TCPServer from the configured Acceptors and Reactors.
func (this *Builder) Build() (server *TCPServer, err error)
```

### TCPServer

* Created by Builder.Build(), it manages accepting connections and starting and stopping the Reactors.
* Start() starts all Reactors and then starts listening.
* Stop() stops accepting new connections, stops all Reactors, waits for them to finish, and releases their epoll and eventfd resources. It returns nil regardless of the stop results.
* Listen address indexes start at 0 in registration order. If you specified port 0, you can query the port actually assigned after Start() succeeds.

```go
// Starts all Reactors and listeners.
func (this *TCPServer) Start() error

// Stops accepting connections, stops all Reactors, and releases their resources.
func (this *TCPServer) Stop() error

// Returns the number of ClientHandlers registered in all Reactors of the TCPServer.
func (this *TCPServer) HandlerCount() uint32

// Returns the addresses and ports being listened on.
func (this *TCPServer) ListenAddrPorts() []netip.AddrPort
```

### ClientHandler

All ClientHandler callbacks are called on the same goroutine, which keeps session events serialized.

Because erio **avoids implementation approaches that rely solely on callbacks, such as the On{Event}(completion callback function) pattern**, it does not provide **default/empty implementations or per-event callback functions**, except for OnUserEvent, OnTimeout, and OnReadClosed, which are optional for users.

* Users must implement the ClientHandler interface. A ClientHandler is created for each connection.
* ClientHandler must not be used from other goroutines.
  * When you need to send commands from another goroutine, you can safely use the HandlerContext owned by the ClientHandler.
* ClientHandler is called only from the TCPReactor's goroutine.

```Go
package erio

import (
	"github.com/netiogreem/erio/internal"
	"net/netip"
)

// FileDescriptor is the file descriptor of a connection socket, and is an alias of
// internal.FileDescriptor (int32).
type FileDescriptor = internal.FileDescriptor

// ClientHandlerFactory is the ClientHandler factory function that Acceptor calls for each accepted connection.
// fd is the FileDescriptor of the connection, and listenAddress is the listen address that accepted the connection.
// If it returns an error or a nil Handler, Acceptor rejects the connection.
type ClientHandlerFactory func(fd FileDescriptor, listenAddress netip.AddrPort) (ClientHandler, error)

// ClientHandler is the user-facing callback interface for handling client events in a TCPReactor.
// Users implement a handler that embeds HandlerContext.
// The handler is registered with the TCPReactor via ClientHandlerFactory,
// and when a socket event occurs, the TCPReactor calls the handler's callback for that event.
type ClientHandler interface {
	// OnConnect is called when the connection is established and registered with the Reactor.
	//
	//   - context: HandlerContext of the connection
	OnConnect(context *HandlerContext)

	// OnRead is called when data is received.
	// To keep the data after the callback, receivedData must be copied.
	//
	//   - context: HandlerContext of the connection
	//   - receivedData: received data
	OnRead(context *HandlerContext, receivedData []byte)

	// OnWritten is called when the data requested with Write has been written to the kernel.
	//
	//   - context: HandlerContext of the connection
	//   - streamID: streamID defined by the user in the Write call
	//   - writtenBytes: number of bytes written
	OnWritten(context *HandlerContext, streamID int32, writtenBytes uint32)

	// OnUserEvent processes user event data submitted via PostUserEvent.
	// It is optional for the user; HandlerContext provides an empty implementation.
	//
	//   - context: HandlerContext of the connection
	//   - userEventData: event data submitted by the user
	OnUserEvent(context *HandlerContext, userEventData any)

	// OnTimeout is called when a timer expires.
	//
	//   - context: HandlerContext of the connection
	//   - timerKey: key of the expired timer
	OnTimeout(context *HandlerContext, timerKey uint64)

	// OnReadClosed is called when the peer closes its sending side (half-close).
	// The connection can still send, and no more data is received.
	// HandlerContext provides a default implementation that calls Close, so the connection is
	// closed after the remaining data is sent.
	// To keep the connection and continue sending, implement this method and call Close when
	// sending is finished.
	//
	//   - context: HandlerContext of the connection
	OnReadClosed(context *HandlerContext)

	// OnError is called when an error occurs while handling an event.
	//
	//   - context: HandlerContext of the connection
	//   - clientError: error that occurred
	//
	// If the TCPReactor fails while processing the registration, only OnError is called, and
	// OnConnect and OnClose are not called.
	// If TCPReactor.RegisterHandler returns an error, such as ErrTCPReactorStopped, no ClientHandler
	// callback is called, and the error is passed to the callback set with WithAcceptErrorCallback.
	// Send and receive errors do not close the connection. If the connection is lost, it is cleaned
	// up with OnClose(ErrTCPReactorHangup). To close the connection after checking the error, call
	// Close or AbortiveClose.
	OnError(context *HandlerContext, clientError error)

	// OnClose is called when the connection is lost and detached from the Reactor.
	//
	//   - context: HandlerContext of the connection
	//   - closeReason: reason the connection was closed
	//
	// closeReason is the situation at the time the connection is closed.
	//   - ErrTCPReactorCloseRequested: Close closed the connection, including Close called by the
	//     default OnReadClosed. The remaining data is sent before closing, but if registering
	//     write monitoring fails, OnError is called and the connection is closed without sending
	//     the remaining data.
	//   - ErrTCPReactorAbortiveCloseRequested: AbortiveClose closed the connection with RST.
	//   - ErrTCPReactorClosePendingWriteTimeout: the remaining data was not sent within
	//     closePendingWriteTimeout after Close, and the connection was closed with RST.
	//   - ErrTCPReactorHangup: the connection was lost.
	//   - ErrTCPReactorStopped: TCPReactor was stopped.
	OnClose(context *HandlerContext, closeReason error)

	// GetCommandQuota returns the command limit.
	// It is a function implemented by HandlerContext.
	//
	// It returns the limit.
	GetCommandQuota() uint32

	// context returns the HandlerContext embedded in the Handler.
	//
	// It returns the HandlerContext pointer of the connection.
	context() *HandlerContext
}
```

### HandlerContext

* Must be embedded in a ClientHandler.
* HandlerContext is the type that controls a ClientHandler, and its methods, except Init() and Reset(), are thread-safe and can also be used from other goroutines.
* Calls to HandlerContext's command methods (Write, SetTimeout, UnsetTimeout, Close, AbortiveClose, PostUserEvent) are asynchronous requests to the TCPReactor for serialized event processing; the TCPReactor processes them and calls the corresponding ClientHandler functions.

```Go
// Creates a HandlerContext for a client TCP connection.
func NewHandlerContext(handlerFD FileDescriptor, listenAddrPort netip.AddrPort,
	writeBufferSize uint32, commandQuota uint32, closePendingWriteTimeout time.Duration) (*HandlerContext, error)

// Reuses the existing buffers, commandQuota, and closePendingWriteTimeout, and reinitializes the object with the given connection information.
// Does not allocate new buffers.
// It is safe to call only before the handler is registered with the Reactor (before returning it
// from ClientHandlerFactory) or as the last call in OnClose.
func (this *HandlerContext) Init(handlerFD FileDescriptor, listenAddrPort netip.AddrPort) error

// Clears all state except commandQuota and closePendingWriteTimeout, and sets handlerFD to -1.
// Buffers keep their capacity. It does not close handlerFD, so call it after the connection is closed.
// It is safe to call only before the handler is registered with the Reactor (before returning it
// from ClientHandlerFactory) or as the last call in OnClose.
func (this *HandlerContext) Reset() error

// Returns the number of connected clients.
func (this *HandlerContext) Count() uint32

// Returns the address and port of the remote peer.
func (this *HandlerContext) PeerAddrPort() netip.AddrPort

// Returns the listen address and port that accepted the connection.
func (this *HandlerContext) ListenAddrPort() netip.AddrPort

// Returns the command quota of this session (commandQuota of NewHandlerContext).
func (this *HandlerContext) GetCommandQuota() uint32

// Copies the data into the send buffer and submits a send command. The streamID identifies it in OnWritten.
func (this *HandlerContext) Write(streamID int32, buffer []byte) error

// Submits a command to set or replace the timer for the given key.
// One call does not invoke OnTimeout repeatedly; it is one-shot.
func (this *HandlerContext) SetTimeout(timerKey uint64, timeout time.Duration) error

// Submits a command to unset the timer for the given key.
func (this *HandlerContext) UnsetTimeout(timerKey uint64) error

// Submits a user command and delivers it to ClientHandler.OnUserEvent on the session's Reactor goroutine.
func (this *HandlerContext) PostUserEvent(userEventData any) error

// Records the closed state first and requests a normal close of the connection.
// The Reactor stops receiving, sends the data passed to Write before the call, and then closes with FIN.
// If the data is not sent within closePendingWriteTimeout, the connection is closed with RST.
// If received data remains unread, the peer receives RST instead of FIN.
func (this *HandlerContext) Close() error

// Records the closed state first and requests a forced close (RST) of the connection.
// Data not yet delivered to the peer is discarded, including data already reported by OnWritten.
// It can also be called while Close is sending the remaining data.
func (this *HandlerContext) AbortiveClose() error

// Reports whether the session is closed.
// It is true after Close is called, even while the remaining data is being sent.
func (this *HandlerContext) IsClosed() bool

// Reports whether the peer has closed only its sending side (half-close).
// While it is true, the session can still send, and no more data is received.
func (this *HandlerContext) IsReadClosed() bool
```

It can be used as follows.

```Go
type MyHandler struct {
	*HandlerContext
	.....
}

func (this *MyHandler) OnConnect(context *HandlerContext) {
	......
}

func (this *MyHandler) OnRead(context *HandlerContext, receivedData []byte) {
	......
}

func (this *MyHandler) OnClose(context *HandlerContext, closeReason error) {
	......
}

.....
```

---

## Caveats

* **Do not block for long in event callbacks.**
  * ClientHandler sessions assigned to the same Reactor are processed on a single goroutine. Long computations, time.Sleep, or infinite retries inside a callback delay the processing of other sessions on that Reactor. Long transactions must be designed separately.
* **Do not use ClientHandler from other goroutines.**
  * ClientHandler is not safe to call from other goroutines. From other goroutines, use the control methods provided by HandlerContext.
* **Init() and Reset() on HandlerContext are not thread-safe.**
  * When necessary, call it last, either before registering with the Reactor (i.e., before returning from ClientHandlerFactory) or inside OnClose.
* **Copy the OnRead receive buffer if you need to keep it.**
  * receivedData in OnRead is a reused internal buffer. Copy it if you keep it after the callback or pass it to another goroutine. Write, on the other hand, copies the data into an internal buffer, so you can reuse the original buffer after it returns.
* **OnWritten does not mean the peer has received the data.**
  * It means the data has been fully handed over to the local kernel. To confirm the peer's receipt or the completion of application-level processing, you need a protocol-level response.
* **Close closes the connection after the remaining data is sent.**
  * Close() records the closed state and submits a close request to the Reactor. The Reactor sends the data remaining in the HandlerContext send buffer, closes the connection with FIN, and then calls ClientHandler.OnClose().
  * If the peer does not receive the data, the connection stays open. Set closePendingWriteTimeout of NewHandlerContext, or call AbortiveClose().
* **By default, the connection is closed when the peer closes its sending side (half-close).**
  * The default OnReadClosed calls Close(), so the connection is closed after the remaining data is sent.
