// erio echo server example: sends received data back and closes the connection if nothing is sent or received for 10 seconds.
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
	// Receive buffer 32KiB, send buffer 64KiB, command quota 64, SO_LINGER 5 seconds.
	// See: handler_context.go::NewHandlerContext
	handler, err := erio.NewHandlerContext(fd, listenAddr, 32*1024, 65536, 64, 5)
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
// closeReason: ErrTCPReactorReadHangup, ErrTCPReactorHangup, ErrTCPReactorStopped, etc.
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

	server, err := erio.Builder().
		WithReactor(erio.ReactorParam{Count: 3,
			EventBatchSize:       128,
			RegisterCommandQuota: 1024,
			CommandReserveSize:   4096,
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
