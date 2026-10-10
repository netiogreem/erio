# erio

[[English](README.md)]

Epoll Reactor-based TCP I/O framework

## 소개

**세션 연결 상태 불변성과 세션 이벤트 직렬화를 위해 동일 고루틴에서 세션 처리**를 보장하도록 설계된 프레임워크입니다.

Reactor 패턴으로 Linux epoll을 사용하여 입출력 다중화를 처리하며, 연결마다 고루틴을 생성하지 않는 이벤트 드리븐 방식으로 동작합니다.

erio는 다음과 같은 경우에 적합합니다.

* 세션을 유지하면서, 해당 세션 내의 데이터를 세션 내에서 락 없이 변경하고 싶을 때
* 세션 내에서 락이나 복잡한 코드 없이 독립적인 여러 타이머(예: 유휴 연결 감지, 인증 타임아웃)가 필요할 때
* 외부 고루틴에서 세션 고루틴으로 안전한 비동기 이벤트 전달(예: Write, PostUserEvent 사용)이 필요할 때

## 요구사항

- **Linux**: epoll과 eventfd를 사용합니다.
- **Go 1.26.0 이상**: 현재 go.mod에 지정된 버전입니다.
- **외부 의존성**: 타이머 관리에 github.com/emirpasic/gods v1.18.1을 사용하며, Go 모듈로 관리합니다.

## 주요 특징

* **세션 이벤트 직렬화**
  * 연결, 수신, 송신/송신 완료, 멀티 타이머, 종료 이벤트를 동일한 Reactor 고루틴에서 직렬로 처리하며, 같은 세션의 이벤트 콜백이 동시에 실행되지 않습니다.
* **여러 리스닝 주소와 연결 분산**
  * 여러 주소와 포트를 등록하고, 수락한 연결을 여러 Reactor에 순환 배정할 수 있습니다.
* **연결별 고루틴 없는 다중 세션 처리**
  * 연결마다 송수신 고루틴을 사용하지 않고, 각 Reactor는 epoll로 이벤트를 받아, 그 이벤트가 발생한 연결의 ClientHandler를 식별해 처리합니다.
* **보낸 데이터 완료시점**
  * 세션 이벤트가 처리되는 동일한 고루틴에서 Write로 쓰기 요청한 데이터를 OnWritten(streamID)으로 커널 전달 완료를 확인하고, 다음 데이터의 쓰기를 이어갈 수 있습니다.
* **하나의 세션에서 멀티 타이머**
  * 세션 이벤트가 처리되는 동일한 고루틴에서 SetTimeout(timerKey, timeout), OnTimeout(timerKey) 타이머키로 응답 대기·유휴 연결·인증 제한 시간을 각각 관리할 수 있습니다.
* **외부 고루틴에서 세션 제어**
  * HandlerContext의 Write, SetTimeout, UnsetTimeout, Close, AbortiveClose, PostUserEvent는 외부 고루틴에서 별도의 추가 작업(동기화 작업따위) 없이 호출할 수 있습니다.
* **원하는 세션에 사용자 데이터셋의 비동기 전달**
  * 외부 고루틴에서 원하는 세션에 사용자가 정의한 데이터셋을 비동기로 전달하고, 해당 세션이 동작하는 고루틴의 세션에서 받을 수 있습니다.

## 성능

erio와 gnet 모두 공개 API를 사용하며 수신된 패킷별로 하나의 응답을 전송합니다(수신된 패킷당 'Write()' 호출 1회, Pipeline이 100인 경우 100개의 패킷, 100번 응답).

* AMD Ryzen™ 9 7945HX (32 logical cores), CPU clock 2.85GHz to 3.7GHz
* 6 cores (1 Acceptor, 5 Reactors) / Clients: 25 separate processes

### 8-byte response

클라이언트로 부터 수신 후 테스트 서버는 개별 패킷별로 8바이트 수신 완료 응답을 보냅니다.

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

클라이언트로 부터 수신 후 테스트 서버는 개별 패킷별로 65536 바이트 수신 완료 응답을 보냅니다.

#### erio

| Client<br />message <br />size | Pipelined<br />requests<br /> per client |     **TPS** | Avg TPS<br /> per Reactor | Receive<br /> throughput | Messages<br /> processed | Elapsed<br /> time |           CPU / RSS |
| -----------------------------: | ---------------------------------------: | ----------------: | ------------------------: | -----------------------: | -----------------------: | -----------------: | ------------------: |
|                    1,000 bytes |                                      100 | **123,300** |          **24,660** |              123.30 MB/s |                3,701,594 |           30.021 s | 501.9% / 266.00 MiB |
|                    1,000 bytes |                                        1 | **143,270** |          **28,654** |              143.27 MB/s |                4,298,296 |           30.001 s |  501.7% / 21.25 MiB |

> 1,000 bytes-100 pipelined 에서 RSS(266.00MiB)가 높은 이유는 OnWritten을 이용하지 않고 단순히 세션당 write버퍼를 65536*100 으로 설정했기 때문입니다.

#### gnet v2.10.0

| Client<br />message <br />size | Pipelined<br /> requests<br /> per client |     **TPS** | Avg TPS<br /> per Reactor | Receive<br /> throughput | Messages<br /> processed | Elapsed<br /> time |          CPU / RSS |
| -----------------------------: | ----------------------------------------: | ----------------: | ------------------------: | -----------------------: | -----------------------: | -----------------: | -----------------: |
|                    1,000 bytes |                                       100 | **149,553** |          **29,911** |              149.55 MB/s |                4,489,290 |           30.018 s | 509.1% / 16.25 MiB |
|                    1,000 bytes |                                         1 | **148,704** |          **29,741** |              148.70 MB/s |                4,461,289 |           30.001 s | 503.1% / 10.25 MiB |

## 빠른 시작

> 새 서버를 만들 때 템플릿으로 활용하실 수 있습니다. 아래 코드는 examples/echo_server/main.go와 같습니다.

erio는 **On{Event}(onComplete func(...)) 패턴**의 **콜백에만 의존하는 구현 방식을 지양**합니다.

대신 사용자가 인터페이스의 콜백 이벤트들을 직접 구현하도록 설계하고, **오용(misuse)**하기 어려운 구조를 만드는 데 노력하였습니다.

> 콜백으로 완료 인자를 받는 **On{Event}(onComplete func(...))** 의 형태는 동시성 문제를 사용자가 추가적으로 해결해야 합니다.
>
> 따라서 erio는 다른 프레임워크들과 달리 **기본/공백 구현 메서드나 이벤트별로 콜백함수를 인자로 받는 메서드**는 제공하지 않습니다.
>
> - OnUserEvent, OnTimeout, OnReadClosed는 사용자 선택사항으로 예외로 기본/공백 구현 메서드가 구현되어 있습니다.

### 에코서버 예제

다음은 수신한 데이터를 그대로 돌려주는 TCP 에코 서버입니다.

HandlerContext를 임베딩하고 이벤트 콜백을 구현한 뒤, Builder에 Handler 생성 함수를 등록합니다.

```go
// erio 에코 서버 예제: 받은 데이터를 돌려보내고, 10초 동안 송수신이 없으면 연결을 닫습니다.
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

// HandlerContext에 별칭을 부여합니다.
type HandlerContext = erio.HandlerContext

// 연결별 핸들러입니다.
// EchoHandler(ClientHandler)의 콜백 함수는 모두 동일한 고루틴에서 호출되므로 오래 걸리는 작업은 피해야 합니다.
type EchoHandler struct {
	*HandlerContext
	streamID int32 // OnWritten에서 어느 Write가 끝났는지 구분하는 ID
}

var _ erio.ClientHandler = (*EchoHandler)(nil)

// 무통신 감시 타이머 키입니다. 타이머는 연결별로 관리됩니다.
const ALIVE_TIMER_KEY uint64 = 10

// Acceptor가 연결을 수락할 때마다 호출합니다.
// 참고: client_handler.go::ClientHandlerFactory
func EchoHandlerFactory(fd erio.FileDescriptor, listenAddr netip.AddrPort) (erio.ClientHandler, error) {
	// 송신 버퍼 64KiB, 명령 한도 64개
	// closePendingWriteTimeout 0 (Close가 남은 데이터를 시간제한 없이 모두 보낸 후 끊습니다.)
	// 참고: handler_context.go::NewHandlerContext
	handler, err := erio.NewHandlerContext(fd, listenAddr, 65536, 64, 0)
	if err != nil {
		return nil, err
	}

	return &EchoHandler{HandlerContext: handler, streamID: 0}, nil
}

// 연결이 Reactor에 등록된 뒤 한 번 호출됩니다.
func (this *EchoHandler) OnConnect(context *HandlerContext) {
	log.Printf("%s 연결, 접속중인 Client 수: %d", context.PeerAddrPort(), context.Count())
	// 타이머는 1회성이며, 같은 키로 다시 설정하면 만료 시각이 교체됩니다.
	if err := context.SetTimeout(ALIVE_TIMER_KEY, 10*time.Second); err != nil {
		log.Printf("%s 타이머 설정 오류: %v", context.PeerAddrPort(), err)
	}
}

// receivedData는 콜백이 끝나면 바뀌므로, 보관하려면 복사해야 합니다.
func (this *EchoHandler) OnRead(context *HandlerContext, receivedData []byte) {
	if err := context.SetTimeout(ALIVE_TIMER_KEY, 10*time.Second); err != nil {
		log.Printf("%s 타이머 설정 오류: %v", context.PeerAddrPort(), err)
	}

	log.Printf("%s 수신: %d", context.PeerAddrPort(), len(receivedData))
	this.streamID++
	// 송신 버퍼에 복사하고 바로 반환합니다. 송신 완료는 같은 streamID로 OnWritten으로 호출됩니다.
	if err := context.Write(this.streamID, receivedData); err != nil {
		log.Printf("%s 전송 오류: %v", context.PeerAddrPort(), err)
		return
	}

	log.Printf("%s 전송: %d, streamID:%d", context.PeerAddrPort(), len(receivedData), this.streamID)
}

// Write한 데이터가 커널 송신 버퍼에 쓰기 성공한 streamID 별로 호출됩니다.
func (this *EchoHandler) OnWritten(context *HandlerContext, streamID int32, writtenBytes uint32) {
	log.Printf("%s 쓰기 완료: streamID %d, 완료된 바이트 %d", context.PeerAddrPort(), streamID, writtenBytes)
}

// 타이머가 만료되면 호출됩니다.
func (this *EchoHandler) OnTimeout(context *HandlerContext, timerKey uint64) {
	log.Printf("%s 타임아웃: timerKey %d", context.PeerAddrPort(), timerKey)
	// 종료를 요청만 하고 반환합니다. 연결 정리 후 OnClose가 호출됩니다.
	if err := context.Close(); err != nil {
		log.Printf("%s 종료 요청 오류: %v", context.PeerAddrPort(), err)
	}
}

// 수신,송신,명령 처리 중 오류가 발생하면 호출됩니다.
func (this *EchoHandler) OnError(context *HandlerContext, clientError error) {
	log.Printf("%s 오류: %v", context.PeerAddrPort(), clientError)
}

// 연결이 제거될 때 한 번 호출됩니다.
// closeReason: ErrTCPReactorCloseRequested, ErrTCPReactorHangup, ErrTCPReactorStopped 등
func (this *EchoHandler) OnClose(context *HandlerContext, closeReason error) {
	log.Printf("%s 종료: %v", context.PeerAddrPort(), closeReason)
	log.Printf("접속중인 Client 수: %d", context.Count())
}

func main() {
	// Ctrl+C 또는 SIGTERM을 받으면 서버를 종료합니다.
	shutdown, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	reactorError := func(err error) { log.Print(err) }
	acceptError := func(err error) { log.Print(err) }

	server, err := erio.NewBuilder().
		WithReactor(erio.ReactorParam{Count: 3,
			EventBatchSize:       128,
			RegisterCommandQuota: 1024,
			CommandReserveSize:   4096,
			ReadBufferSize:       32 * 1024, // Reactor별 수신 버퍼입니다.
			ErrorCallback:        reactorError}).
		// 두 주소에서 연결을 수락합니다.
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

	log.Print("127.0.0.1:2000, 127.0.0.1:3000에서 대기 중입니다. Ctrl+C로 종료합니다.")
	<-shutdown.Done()
	if err := server.Stop(); err != nil {
		log.Fatal(err)
	}
}
```

위 코드를 erio/examples/echo_server/main.go에 저장한 뒤 erio/ 디렉터리에서 실행합니다.

```bash
go run ./examples/echo_server
```

### 사용자 데이터셋 전달 예제

명령에는 식별자와 데이터를 정의하고 HandlerContext.PostUserEvent로 전달하면 ClientHandler.OnUserEvent에서 데이터를 받을 수 있습니다.

```Go
// 사용자가 정의하는 데이터셋
type TaskCompleted struct {
	taskID uint64
	result string
}

// 사용자 핸들러
type MyHandler struct {
	*erio.HandlerContext
	taskID uint64
	result string
  ...
}

// 외부 고루틴에서 다음과 같이 사용자가 정의한 ID와 데이터셋을 전달할 수 있습니다.
// handler는 대상 세션의 *erio.HandlerContext입니다.
// PostUserEvent는 비동기로 내용을 전달합니다.
if err := handler.PostUserEvent(TaskCompleted{
	taskID: 123,
	result: "처리 완료",
}); err != nil {
	log.Printf("작업 완료 명령 접수 실패: %v", err)
}

// 사용자 핸들러에서 전달받은 데이터를 다음과 같이 처리합니다.
func (this *MyHandler) OnUserEvent(handler *erio.HandlerContext, userEventData any) {
	switch received := userEventData.(type) {
	case TaskCompleted:
		this.taskID = received.taskID
		this.result = received.result
		log.Printf("세션=%s 작업=%d 결과=%s", handler.PeerAddrPort(), this.taskID, this.result)
	}
}
```

## API

### Builder

* Builder는 플루언트 인터페이스로 구성되어 있어 메서드 체이닝으로 호출, 서버를 구성할 수 있습니다.
  * 설정이 올바르다면, TCPServer 인스턴스를 반환 받습니다.
* WithReactor
  * Reactor 수, Reactor별 이벤트 배치 크기, 명령 대기열 용량, 수신 버퍼 크기와 오류 콜백을 설정합니다.
  * Reactor는 Epoll 이벤트를 처리하는 인스턴스로 syscall.Epoll 특성상 1 스레드를 점유합니다.
  * ReactorParam
    * Count: Reactor의 개수 입니다.
    * EventBatchSize: epoll 대기시 한번에 받을 최대 이벤트 수 입니다.
    * RegisterCommandQuota: Reactor에 등록할 수 있는 대기열 한도 입니다.
    * CommandReserveSize: 명령 대기열의 예약 크기 입니다.
    * ReadBufferSize: Reactor별 수신 버퍼의 크기 입니다.
    * ErrorCallback: Reactor 오류 콜백 함수 입니다.
* WithListenAddress(listenAddress string)
  * 리스닝 주소를 등록 합니다.(ex: 172.30.10.10:30010)
  * 다수의 리스닝을 추가하려면 재호출 합니다.
* WithClientHandlerFactory
  * ClientHandler 인터페이스의 구현체를 반환하는 Factory 함수를 등록합니다.
  * Factory함수 형식: func(fd FileDescriptor, listenAddress netip.AddrPort) (ClientHandler, error)
  * Factory함수는 ClientHandler를 항상 포인터(예: &MyHandler{...})로 반환해야 합니다. 값으로 반환하면 연결이 거절됩니다.
  * 클라이언트가 접속하면 Acceptor는 Factory함수를 호출하고 Reactor와 연결됩니다.

```go
// ReactorParam은 Builder가 생성할 TCPReactor의 설정입니다.
type ReactorParam struct {
	Count                uint32      // Reactor 개수 (0인 경우, GOMAXPROCS-1, 최소 1)
	EventBatchSize       uint32      // epoll 대기 한 번에 받을 최대 이벤트 수
	RegisterCommandQuota uint32      // Register 명령 대기 한도
	CommandReserveSize   uint32      // 명령 대기열의 예약 크기 입니다.
	ReadBufferSize       uint32      // Reactor별 수신 버퍼 크기
	ErrorCallback        func(error) // Reactor 오류 콜백 함수
}

// 빈 설정으로 Builder를 생성합니다.
func NewBuilder() *Builder

// Reactor 수, Reactor별 이벤트 배치 크기, 명령 대기열 용량, 수신 버퍼 크기와 오류 콜백을 설정합니다.
func (this *Builder) WithReactor(param ReactorParam) *Builder

// 리스닝 주소를 추가합니다. 포트가 0이면 시작 시 자동 할당됩니다.
func (this *Builder) WithListenAddress(listenAddress string) *Builder

// 연결 수락·Handler 생성·Reactor 등록 요청 과정의 오류 콜백을 설정합니다.
func (this *Builder) WithAcceptErrorCallback(acceptErrorCallback func(error)) *Builder

// 수락한 연결마다 ClientHandler를 생성할 Factory를 설정합니다.
func (this *Builder) WithClientHandlerFactory(factory ClientHandlerFactory) *Builder

// 설정한 Acceptors와 Reactor들로 TCPServer를 생성합니다.
func (this *Builder) Build() (server *TCPServer, err error)
```

### TCPServer

* Builder.Build()로 생성하며, 연결 수락과 Reactor들의 시작·종료를 관리합니다.
* Start()는 모든 Reactor를 시작한 뒤 리스닝을 시작합니다.
* Stop()은 새로운 연결 수락을 중단하고 모든 Reactor를 종료한 뒤 종료 완료를 기다리고, epoll과 eventfd 자원을 해제합니다. 종료 결과와 관계없이 nil을 반환합니다.
* 리스닝 주소의 인덱스는 등록 순서대로 0부터 시작합니다. 포트를 0으로 지정했다면 Start() 성공 후 실제 할당된 포트를 조회할 수 있습니다.

```go
// 모든 Reactor와 리스너를 시작합니다.
func (this *TCPServer) Start() error

// 연결 수락을 중단하고 모든 Reactor를 종료한 뒤 자원을 해제합니다.
func (this *TCPServer) Stop() error

// TCPServer가 가진 모든 Reactor에 등록된 ClientHandler 수를 반환합니다.
func (this *TCPServer) HandlerCount() uint32

// 리스닝 하고 있는 주소와 포트들을 반환합니다.
func (this *TCPServer) ListenAddrPorts() []netip.AddrPort
```

### ClientHandler

ClientHandler의 콜백 함수는 모두 동일한 고루틴에서 호출되어 세션 이벤트 직렬화를 유지합니다.

erio는 **On{Event}(완료 콜백 함수) 구조와 같은 콜백에만 의존하는 구현 방식을 지양함**에 따라 사용자 선택 사항인 OnUserEvent, OnTimeout, OnReadClosed를 제외하고는 **기본/공백 구현 함수나 이벤트별 콜백함수**를 제공하지 않습니다.

* 사용자는 연결당 생성되는 ClientHandler 인터페이스를 구현해야 합니다.
* ClientHandler는 외부 고루틴에서 사용해서는 안됩니다.
  * 외부 고루틴에서 명령 전달이 필요한 경우 ClientHandler가 가진 HandlerContext를 안전하게 사용할 수 있습니다.
* ClientHandler는 오롯이 TCPReactor의 고루틴에서만 호출되어 집니다.

```Go
package erio

import (
	"github.com/netiogreem/erio/internal"
	"net/netip"
)

// FileDescriptor는 연결 소켓의 파일 디스크립터이며, internal.FileDescriptor(int32)의 별칭입니다.
type FileDescriptor = internal.FileDescriptor

// ClientHandlerFactory는 Acceptor가 수락한 연결마다 호출하는 ClientHandler 생성 함수입니다.
// fd는 연결된 FileDescriptor이고, listenAddress는 연결을 수락한 리스닝 주소입니다.
// 오류를 반환하거나 nil Handler를 반환하면 Acceptor가 연결을 거절합니다.
// 반환하는 ClientHandler는 항상 포인터(예: &MyHandler{...})여야 합니다.
// 포인터가 아니면 TCPReactor.RegisterHandler가 ErrTCPReactorNonPointerHandler로 거절하고,
// Acceptor가 연결을 닫습니다.
type ClientHandlerFactory func(fd FileDescriptor, listenAddress netip.AddrPort) (ClientHandler, error)

// ClientHandler는 TCPReactor에서 클라이언트 이벤트를 처리하는 사용자용 콜백 인터페이스입니다.
// 사용자는 HandlerContext를 임베딩한 핸들러를 구현합니다.
// 구현된 핸들러는 ClientHandlerFactory를 통해 TCPReactor에 등록되며,
// 등록된 핸들러는 소켓 이벤트가 발생하면 TCPReactor에 의해 해당하는 콜백이 호출됩니다.
type ClientHandler interface {
	// OnConnect는 연결 되었을 때 호출됩니다.
	//
	//   - context: 해당 연결의 HandlerContext
	OnConnect(context *HandlerContext)

	// OnRead는 데이터를 수신했을 때 호출됩니다.
	// 콜백 이후에도 데이터를 보관하려면 receivedData를 복사해야 합니다.
	//
	//   - context: 해당 연결의 HandlerContext
	//   - receivedData: 수신한 데이터
	OnRead(context *HandlerContext, receivedData []byte)

	// OnWritten은 Write로 요청한 데이터가 커널에 쓰기 완료되었을 때 호출됩니다.
	//
	//   - context: 해당 연결의 HandlerContext
	//   - streamID: Write 시 사용자가 정의한 streamID
	//   - writtenBytes: 쓰기 완료된 바이트 수
	OnWritten(context *HandlerContext, streamID int32, writtenBytes uint32)

	// OnUserEvent는 PostUserEvent로 접수한 사용자 이벤트 데이터를 처리합니다.
	// 사용자 선택사항이므로 HandlerContext에 빈메서드로 구현되어 있습니다.
	//
	//   - context: 해당 연결의 HandlerContext
	//   - userEventData: 사용자가 접수한 이벤트 데이터
	OnUserEvent(context *HandlerContext, userEventData any)

	// OnTimeout은 타이머가 만료되었을 때 호출됩니다.
	//
	//   - context: 해당 연결의 HandlerContext
	//   - timerKey: 만료된 타이머의 키
	OnTimeout(context *HandlerContext, timerKey uint64)

	// OnReadClosed는 상대방이 송신 측을 닫으면(half-close) 호출됩니다.
	// 연결은 계속 송신할 수 있고, 더 이상 데이터를 수신하지 않습니다.
	// HandlerContext가 Close를 호출하는 기본 구현을 제공하므로, 남은 데이터를 보낸 뒤 연결이 닫힙니다.
	// 연결을 유지하며 계속 송신하려면 이 메서드를 구현하고, 송신이 끝나면 Close를 호출합니다.
	//
	//   - context: 해당 연결의 HandlerContext
	OnReadClosed(context *HandlerContext)

	// OnError는 이벤트 처리 오류시 호출됩니다.
	//
	//   - context: 해당 연결의 HandlerContext
	//   - clientError: 발생한 오류
	//
	// TCPReactor가 등록을 처리하다 실패하면 OnError만 호출되고, OnConnect와 OnClose는 호출되지 않습니다.
	// TCPReactor.RegisterHandler가 ErrTCPReactorStopped 같은 오류를 반환하면 ClientHandler 콜백은
	// 호출되지 않고, 오류는 WithAcceptErrorCallback으로 설정한 콜백에 전달됩니다.
	// 송수신 오류가 나도 연결을 닫지 않습니다. 연결이 끊긴 경우는 OnClose(ErrTCPReactorHangup)로
	// 정리되며, 오류를 보고 연결을 끊으려면 Close나 AbortiveClose를 호출합니다.
	OnError(context *HandlerContext, clientError error)

	// OnClose는 연결이 끊어지고 Reactor에서 분리되고 난 후 호출됩니다.
	//
	//   - context: 해당 연결의 HandlerContext
	//   - closeReason: 연결 종료 사유
	//
	// closeReason은 연결이 닫히는 시점의 상황입니다.
	//   - ErrTCPReactorCloseRequested: Close가 연결을 닫았습니다. 기본 OnReadClosed가 호출한 Close도
	//     포함합니다. 남은 데이터를 보낸 뒤 닫지만, 쓰기 감시 등록에 실패하면 OnError를 호출하고
	//     남은 데이터를 보내지 않고 닫습니다.
	//   - ErrTCPReactorAbortiveCloseRequested: AbortiveClose가 RST로 연결을 닫았습니다.
	//   - ErrTCPReactorClosePendingWriteTimeout: Close 후 closePendingWriteTimeout 안에 남은 데이터를
	//     보내지 못해 RST로 연결을 닫았습니다.
	//   - ErrTCPReactorHangup: 연결이 끊겼습니다.
	//   - ErrTCPReactorStopped: TCPReactor가 중지되었습니다.
	OnClose(context *HandlerContext, closeReason error)

	// GetCommandQuota는 명령어 한도를 반환합니다.
	// HandlerContext의 구현된 함수입니다.
	//
	// 한도를 리턴합니다.
	GetCommandQuota() uint32

	// context는 Handler에 임베딩된 HandlerContext를 반환합니다.
	//
	// 해당 연결의 HandlerContext 포인터를 반환합니다.
	context() *HandlerContext
}
```

### HandlerContext

* ClientHandler에 임베딩하여 사용해야 합니다.
* HandlerContext는 ClientHandler를 제어하는 Type으로써 HandlerContext의 Init() and Reset() 메서드를 제외한 다른 메서드들은 외부 고루틴에서도 사용할 수 있는 thread-safe 메서드들 입니다.
* HandlerContext 메서드의 명령 요청(Write, SetTimeout, UnsetTimeout, Close, AbortiveClose, PostUserEvent) 호출은 이벤트 직렬화를 위해 TCPReactor로의 비동기 요청이며, 이 요청은 TCPReactor에서 처리되고 ClientHandler의 해당 함수들을 호출하여 줍니다.

```Go
// 클라이언트 TCP 연결을 위한 HandlerContext를 생성합니다.
func NewHandlerContext(handlerFD FileDescriptor, listenAddress netip.AddrPort,
	writeBufferSize uint32, commandQuota uint32, closePendingWriteTimeout time.Duration) (*HandlerContext, error)

// 기존 버퍼, commandQuota, closePendingWriteTimeout을 재활용하고 지정한 연결 정보로 생성 직후 상태를 만듭니다.
// 버퍼를 새로 할당하지 않습니다.
// Reactor에 등록하기 전(ClientHandlerFactory에서 반환하기 전),
// 또는 OnClose 안에서 마지막으로 호출해야 안전합니다
func (this *HandlerContext) Init(handlerFD FileDescriptor, listenAddress netip.AddrPort) error

// commandQuota와 closePendingWriteTimeout을 제외한 모든 상태를 비우고 handlerFD를 -1로 만듭니다.
// 버퍼는 용량을 유지합니다. handlerFD를 닫지 않으므로 연결이 닫힌 뒤 호출해야 합니다.
// Reactor에 등록하기 전(ClientHandlerFactory에서 반환하기 전),
// OnClose 안에서 마지막으로 호출해야 안전합니다.
func (this *HandlerContext) Reset() error

// 연결된 클라이언트 수를 반환합니다.
func (this *HandlerContext) Count() uint32

// 원격 피어의 주소와 포트를 반환합니다.
func (this *HandlerContext) PeerAddrPort() netip.AddrPort

// 연결을 수락한 리스닝 주소와 포트를 반환합니다.
func (this *HandlerContext) ListenAddrPort() netip.AddrPort

// 이 세션의 명령 한도(NewHandlerContext의 commandQuota)를 반환합니다.
func (this *HandlerContext) GetCommandQuota() uint32

// 데이터를 송신 버퍼에 복사하고 송신 명령을 요청합니다. streamID로 OnWritten에서 구분할 수 있습니다.
func (this *HandlerContext) Write(streamID int32, buffer []byte) error

// 지정한 키의 타이머 설정 또는 교체 명령을 요청합니다.
// 한번 호출로 지속적으로 OnTimeout을 호출 하는 것은 아닙니다. 1회성 입니다.
func (this *HandlerContext) SetTimeout(timerKey uint64, timeout time.Duration) error

// 지정한 키의 타이머 해제 명령을 요청합니다.
func (this *HandlerContext) UnsetTimeout(timerKey uint64) error

// 사용자 명령을 접수하여 해당 세션의 Reactor 고루틴에서 ClientHandler.OnUserEvent로 전달합니다.
func (this *HandlerContext) PostUserEvent(userEventData any) error

// 닫힌 상태를 먼저 기록하고 연결의 일반 종료를 요청합니다.
// Reactor는 수신을 멈추고, 호출 전에 Write한 데이터를 보낸 뒤 FIN으로 닫습니다.
// closePendingWriteTimeout 안에 보내지 못하면 RST로 닫습니다.
// 읽지 않은 수신 데이터가 남아 있으면 상대방은 FIN 대신 RST를 받습니다.
func (this *HandlerContext) Close() error

// 닫힌 상태를 먼저 기록하고 연결의 강제 종료(RST)를 요청합니다.
// OnWritten으로 완료를 알린 데이터라도 상대방에게 전달되지 않았으면 버려집니다.
// Close가 남은 데이터를 보내는 중에도 호출할 수 있습니다.
func (this *HandlerContext) AbortiveClose() error

// 세션이 닫힌 상태인지 확인합니다.
// Close 호출 후 남은 데이터를 보내는 중에도 true입니다.
func (this *HandlerContext) IsClosed() bool

// 상대방이 송신 측만 닫았는지(half-close) 확인합니다.
// true인 동안 세션은 계속 송신할 수 있고, 더 이상 데이터를 수신하지 않습니다.
func (this *HandlerContext) IsReadClosed() bool
```

아래와 같이 사용할 수 있습니다.

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

## 주의 사항

* **이벤트 콜백에서 오래 대기하지 마세요.**
  * 같은 Reactor에 배정된 ClientHandler 세션들은 하나의 고루틴에서 처리됩니다. 콜백 안에서 긴 연산, time.Sleep, 무한 재시도를 수행하면 해당 Reactor의 다른 세션도 처리가 지연됩니다. Long Transaction은 별개로 설계해야 합니다.
* **ClientHandler는 외부 고루틴에서 사용하지 마세요.**
  * ClientHandler는 외부 고루틴에 의해 호출되는 것을 보호하지 않습니다. 외부 고루틴에서는 HandlerContext가 제공하는 제어 메서드를 사용하세요.
* **HandlerContext의 Init() Reset()은 thread-safe가 아닙니다.**
  * 필요시 Reactor에 등록하기 전(ClientHandlerFactory에서 반환하기 전) 또는 OnClose 안에서 마지막으로 호출해야 합니다.
* **OnRead의 수신 버퍼를 보관하려면 복사하세요.**
  * OnRead의 receivedData는 재사용되는 내부 버퍼입니다. 콜백 이후 보관하거나 다른 고루틴에 전달하려면 복사해야 합니다. 반면 Write는 데이터를 내부 버퍼에 복사하므로 반환 후 원본 버퍼를 재사용할 수 있습니다.
* **OnWritten은 상대방의 수신 완료를 의미하지 않습니다.**
  * 해당 데이터가 로컬 커널에 모두 전달되었다는 의미입니다. 상대방의 수신이나 업무 처리 완료를 확인하려면 프로토콜 수준의 응답이 필요합니다.
* **Close는 남은 데이터를 다 보낸 뒤 닫습니다.**
  * Close()는 닫힌 상태를 기록하고 Reactor에 연결 종료를 요청합니다. Reactor는 HandlerContext 송신 버퍼에 남은 데이터를 보낸 뒤 FIN으로 닫고, ClientHandler.OnClose를 호출하여 줍니다.
  * 상대방이 데이터를 받지 않으면 연결이 남습니다. NewHandlerContext의 closePendingWriteTimeout을 설정하거나 AbortiveClose()를 호출하세요.