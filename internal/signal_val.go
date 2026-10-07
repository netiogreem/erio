package internal

import "sync"

type signalT[T any] struct {
	channel chan struct{}
	value   T
}

type SignalVal[T any] struct {
	mutex sync.Mutex
	value *signalT[T]
}

func (this *SignalVal[T]) Broadcast() {
	var value T
	this.BroadcastWith(value)
}

func (this *SignalVal[T]) BroadcastWith(value T) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.value == nil {
		this.value = &signalT[T]{channel: make(chan struct{})}
	}

	select {
	case <-this.value.channel:
	default:
		this.value.value = value
		close(this.value.channel)
	}
}

func (this *SignalVal[T]) Wait() T {
	this.mutex.Lock()
	if this.value == nil {
		this.value = &signalT[T]{channel: make(chan struct{})}
	}

	value := this.value
	this.mutex.Unlock()

	<-value.channel
	return value.value
}

func (this *SignalVal[T]) Reset() {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if this.value == nil {
		return
	}

	select {
	case <-this.value.channel:
		this.value = &signalT[T]{channel: make(chan struct{})}
	default:
	}
}
