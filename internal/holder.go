package internal

import "sync/atomic"

type Holder[T any] struct {
	value atomic.Pointer[T]
}

func (this *Holder[T]) Snapshot() *T {
	return this.value.Load()
}

func (this *Holder[T]) Load() *T {
	return this.value.Load()
}

func (this *Holder[T]) Publish(value *T) {
	this.value.Store(value)
}

func (this *Holder[T]) Store(value *T) {
	this.value.Store(value)
}

func (this *Holder[T]) Clear() {
	this.value.Store(nil)
}
