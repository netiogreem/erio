package internal

import (
	"sync"
	"time"
)

type Signal struct {
	mutex *sync.Mutex
	latch chan struct{}
}

func NewSignal() *Signal {
	return NewSignalWithMutex(&sync.Mutex{})
}

func NewSignalWithMutex(mutex *sync.Mutex) *Signal {
	return &Signal{
		mutex: mutex,
		latch: make(chan struct{}, 1),
	}
}

func (this *Signal) Signal() {
	this.SignalWith(nil)
}

func (this *Signal) SignalWith(callback func() error) (err error) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if callback != nil {
		err = callback()
	}

	select {
	case this.latch <- struct{}{}:
	default:
	}

	return err
}

func (this *Signal) Wait() {
	this.WaitWith(nil)
}

func (this *Signal) WaitWith(callback func()) {
	<-this.latch

	this.mutex.Lock()
	defer this.mutex.Unlock()

	if callback != nil {
		callback()
	}
}

func (this *Signal) WaitUntil(deadline time.Time) bool {
	select {
	case <-this.latch:
		return true
	default:
	}

	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case <-this.latch:
		return true
	case <-timer.C:
		return false
	}
}

func (this *Signal) WaitUntilWith(callback func(), deadline time.Time) bool {
	signaled := this.WaitUntil(deadline)

	this.mutex.Lock()
	defer this.mutex.Unlock()

	if callback != nil {
		callback()
	}

	return signaled
}
