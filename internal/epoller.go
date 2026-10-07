package internal

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"syscall"
	"time"
	"unsafe"
)

type FileDescriptor int32

type EpollEventMask uint32

type EpollEvent struct {
	FileDescriptor int32
	Events         uint32
}

type EpollerError string

func (this EpollerError) Error() string {
	return string(this)
}

const (
	ErrEpollerInvalidTimeout EpollerError = "erio: invalid epoll timeout"
	ErrEpollerClosed         EpollerError = "erio: epoller is closed or uninitialized"
	ErrEpollerNotRegistered  EpollerError = "erio: file descriptor is not registered"
)

const maxEpollEventBatchSize = uint32(math.MaxInt32 / unsafe.Sizeof(syscall.EpollEvent{}))

type Epoller struct {
	epollFD   FileDescriptor
	interests map[FileDescriptor]EpollEventMask
	events    []syscall.EpollEvent
}

func NewEpoller(eventBatchSize uint32) (*Epoller, error) {
	if eventBatchSize == 0 || eventBatchSize > maxEpollEventBatchSize {
		return nil, fmt.Errorf("epoll event batch size %d (valid range: 1..%d): %w",
			eventBatchSize, maxEpollEventBatchSize, syscall.EINVAL)
	}

	epoller := &Epoller{
		epollFD:   -1,
		interests: make(map[FileDescriptor]EpollEventMask),
		events:    make([]syscall.EpollEvent, eventBatchSize),
	}

	fd, createError := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if createError != nil {
		return nil, os.NewSyscallError("epoll_create1", createError)
	}

	epoller.epollFD = FileDescriptor(fd)
	return epoller, nil
}

func (this *Epoller) RegisterRead(fd FileDescriptor) error {
	return this.changeInterest(fd, syscall.EPOLLIN, 0)
}

func (this *Epoller) RegisterReadWithHangup(fd FileDescriptor) error {
	return this.changeInterest(fd, syscall.EPOLLIN|syscall.EPOLLRDHUP, 0)
}

func (this *Epoller) UnregisterRead(fd FileDescriptor) error {
	return this.changeInterest(fd, 0, syscall.EPOLLIN)
}

func (this *Epoller) RegisterWrite(fd FileDescriptor) error {
	return this.changeInterest(fd, syscall.EPOLLOUT, 0)
}

func (this *Epoller) UnregisterWrite(fd FileDescriptor) error {
	return this.changeInterest(fd, 0, syscall.EPOLLOUT)
}

func (this *Epoller) RegisterReadHangup(fd FileDescriptor) error {
	return this.changeInterest(fd, syscall.EPOLLRDHUP, 0)
}

func (this *Epoller) UnregisterReadHangup(fd FileDescriptor) error {
	return this.changeInterest(fd, 0, syscall.EPOLLRDHUP)
}

func (this *Epoller) changeInterest(fd FileDescriptor, addedEvents, removedEvents EpollEventMask) error {
	if this.interests == nil {
		return ErrEpollerClosed
	}

	currentEvents, contains := this.interests[fd]
	if !contains && removedEvents != 0 {
		return fmt.Errorf("%w: fd %d", ErrEpollerNotRegistered, fd)
	}
	nextEvents := (currentEvents | addedEvents) &^ removedEvents
	if contains && currentEvents == nextEvents {
		return nil
	}

	operation := syscall.EPOLL_CTL_MOD
	syscallName := "epoll_ctl mod"
	if !contains {
		operation = syscall.EPOLL_CTL_ADD
		syscallName = "epoll_ctl add"
	}

	kernelEvent := syscall.EpollEvent{Fd: int32(fd), Events: uint32(nextEvents)}

	if controlError := syscall.EpollCtl(int(this.epollFD), operation, int(fd), &kernelEvent); controlError != nil {
		return fmt.Errorf("fd %d: %w", fd, os.NewSyscallError(syscallName, controlError))
	}

	this.interests[fd] = nextEvents
	return nil
}

func (this *Epoller) Unregister(fd FileDescriptor) error {
	if this.interests == nil {
		return ErrEpollerClosed
	}

	if controlError := syscall.EpollCtl(int(this.epollFD), syscall.EPOLL_CTL_DEL, int(fd), nil); controlError != nil {
		return fmt.Errorf("fd %d: %w", fd, os.NewSyscallError("epoll_ctl del", controlError))
	}

	delete(this.interests, fd)
	return nil
}

func (this *Epoller) Wait(timeout time.Duration) ([]syscall.EpollEvent, error) {
	if this.interests == nil {
		return nil, ErrEpollerClosed
	}

	if timeout > time.Duration(math.MaxInt32)*time.Millisecond {
		return nil, fmt.Errorf("%w: %s exceeds %dms", ErrEpollerInvalidTimeout, timeout, math.MaxInt32)
	}

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}

	msec := 0
	for {
		var eventCount int
		var epollError error
		if msec == 0 {
			r, _, errno := syscall.RawSyscall6(syscall.SYS_EPOLL_PWAIT, uintptr(this.epollFD),
				uintptr(unsafe.Pointer(&this.events[0])), uintptr(len(this.events)), 0, 0, 0)
			eventCount = int(r)
			if errno != 0 {
				epollError = errno
			}
		} else {
			eventCount, epollError = syscall.EpollWait(int(this.epollFD), this.events, msec)
		}
		if epollError != nil && !errors.Is(epollError, syscall.EINTR) {
			return nil, os.NewSyscallError("epoll_wait", epollError)
		}

		if eventCount > 0 {
			return this.events[:eventCount:eventCount], nil
		}

		if timeout == 0 {
			return this.events[:0:0], nil
		}

		msec = -1
		if timeout > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return this.events[:0:0], nil
			}
			msec = int((remaining + time.Millisecond - 1) / time.Millisecond)
		}

		runtime.Gosched()
	}
}

func (this *Epoller) Close() error {
	if this.interests == nil {
		return nil
	}

	fd := this.epollFD
	this.epollFD = -1
	this.interests = nil
	this.events = nil
	if closeError := syscall.Close(int(fd)); closeError != nil {
		return fmt.Errorf("fd %d: %w", fd, os.NewSyscallError("close", closeError))
	}
	return nil
}
