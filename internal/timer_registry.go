package internal

import (
	"time"
)

type TimerRegistryError string

func (this TimerRegistryError) Error() string {
	return string(this)
}

const ErrTimerRegistryUninitialized TimerRegistryError = "erio: timer registry is uninitialized"

type timerRegistryEntry[K comparable] struct {
	expiresAt time.Time
	key       K
}

type TimerRegistry[K comparable] struct {
	entries  []timerRegistryEntry[K]
	position map[K]int
	expired  []K
}

func NewTimerRegistry[K comparable]() *TimerRegistry[K] {
	return &TimerRegistry[K]{
		position: make(map[K]int),
	}
}

func (this *TimerRegistry[K]) Register(expiresAt time.Time, key K) error {
	if this.position == nil {
		return ErrTimerRegistryUninitialized
	}

	if index, registered := this.position[key]; registered {
		previous := this.entries[index].expiresAt
		this.entries[index].expiresAt = expiresAt
		if expiresAt.Before(previous) {
			this.siftUp(index)
		} else {
			this.siftDown(index)
		}
		return nil
	}

	this.entries = append(this.entries, timerRegistryEntry[K]{expiresAt: expiresAt, key: key})
	index := len(this.entries) - 1
	this.position[key] = index
	this.siftUp(index)
	return nil
}

func (this *TimerRegistry[K]) Unregister(key K) bool {
	index, registered := this.position[key]
	if !registered {
		return false
	}

	last := len(this.entries) - 1
	if index != last {
		this.entries[index] = this.entries[last]
		this.position[this.entries[index].key] = index
	}
	this.entries = this.entries[:last]
	delete(this.position, key)

	if index != last {
		this.siftUp(index)
		this.siftDown(index)
	}
	return true
}

func (this *TimerRegistry[K]) NextExpiration() (expiresAt time.Time, hasTimer bool) {
	if len(this.entries) == 0 {
		return time.Time{}, false
	}
	return this.entries[0].expiresAt, true
}

func (this *TimerRegistry[K]) PopExpired(onExpired func(key K)) int {
	now := time.Now()
	for len(this.entries) > 0 && !this.entries[0].expiresAt.After(now) {
		key := this.entries[0].key
		last := len(this.entries) - 1
		this.entries[0] = this.entries[last]
		this.position[this.entries[0].key] = 0
		this.entries = this.entries[:last]
		delete(this.position, key)
		if last > 0 {
			this.siftDown(0)
		}
		this.expired = append(this.expired, key)
	}

	count := len(this.expired)
	if onExpired != nil {
		for _, key := range this.expired {
			onExpired(key)
		}
	}
	clear(this.expired)
	this.expired = this.expired[:0]
	return count
}

func (this *TimerRegistry[K]) siftUp(index int) {
	for index > 0 {
		parent := (index - 1) / 2
		if !this.entries[index].expiresAt.Before(this.entries[parent].expiresAt) {
			return
		}
		this.swap(index, parent)
		index = parent
	}
}

func (this *TimerRegistry[K]) siftDown(index int) {
	length := len(this.entries)
	for {
		smallest := index
		left := 2*index + 1
		if left < length && this.entries[left].expiresAt.Before(this.entries[smallest].expiresAt) {
			smallest = left
		}
		right := left + 1
		if right < length && this.entries[right].expiresAt.Before(this.entries[smallest].expiresAt) {
			smallest = right
		}
		if smallest == index {
			return
		}
		this.swap(index, smallest)
		index = smallest
	}
}

func (this *TimerRegistry[K]) swap(first, second int) {
	this.entries[first], this.entries[second] = this.entries[second], this.entries[first]
	this.position[this.entries[first].key] = first
	this.position[this.entries[second].key] = second
}
