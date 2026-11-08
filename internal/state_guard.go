package internal

import "sync"

type StateGuardType uint8

type lifecycleStateError string

func (this lifecycleStateError) Error() string {
	return string(this)
}

type StateGuard struct {
	mutex sync.Mutex
	state StateGuardType
}

func NewStateGuard(initState StateGuardType) *StateGuard {
	return &StateGuard{
		mutex: sync.Mutex{},
		state: initState,
	}
}

func (this *StateGuard) Access(callback func(state *StateGuardType) error) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return callback(&this.state)
}
