package internal

import "sync"

type LifecycleStateType uint8

type lifecycleStateError string

func (this lifecycleStateError) Error() string {
	return string(this)
}

type LifecycleState struct {
	mutex sync.Mutex
	state LifecycleStateType
}

func NewLifecycleState(initState LifecycleStateType) *LifecycleState {
	return &LifecycleState{
		mutex: sync.Mutex{},
		state: initState,
	}
}

func (this *LifecycleState) Access(callback func(state *LifecycleStateType) error) error {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return callback(&this.state)
}
