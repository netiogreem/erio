package internal

import (
	"time"

	"github.com/emirpasic/gods/maps/treemap"
)

type TimerRegistryError string

func (this TimerRegistryError) Error() string {
	return string(this)
}

const ErrTimerRegistryUninitialized TimerRegistryError = "erio: timer registry is uninitialized"

type TimerRegistry struct {
	timerKeysByTime *treemap.Map
	timeByTimerKey  map[uint64]time.Time
}

func NewTimerRegistry() *TimerRegistry {
	return &TimerRegistry{
		timerKeysByTime: treemap.NewWith(func(first, second interface{}) int {
			return first.(time.Time).Compare(second.(time.Time))
		}),
		timeByTimerKey: make(map[uint64]time.Time),
	}
}

func (this *TimerRegistry) Register(expiresAt time.Time, timerKey uint64) error {
	if this.timerKeysByTime == nil || this.timeByTimerKey == nil {
		return ErrTimerRegistryUninitialized
	}
	this.Unregister(timerKey)
	value, found := this.timerKeysByTime.Get(expiresAt)
	var timerKeys map[uint64]struct{}
	if found {
		timerKeys = value.(map[uint64]struct{})
	} else {
		timerKeys = make(map[uint64]struct{})
		this.timerKeysByTime.Put(expiresAt, timerKeys)
	}
	timerKeys[timerKey] = struct{}{}
	this.timeByTimerKey[timerKey] = expiresAt
	return nil
}

func (this *TimerRegistry) Unregister(timerKey uint64) bool {
	expiresAt, registered := this.timeByTimerKey[timerKey]
	if !registered {
		return false
	}
	value, _ := this.timerKeysByTime.Get(expiresAt)
	timerKeys := value.(map[uint64]struct{})
	delete(timerKeys, timerKey)
	if len(timerKeys) == 0 {
		this.timerKeysByTime.Remove(expiresAt)
	}
	delete(this.timeByTimerKey, timerKey)
	return true
}

func (this *TimerRegistry) NextExpiration() (expiresAt time.Time, hasTimer bool) {
	if len(this.timeByTimerKey) == 0 {
		return time.Time{}, false
	}
	value, _ := this.timerKeysByTime.Min()
	return value.(time.Time), true
}

func (this *TimerRegistry) PopExpired() []uint64 {
	if this.timerKeysByTime == nil {
		return nil
	}
	cutoff := time.Now()
	var expired []uint64
	for !this.timerKeysByTime.Empty() {
		expiresAt, value := this.timerKeysByTime.Min()
		if expiresAt.(time.Time).After(cutoff) {
			break
		}
		for timerKey := range value.(map[uint64]struct{}) {
			expired = append(expired, timerKey)
			delete(this.timeByTimerKey, timerKey)
		}
		this.timerKeysByTime.Remove(expiresAt)
	}
	return expired
}
