package internal

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

func NewBufferDirector(bufferSize uint32) (*BufferDirector, error) {
	if bufferSize == 0 || uint64(bufferSize) > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("%w: %d", ErrBufferDirectorInvalidSize, bufferSize)
	}

	return &BufferDirector{
		consumeBuffer:  make([]byte, 0, int(bufferSize)),
		consumeMesgs:   make([]MessageInfo, 0, 16),
		consumeSize:    0,
		consumeMesgIdx: 0,
		prepareBuffer:  make([]byte, 0, int(bufferSize)),
		prepareMesgs:   make([]MessageInfo, 0, 16),
		prepareMutex:   sync.Mutex{},
		remainingSize:  atomic.Uint64{},
	}, nil
}

type BufferDirectorError string

func (this BufferDirectorError) Error() string {
	return string(this)
}

const (
	ErrBufferDirectorInvalidSize      BufferDirectorError = "erio: invalid write buffer size"
	ErrBufferDirectorUninitialized    BufferDirectorError = "erio: write buffer is uninitialized"
	ErrBufferDirectorFull             BufferDirectorError = "erio: write buffer is full"
	ErrBufferDirectorNilWrite         BufferDirectorError = "erio: write buffer consumer is nil"
	ErrBufferDirectorNilOnApprove     BufferDirectorError = "erio: write buffer approval callback is nil"
	ErrBufferDirectorNilOnConsumed    BufferDirectorError = "erio: write buffer consumed callback is nil"
	ErrBufferDirectorInvalidWriteSize BufferDirectorError = "erio: invalid write buffer consumed size"
)

type MessageInfo struct {
	ID     int32
	Length uint32
	remain uint32
}

type BufferDirector struct {
	consumeBuffer  []byte
	consumeMesgs   []MessageInfo
	consumeSize    uint32
	consumeMesgIdx uint32

	prepareBuffer []byte
	prepareMesgs  []MessageInfo
	prepareMutex  sync.Mutex

	// remainingSize is the number of bytes appended but not yet consumed in both buffers.
	// RemainingSize reads it without prepareMutex while the consumer runs.
	remainingSize atomic.Uint64
}

func (this *BufferDirector) Append(messageID int32, data []byte) error {
	dataSize := uint32(len(data))
	if dataSize == 0 {
		return nil
	}

	this.prepareMutex.Lock()
	defer this.prepareMutex.Unlock()

	if this.prepareBuffer == nil {
		return ErrBufferDirectorUninitialized
	}

	if len(data) > cap(this.prepareBuffer)-len(this.prepareBuffer) {
		return ErrBufferDirectorFull
	}

	this.prepareBuffer = append(this.prepareBuffer, data...)
	this.prepareMesgs = append(this.prepareMesgs, MessageInfo{
		ID:     messageID,
		Length: dataSize,
		remain: dataSize,
	})
	this.remainingSize.Add(uint64(dataSize))

	return nil
}

func (this *BufferDirector) AppendWith(messageID int32, data []byte, lockedOnApprove func() error) error {
	if lockedOnApprove == nil {
		return ErrBufferDirectorNilOnApprove
	}

	dataSize := uint32(len(data))
	if dataSize == 0 {
		return nil
	}

	this.prepareMutex.Lock()
	defer this.prepareMutex.Unlock()

	if this.prepareBuffer == nil {
		return ErrBufferDirectorUninitialized
	}

	if len(data) > cap(this.prepareBuffer)-len(this.prepareBuffer) {
		return ErrBufferDirectorFull
	}

	if err := lockedOnApprove(); err != nil {
		return err
	}

	this.prepareBuffer = append(this.prepareBuffer, data...)
	this.prepareMesgs = append(this.prepareMesgs, MessageInfo{
		ID:     messageID,
		Length: dataSize,
		remain: dataSize,
	})
	this.remainingSize.Add(uint64(dataSize))

	return nil
}

func (this *BufferDirector) ClearAppendBuffer() {
	this.prepareMutex.Lock()
	defer this.prepareMutex.Unlock()
	this.remainingSize.Add(-uint64(len(this.prepareBuffer)))
	this.prepareBuffer = this.prepareBuffer[:0]
	this.prepareMesgs = this.prepareMesgs[:0]
}

func (this *BufferDirector) Reset() error {
	if this.consumeBuffer == nil {
		return ErrBufferDirectorUninitialized
	}

	this.prepareMutex.Lock()
	defer this.prepareMutex.Unlock()

	this.consumeBuffer = this.consumeBuffer[:0]
	this.consumeMesgs = this.consumeMesgs[:0]
	this.consumeSize = 0
	this.consumeMesgIdx = 0
	this.prepareBuffer = this.prepareBuffer[:0]
	this.prepareMesgs = this.prepareMesgs[:0]
	this.remainingSize.Store(0)

	return nil
}

func (this *BufferDirector) RemainingSize() uint64 {
	if this == nil {
		return 0
	}
	return this.remainingSize.Load()
}

func (this *BufferDirector) ConsumeWith(
	consumer func(datas []byte, mesgs []MessageInfo) (consumedBytes uint32, err error),
	onConsumed func(info MessageInfo),
) (consumedBytes uint32, hasRemaining bool, err error) {
	if this.consumeBuffer == nil {
		return 0, false, ErrBufferDirectorUninitialized
	}
	if consumer == nil {
		return 0, false, ErrBufferDirectorNilWrite
	}
	if onConsumed == nil {
		return 0, false, ErrBufferDirectorNilOnConsumed
	}

	if this.swapPrepared() == false {
		return 0, false, nil
	}

	datas := this.consumeBuffer[this.consumeSize:len(this.consumeBuffer):len(this.consumeBuffer)]

	mesgs := this.consumeMesgs[this.consumeMesgIdx:len(this.consumeMesgs):len(this.consumeMesgs)]

	consumedBytes, consumerError := consumer(datas, mesgs)

	if consumedBytes > uint32(len(datas)) {
		consumerError = errors.Join(
			fmt.Errorf("%w: wrote %d of %d bytes",
				ErrBufferDirectorInvalidWriteSize, consumedBytes, len(datas)), consumerError)
		consumedBytes = uint32(len(datas))
	}

	this.remainingSize.Add(-uint64(consumedBytes))
	this.updateActiveMesgs(consumedBytes, onConsumed)
	return consumedBytes, this.swapPrepared(), consumerError
}

func (this *BufferDirector) updateActiveMesgs(consumedSize uint32, onConsumed func(info MessageInfo)) {
	this.consumeSize += consumedSize

	remainSize := consumedSize
	for this.consumeMesgIdx < uint32(len(this.consumeMesgs)) {
		info := &this.consumeMesgs[this.consumeMesgIdx]

		if remainSize < info.remain {
			info.remain -= remainSize
			break
		}

		remainSize -= info.remain
		info.remain = 0

		onConsumed(*info)
		this.consumeMesgIdx++
	}
}

func (this *BufferDirector) Consume() (datas []byte, mesgs []MessageInfo, hasRemaining bool, err error) {

	if this.consumeBuffer == nil {
		return nil, nil, false, ErrBufferDirectorUninitialized
	}

	if this.swapPrepared() == false {
		return nil, nil, false, nil
	}

	datas = this.consumeBuffer[this.consumeSize:len(this.consumeBuffer):len(this.consumeBuffer)]
	this.consumeSize += uint32(len(datas))
	this.remainingSize.Add(-uint64(len(datas)))

	mesgs = this.consumeMesgs[this.consumeMesgIdx:len(this.consumeMesgs):len(this.consumeMesgs)]
	this.consumeMesgIdx += uint32(len(mesgs))

	return datas, mesgs, this.swapPrepared(), nil
}

func (this *BufferDirector) swapPrepared() (hasRemaining bool) {
	if this.consumeSize < uint32(len(this.consumeBuffer)) {
		return true
	}

	this.prepareMutex.Lock()
	defer this.prepareMutex.Unlock()

	if len(this.prepareBuffer) == 0 {
		return false
	}

	this.consumeBuffer, this.prepareBuffer = this.prepareBuffer, this.consumeBuffer[:0]
	this.consumeMesgs, this.prepareMesgs = this.prepareMesgs, this.consumeMesgs[:0]
	this.consumeMesgIdx = 0
	this.consumeSize = 0

	return true
}
