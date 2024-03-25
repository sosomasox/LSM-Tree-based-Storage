package sstable

import (
	"os"
	"sync"
)

type TombstoneType uint8

const (
	NO_TOMBSTONE TombstoneType = iota
	TOMBSTONE
)

const (
	TOMBSTONE_SIZE int = 1 // Byte
	KV_SIZE        int = 8 // Byte
	K_SIZE         int = 8 // Byte
	V_SIZE         int = 8 // Byte
)

const (
	OFFSET_SIZE int = 8 // Byte
)

type SSTable struct {
	rwmu    sync.RWMutex
	Index   *Index
	Segment *Segment
}

func New(idxfile, segfile *os.File) (*SSTable, error) {
	index, err := newIndex(idxfile)
	if err != nil {
		return nil, err
	}

	segment, err := newSegment(segfile)
	if err != nil {
		return nil, err
	}

	sst := &SSTable{
		Index:   index,
		Segment: segment,
	}

	return sst, nil
}

func (sst *SSTable) Close() {
	sst.rwmu.Lock()
	defer sst.rwmu.Unlock()

	sst.Index.Close()
	sst.Segment.Close()
}

func (sst *SSTable) Get(key []byte) (value []byte, found, tombstone bool) {
	sst.rwmu.RLock()
	defer sst.rwmu.RUnlock()

	offset, found := sst.Index.Get([]byte(key))

	if !found {
		return []byte(""), found, false
	}

	_, value, found, tombstone = sst.Segment.Get(offset)

	return value, found, tombstone
}
