package memtable

import (
	"os"
	"sync"

	rbt "github.com/emirpasic/gods/trees/redblacktree"

	"github.com/sosomasox/LSM-Tree-based-Storage/sstable"
)

type Tombstone struct{}

type MemTable struct {
	// read & write lock to control access to the in-memory tree.
	rwmu sync.RWMutex
	// the in-memory tree.
	tree *rbt.Tree
	size uint64
}

func New() *MemTable {
	return &MemTable{
		tree: rbt.NewWithStringComparator(),
	}
}

func (mt *MemTable) Clear() {
	mt.rwmu.Lock()
	defer mt.rwmu.Unlock()

	mt.tree.Clear()
	mt.size = 0
}

func (mt *MemTable) Put(key, value []byte) {
	mt.rwmu.Lock()
	defer mt.rwmu.Unlock()

	if val, found := mt.tree.Get(string(key)); !found {
		mt.size += uint64(len(key) + len(value))
	} else {
		if val == (Tombstone{}) {
			mt.size += uint64(len(value))
		} else {
			mt.size += uint64(len(value) - len(val.([]byte)))
		}
	}

	mt.tree.Put(string(key), value)
}

func (mt *MemTable) Get(key []byte) (value []byte, found, tombstone bool) {
	mt.rwmu.RLock()
	defer mt.rwmu.RUnlock()

	val, found := mt.tree.Get(string(key))

	if val == (Tombstone{}) {
		found = false
		value = []byte("")

		return value, found, true
	}

	if !found {
		value = []byte("")
	} else {
		value = val.([]byte)
	}

	return value, found, false
}

func (mt *MemTable) Del(key []byte) {
	mt.rwmu.Lock()
	defer mt.rwmu.Unlock()

	if value, found := mt.tree.Get(string(key)); found {
		if value != (Tombstone{}) {
			mt.size -= uint64(len(value.([]byte)))
		}
	}

	mt.tree.Put(string(key), Tombstone{})
}

func (mt *MemTable) Size() uint64 {
	mt.rwmu.RLock()
	defer mt.rwmu.RUnlock()

	return mt.size
}

func (mt *MemTable) Flush(idxfile, segfile *os.File) (*sstable.SSTable, error) {
	mt.rwmu.RLock()
	defer mt.rwmu.RUnlock()

	sst, err := sstable.New(idxfile, segfile)

	if err != nil {
		return nil, err
	}

	for it := mt.tree.Iterator(); it.Next(); {
		var value []byte
		var tombstone bool

		key := []byte(it.Node().Key.(string))
		val := it.Node().Value

		if val == (Tombstone{}) {
			tombstone = true
		} else {
			tombstone = false
			value = val.([]byte)
		}

		if err := sst.Index.Append(key, sst.Segment.Size()); err != nil {
			return nil, err
		}

		if err := sst.Segment.Append(key, value, tombstone); err != nil {
			return nil, err
		}
	}

	return sst, nil
}
