package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

type Segment struct {
	rwmu sync.RWMutex
	file *os.File
	size uint64
}

func newSegment(f *os.File) (*Segment, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &Segment{
		file: f,
		size: uint64(fi.Size()),
	}, nil
}

func (seg *Segment) Close() {
	seg.rwmu.Lock()
	defer seg.rwmu.Unlock()

	seg.file.Sync()
	seg.file.Close()
	seg.size = 0
}

func (seg *Segment) Append(key, value []byte, tombstone bool) error {
	seg.rwmu.Lock()
	defer seg.rwmu.Unlock()

	bw := bufio.NewWriter(seg.file)

	// write tombstone
	if tombstone {
		if err := binary.Write(bw, enc, TOMBSTONE); err != nil {
			//if err := binary.Write(bw, enc, uint8(TOMBSTONE)); err != nil {
			return err
		}
	} else {
		if err := binary.Write(bw, enc, NO_TOMBSTONE); err != nil {
			return err
		}
	}

	seg.size += uint64(TOMBSTONE_SIZE)

	// write kvsize(key/value size)
	if err := binary.Write(bw, enc, uint64(len(key)+len(value))); err != nil {
		return err
	}

	seg.size += uint64(KV_SIZE)

	// write ksize(key size)
	if err := binary.Write(bw, enc, uint64(len(key))); err != nil {
		return err
	}

	seg.size += uint64(K_SIZE)

	// write vsize(value size)
	if err := binary.Write(bw, enc, uint64(len(value))); err != nil {
		return err
	}

	seg.size += uint64(V_SIZE)

	// write key
	if _, err := bw.Write(key); err != nil {
		return err
	}

	seg.size += uint64(len(key))

	// write value
	if _, err := bw.Write(value); err != nil {
		return err
	}

	seg.size += uint64(len(value))

	if err := bw.Flush(); err != nil {
		return err
	}

	/*
		if err := seg.file.Sync(); err != nil {
			return err
		}
	*/

	return nil
}

func (seg *Segment) Get(offset uint64) (key, value []byte, found, tombstone bool) {
	tombstoneBuf := make([]byte, TOMBSTONE_SIZE)
	n, err := seg.file.ReadAt(tombstoneBuf, int64(offset))

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	offset += uint64(n)

	var ts TombstoneType
	err = binary.Read(bytes.NewReader(tombstoneBuf), enc, &ts)

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	kvsizeBuf := make([]byte, KV_SIZE)
	n, err = seg.file.ReadAt(kvsizeBuf, int64(offset))

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	offset += uint64(n)

	var kvsize uint64
	err = binary.Read(bytes.NewReader(kvsizeBuf), enc, &kvsize)

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	ksizeBuf := make([]byte, K_SIZE)
	n, err = seg.file.ReadAt(ksizeBuf, int64(offset))

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	offset += uint64(n)

	var ksize uint64
	err = binary.Read(bytes.NewReader(ksizeBuf), enc, &ksize)

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	vsizeBuf := make([]byte, V_SIZE)
	n, err = seg.file.ReadAt(vsizeBuf, int64(offset))

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	offset += uint64(n)

	var vsize uint64
	err = binary.Read(bytes.NewReader(vsizeBuf), enc, &vsize)
	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	keyBuf := make([]byte, ksize)
	n, err = seg.file.ReadAt(keyBuf, int64(offset))

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	offset += uint64(n)

	key = keyBuf

	if ts == TOMBSTONE {
		return key, []byte(""), false, true
	}

	valueBuf := make([]byte, vsize)
	_, err = seg.file.ReadAt(valueBuf, int64(offset))

	if err != nil {
		return []byte(""), []byte(""), false, false
	}

	value = valueBuf

	return key, value, true, false
}

func (seg *Segment) Size() uint64 {
	seg.rwmu.RLock()
	defer seg.rwmu.RUnlock()

	return seg.size
}

func (seg *Segment) Sync() error {
	return seg.file.Sync()
}
