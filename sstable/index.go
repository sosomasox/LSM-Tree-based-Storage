package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type Iterator struct {
	index  *Index
	offset int64
}

func (itr *Iterator) HasNext() bool {
	ksizeBuf := make([]byte, K_SIZE)
	if _, err := itr.index.file.ReadAt(ksizeBuf, itr.offset); err != nil {
		return false
	}

	return true
}

func (itr *Iterator) Next() (key []byte, value uint64, err error) {
	var ksize uint64
	ksizeBuf := make([]byte, K_SIZE)

	n, err := itr.index.file.ReadAt(ksizeBuf, itr.offset)
	if err != nil {
		return []byte(""), uint64(0), err
	}

	itr.offset += int64(n)

	if err := binary.Read(bytes.NewReader(ksizeBuf), enc, &ksize); err != nil {
		return []byte(""), uint64(0), err
	}

	keyBuf := make([]byte, ksize)

	n, err = itr.index.file.ReadAt(keyBuf, itr.offset)
	if err != nil {
		return []byte(""), uint64(0), err
	}

	itr.offset += int64(n)

	offsetSizeBuf := make([]byte, OFFSET_SIZE)

	n, err = itr.index.file.ReadAt(offsetSizeBuf, itr.offset)
	if err != nil {
		return []byte(""), uint64(0), err
	}

	itr.offset += int64(n)

	key = []byte(string(keyBuf))
	value, _ = itr.index.Get(key)

	return key, value, nil
}

type Index struct {
	rwmu    sync.RWMutex
	file    *os.File
	HashMap map[string]uint64
	size    uint64
}

func newIndex(f *os.File) (*Index, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	if fi.Size() == 0 {
		return buildIndex(f), nil
	}

	return rebuildIndex(f)
}

func buildIndex(f *os.File) *Index {
	return &Index{
		file:    f,
		HashMap: make(map[string]uint64),
	}
}

func rebuildIndex(f *os.File) (*Index, error) {
	hashmap := make(map[string]uint64)
	offset := int64(0)

	for {
		var ksize uint64
		ksizeBuf := make([]byte, K_SIZE)

		n, err := f.ReadAt(ksizeBuf, offset)
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}

		offset += int64(n)

		if err := binary.Read(bytes.NewReader(ksizeBuf), enc, &ksize); err != nil {
			return nil, err
		}

		keyBuf := make([]byte, ksize)

		n, err = f.ReadAt(keyBuf, offset)
		if err != nil {
			return nil, err
		}

		offset += int64(n)

		offsetSizeBuf := make([]byte, OFFSET_SIZE)

		n, err = f.ReadAt(offsetSizeBuf, offset)
		if err != nil {
			return nil, err
		}

		offset += int64(n)

		{
			var offset uint64
			key := string(keyBuf)

			if err = binary.Read(bytes.NewReader(offsetSizeBuf), enc, &offset); err != nil {
				return nil, err
			}

			hashmap[key] = offset
		}
	}

	return &Index{
		file:    f,
		HashMap: hashmap,
		size:    uint64(len(hashmap)),
	}, nil
}

func (idx *Index) Iterator() Iterator {
	return Iterator{index: idx}
}

func (idx *Index) Close() {
	idx.rwmu.Lock()
	defer idx.rwmu.Unlock()

	idx.file.Sync()
	idx.file.Close()
	idx.HashMap = make(map[string]uint64)
	idx.size = 0
}

func (idx *Index) Append(key []byte, offset uint64) error {
	idx.rwmu.Lock()
	defer idx.rwmu.Unlock()

	bw := bufio.NewWriter(idx.file)

	idx.HashMap[string(key)] = offset
	idx.size += 1

	// write ksize(key size)
	if err := binary.Write(bw, enc, uint64(len(key))); err != nil {
		return err
	}

	// write key
	if _, err := bw.Write(key); err != nil {
		return err
	}

	// write offset
	if err := binary.Write(bw, enc, offset); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	/*
		if err := idx.file.Sync(); err != nil {
			return err
		}
	*/

	return nil
}

func (idx *Index) Get(key []byte) (offset uint64, found bool) {
	idx.rwmu.RLock()
	defer idx.rwmu.RUnlock()

	offset, found = idx.HashMap[string(key)]

	return offset, found
}

func (idx *Index) Size() uint64 {
	idx.rwmu.RLock()
	defer idx.rwmu.RUnlock()

	return idx.size
}

func (idx *Index) Sync() error {
	return idx.file.Sync()
}
