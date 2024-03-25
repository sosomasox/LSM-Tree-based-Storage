package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/sosomasox/LSM-Tree-based-Storage/memtable"
)

type OpeType uint8

const (
	OPE_DEL OpeType = iota
	OPE_PUT
)

const (
	OPETYPE_SIZE int = 1 // Byte
	KV_SIZE      int = 8 // Byte
	K_SIZE       int = 8 // Byte
	V_SIZE       int = 8 // Byte
)

var (
	enc = binary.BigEndian
)

type Recode struct {
	Ope   OpeType
	Key   []byte
	Value []byte
}

type WAL struct {
	rwmu sync.RWMutex
	file *os.File
	size uint64
}

func New(f *os.File) (*WAL, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: f,
		size: uint64(fi.Size()),
	}, nil
}

func Destroy(wal *WAL) error {
	wal.rwmu.RLock()
	fileName := wal.file.Name()
	wal.rwmu.RUnlock()

	if err := wal.Close(); err != nil {
		return err
	}

	if err := os.Remove(fileName); err != nil {
		return err
	}

	wal.file = nil
	wal.size = 0
	wal = nil

	return nil
}

func Recover(wal *WAL) (*memtable.MemTable, error) {
	mt := memtable.New()
	offset := int64(0)

	for {
		var ope OpeType
		var kvsize uint64
		var ksize uint64
		var vsize uint64
		var key []byte
		var value []byte

		// read ope type
		{
			opeBuf := make([]byte, OPETYPE_SIZE)

			n, err := wal.file.ReadAt(opeBuf, offset)
			if err != nil && err != io.EOF {
				return nil, err
			} else if err == io.EOF {
				break
			}
			ope = OpeType(opeBuf[0])

			offset += int64(n)
		}

		// read k/v size
		{
			kvsizeBuf := make([]byte, KV_SIZE)
			n, err := wal.file.ReadAt(kvsizeBuf, offset)
			if err != nil {
				return nil, err
			}

			offset += int64(n)

			if err := binary.Read(bytes.NewReader(kvsizeBuf), enc, &kvsize); err != nil {
				return nil, err
			}
		}

		// read key size
		{
			ksizeBuf := make([]byte, K_SIZE)
			n, err := wal.file.ReadAt(ksizeBuf, offset)
			if err != nil {
				return nil, err
			}

			offset += int64(n)

			if err := binary.Read(bytes.NewReader(ksizeBuf), enc, &ksize); err != nil {
				return nil, err
			}
		}

		// read value size
		{
			vsizeBuf := make([]byte, V_SIZE)
			n, err := wal.file.ReadAt(vsizeBuf, offset)
			if err != nil {
				return nil, err
			}

			offset += int64(n)

			if err := binary.Read(bytes.NewReader(vsizeBuf), enc, &vsize); err != nil {
				return nil, err
			}
		}

		// read key
		{
			keyBuf := make([]byte, ksize)
			n, err := wal.file.ReadAt(keyBuf, offset)
			if err != nil {
				return nil, err
			}
			key = keyBuf

			offset += int64(n)
		}

		// read value
		{
			valueBuf := make([]byte, vsize)
			n, err := wal.file.ReadAt(valueBuf, offset)
			if err != nil {
				return nil, err
			}
			value = valueBuf

			offset += int64(n)
		}

		switch ope {
		case OPE_PUT:
			mt.Put(key, value)
		case OPE_DEL:
			mt.Del(key)
		}
	}

	return mt, nil
}

func (wal *WAL) Append(recode Recode) error {
	wal.rwmu.Lock()
	defer wal.rwmu.Unlock()

	bw := bufio.NewWriter(wal.file)

	if err := binary.Write(bw, enc, uint8(recode.Ope)); err != nil {
		return err
	}

	wal.size += uint64(OPETYPE_SIZE)

	// write kvsize(key/value size)
	if err := binary.Write(bw, enc, uint64(len(recode.Key)+len(recode.Value))); err != nil {
		return err
	}

	wal.size += uint64(KV_SIZE)

	// write ksize(key size)
	if err := binary.Write(bw, enc, uint64(len(recode.Key))); err != nil {
		return err
	}

	wal.size += uint64(K_SIZE)

	// write vsize(value size)
	if err := binary.Write(bw, enc, uint64(len(recode.Value))); err != nil {
		return err
	}

	wal.size += uint64(V_SIZE)

	// write key
	if _, err := bw.Write(recode.Key); err != nil {
		return err
	}

	wal.size += uint64(len(recode.Key))

	// write value
	if _, err := bw.Write(recode.Value); err != nil {
		return err
	}

	wal.size += uint64(len(recode.Value))

	if err := bw.Flush(); err != nil {
		return err
	}

	if err := wal.file.Sync(); err != nil {
		return err
	}

	return nil
}

func (wal *WAL) Size() uint64 {
	wal.rwmu.RLock()
	defer wal.rwmu.RUnlock()

	return wal.size
}

func (wal *WAL) Close() error {
	wal.rwmu.Lock()
	defer wal.rwmu.Unlock()

	if err := wal.file.Sync(); err != nil {
		return err
	}

	wal.file.Close()
	wal.size = 0

	return nil
}
