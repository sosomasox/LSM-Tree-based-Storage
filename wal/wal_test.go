package wal

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWal(t *testing.T) {
	f, err := os.CreateTemp("", "test_wal_walfile_")
	require.NoError(t, err)

	wal, err := New(f)
	require.NoError(t, err)

	for scenario, fn := range map[string]func(
		t *testing.T, wal *WAL,
	){
		"Append": test_wal_Append,
	} {
		fn := fn // https://github.com/golang/go/wiki/CommonMistakes
		t.Run(scenario, func(t *testing.T) {
			fn(t, wal)
		})
	}

	t.Run("Close", func(t *testing.T) {
		test_wal_Close(t, wal)
	})

}

func test_wal_Close(t *testing.T, wal *WAL) {
	err := wal.Close()
	require.NoError(t, err)
	require.Equal(t, uint64(0), wal.size)
}

func test_wal_Append(t *testing.T, wal *WAL) {
	require.NoError(t, wal.Append(Recode{Ope: OPE_PUT, Key: []byte("a"), Value: []byte("A")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_PUT, Key: []byte("b"), Value: []byte("BB")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_PUT, Key: []byte("c"), Value: []byte("CCC")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_PUT, Key: []byte("f"), Value: []byte("FFFFF")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_PUT, Key: []byte("g"), Value: []byte("GGGGGG")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_DEL, Key: []byte("a")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_DEL, Key: []byte("c"), Value: []byte("")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_DEL, Key: []byte("d")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_DEL, Key: []byte("e"), Value: []byte("")}))
	require.NoError(t, wal.Append(Recode{Ope: OPE_PUT, Key: []byte("z"), Value: []byte("")}))

	fn := func(offset int64, expectedOpe OpeType, expectedKVSize, expectedKSize, expectedVSize uint64, expectedKey, expectedValue []byte) int64 {
		opeBuf := make([]byte, OPETYPE_SIZE)
		n, err := wal.file.ReadAt(opeBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		ope := OpeType(opeBuf[0])
		require.Equal(t, expectedOpe, ope)

		kvsizeBuf := make([]byte, KV_SIZE)
		n, err = wal.file.ReadAt(kvsizeBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		var kvsize uint64
		err = binary.Read(bytes.NewReader(kvsizeBuf), enc, &kvsize)
		require.NoError(t, err)
		require.Equal(t, expectedKVSize, kvsize)

		ksizeBuf := make([]byte, K_SIZE)
		n, err = wal.file.ReadAt(ksizeBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		var ksize uint64
		err = binary.Read(bytes.NewReader(ksizeBuf), enc, &ksize)
		require.NoError(t, err)
		require.Equal(t, expectedKSize, ksize)

		vsizeBuf := make([]byte, V_SIZE)
		n, err = wal.file.ReadAt(vsizeBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		var vsize uint64
		err = binary.Read(bytes.NewReader(vsizeBuf), enc, &vsize)
		require.NoError(t, err)
		require.Equal(t, expectedVSize, vsize)

		keyBuf := make([]byte, ksize)
		n, err = wal.file.ReadAt(keyBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		key := keyBuf
		require.Equal(t, expectedKey, key)

		valueBuf := make([]byte, vsize)
		n, err = wal.file.ReadAt(valueBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		value := valueBuf
		require.Equal(t, expectedValue, value)

		return offset
	}

	var offset int64
	offset = fn(offset, OPE_PUT, uint64(2), uint64(1), uint64(1), []byte("a"), []byte("A"))
	offset = fn(offset, OPE_PUT, uint64(3), uint64(1), uint64(2), []byte("b"), []byte("BB"))
	offset = fn(offset, OPE_PUT, uint64(4), uint64(1), uint64(3), []byte("c"), []byte("CCC"))
	offset = fn(offset, OPE_PUT, uint64(6), uint64(1), uint64(5), []byte("f"), []byte("FFFFF"))
	offset = fn(offset, OPE_PUT, uint64(7), uint64(1), uint64(6), []byte("g"), []byte("GGGGGG"))
	offset = fn(offset, OPE_DEL, uint64(1), uint64(1), uint64(0), []byte("a"), []byte(""))
	offset = fn(offset, OPE_DEL, uint64(1), uint64(1), uint64(0), []byte("c"), []byte(""))
	offset = fn(offset, OPE_DEL, uint64(1), uint64(1), uint64(0), []byte("d"), []byte(""))
	offset = fn(offset, OPE_DEL, uint64(1), uint64(1), uint64(0), []byte("e"), []byte(""))
	offset = fn(offset, OPE_PUT, uint64(1), uint64(1), uint64(0), []byte("z"), []byte(""))

	buf := make([]byte, 1)
	_, err := wal.file.ReadAt(buf, offset)
	require.Equal(t, io.EOF, err)
}

/*
func openFile(name string) (file *os.File, size uint64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0600,
	)

	if err != nil {
		return nil, 0, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}

	return f, uint64(fi.Size()), nil
}
*/
