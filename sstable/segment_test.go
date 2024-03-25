package sstable

import (
	"bytes"
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	f, err := os.CreateTemp("", "test_segment_segfile_")
	require.NoError(t, err)

	seg, err := newSegment(f)
	require.NoError(t, err)

	for scenario, fn := range map[string]func(
		t *testing.T, seg *Segment,
	){
		"Append": test_segment_Append,
	} {
		fn := fn // https://github.com/golang/go/wiki/CommonMistakes
		t.Run(scenario, func(t *testing.T) {
			fn(t, seg)
		})
	}
}

func test_segment_Append(t *testing.T, seg *Segment) {
	key   := []byte("a")
	value := []byte("A")
	tombstone := false

	err := seg.Append(key, value, tombstone)
	require.NoError(t, err)

	f, size, err := openFile(seg.file.Name())
	defer f.Close()
	require.NoError(t, err)

	{
		actual_size   := size
		expected_size := uint64(TOMBSTONE_SIZE + KV_SIZE + K_SIZE + V_SIZE + len(key) + len(value))
		require.Equal(t, expected_size, actual_size)
	}

	{
		offset := int64(0)

		// check tombstone
		tombstoneBuf := make([]byte, TOMBSTONE_SIZE)
		n, err := f.ReadAt(tombstoneBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		tombstone := uint8(tombstoneBuf[0])
		expectedTombstone := uint8(0)
		require.Equal(t, expectedTombstone, tombstone)


		// check kvsize
		kvsizeBuf := make([]byte, KV_SIZE)
		n, err = f.ReadAt(kvsizeBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		var kvsize uint64
		expectedKVSize := uint64(2)
		err = binary.Read(bytes.NewReader(kvsizeBuf), enc, &kvsize)
		require.NoError(t, err)
		require.Equal(t, expectedKVSize, kvsize)


		// check ksize
		ksizeBuf := make([]byte, K_SIZE)
		n, err = f.ReadAt(ksizeBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		var ksize uint64
		expectedKSize := uint64(1)
		err = binary.Read(bytes.NewReader(ksizeBuf), enc, &ksize)
		require.NoError(t, err)
		require.Equal(t, expectedKSize, ksize)


		// check vsize
		vsizeBuf := make([]byte, V_SIZE)
		n, err = f.ReadAt(vsizeBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		var vsize uint64
		expectedVSize := uint64(1)
		err = binary.Read(bytes.NewReader(vsizeBuf), enc, &vsize)
		require.NoError(t, err)
		require.Equal(t, expectedVSize, vsize)


		//check key
		keyBuf := make([]byte, ksize)
		n, err = f.ReadAt(keyBuf, offset)
		require.NoError(t, err)

		offset += int64(n)

		key := string(keyBuf)
		expectedKey := "a"
		require.Equal(t, expectedKey, key)


		if tombstone == 0 {
			// check value
			valueBuf := make([]byte, vsize)
			n, err = f.ReadAt(valueBuf, offset)
			require.NoError(t, err)

			offset += int64(n)

			value := string(valueBuf)
			expectedValue := "A"
			require.Equal(t, expectedValue, value)
		}
	}

}

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
