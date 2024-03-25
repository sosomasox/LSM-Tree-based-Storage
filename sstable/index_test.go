package sstable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	idxfile, err := os.CreateTemp("", "test_index_idxfile_")
	require.NoError(t, err)

	idx, err := newIndex(idxfile)
	require.NoError(t, err)

	for scenario, fn := range map[string]func(
		t *testing.T, idx *Index,
	){
		"Append": test_index_Append,
	} {
		fn := fn // https://github.com/golang/go/wiki/CommonMistakes
		t.Run(scenario, func(t *testing.T) {
			fn(t, idx)
		})
	}

	t.Run("rebuildIndex", func(t *testing.T) {
		test_rebuildIndex(t, idxfile)
	})

	t.Run("Iterator", func(t *testing.T) {
		test_Iterator(t, idx)
	})
}

func test_Iterator(t *testing.T, idx *Index) {
	itr := idx.Iterator()

	require.Equal(t, true, itr.HasNext())

	{
		key, value, err := itr.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("a"), key)
		require.Equal(t, uint64(0), value)
	}

	require.Equal(t, true, itr.HasNext())

	{
		key, value, err := itr.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("b"), key)
		require.Equal(t, uint64(10), value)
	}

	require.Equal(t, true, itr.HasNext())

	{
		key, value, err := itr.Next()
		require.NoError(t, err)
		require.Equal(t, []byte("c"), key)
		require.Equal(t, uint64(15), value)
	}

	require.Equal(t, false, itr.HasNext())
	{
		key, value, err := itr.Next()
		require.Error(t, err)
		require.Equal(t, []byte(""), key)
		require.Equal(t, uint64(0), value)
	}

}

func test_rebuildIndex(t *testing.T, idxfile *os.File) {
	idx, err := rebuildIndex(idxfile)
	require.NoError(t, err)

	require.Equal(t, uint64(0), idx.HashMap[string([]byte("a"))])
	require.Equal(t, uint64(10), idx.HashMap[string([]byte("b"))])
	require.Equal(t, uint64(15), idx.HashMap[string([]byte("c"))])
}

func test_index_Append(t *testing.T, idx *Index) {

	{
		key := []byte("a")
		offset := uint64(0)

		err := idx.Append(key, offset)
		require.NoError(t, err)
	}

	{
		key := []byte("b")
		offset := uint64(10)

		err := idx.Append(key, offset)
		require.NoError(t, err)
	}

	{
		key := []byte("c")
		offset := uint64(15)

		err := idx.Append(key, offset)
		require.NoError(t, err)
	}

}
