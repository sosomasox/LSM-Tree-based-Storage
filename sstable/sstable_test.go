package sstable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSSTable(t *testing.T) {
	idxfile, err := os.CreateTemp("", "test_sstable_idxfile_")
	require.NoError(t, err)

	segfile, err := os.CreateTemp("", "test_sstable_segfile_")
	require.NoError(t, err)

	{
		for scenario, fn := range map[string]func(
			t *testing.T, sst *SSTable,
		){
			"Get": test_Get,
		} {
			fn := fn // https://github.com/golang/go/wiki/CommonMistakes
			t.Run(scenario, func(t *testing.T) {
				sst, err := New(idxfile, segfile)
				require.NoError(t, err)

				{
					key := "a"
					value := "A"
					tombstone := false

					err = sst.Index.Append([]byte(key), sst.Segment.Size())
					require.NoError(t, err)

					err := sst.Segment.Append([]byte(key), []byte(value), tombstone)
					require.NoError(t, err)
				}

				{
					key := "b"
					value := "BB"
					tombstone := false

					err = sst.Index.Append([]byte(key), sst.Segment.Size())
					require.NoError(t, err)

					err := sst.Segment.Append([]byte(key), []byte(value), tombstone)
					require.NoError(t, err)
				}

				{
					key := "c"
					value := "CCC"
					tombstone := false

					err = sst.Index.Append([]byte(key), sst.Segment.Size())
					require.NoError(t, err)

					err := sst.Segment.Append([]byte(key), []byte(value), tombstone)
					require.NoError(t, err)
				}

				{
					key := "d"
					value := ""
					tombstone := true

					err = sst.Index.Append([]byte(key), sst.Segment.Size())
					require.NoError(t, err)

					err := sst.Segment.Append([]byte(key), []byte(value), tombstone)
					require.NoError(t, err)

				}

				{
					key := "e"
					value := ""
					tombstone := true

					err = sst.Index.Append([]byte(key), sst.Segment.Size())
					require.NoError(t, err)

					err := sst.Segment.Append([]byte(key), []byte(value), tombstone)
					require.NoError(t, err)
				}

					fn(t, sst)
			})
		}
	}

}

func test_Get(t *testing.T, sst *SSTable) {

	{
		value, found, tombstone := sst.Get([]byte("a"))
		require.Equal(t, []byte("A"), value)
		require.Equal(t, true, found)
		require.Equal(t, false, tombstone)
	}

	{
		value, found, tombstone := sst.Get([]byte("b"))
		require.Equal(t, []byte("BB"), value)
		require.Equal(t, true, found)
		require.Equal(t, false, tombstone)
	}

	{
		value, found, tombstone := sst.Get([]byte("d"))
		require.Equal(t, []byte(""), value)
		require.Equal(t, false, found)
		require.Equal(t, true, tombstone)
	}

	{
		value, found, tombstone := sst.Get([]byte("c"))
		require.Equal(t, []byte("CCC"), value)
		require.Equal(t, true, found)
		require.Equal(t, false, tombstone)
	}

	{
		value, found, tombstone := sst.Get([]byte("e"))
		require.Equal(t, []byte(""), value)
		require.Equal(t, false, found)
		require.Equal(t, true, tombstone)
	}

}
