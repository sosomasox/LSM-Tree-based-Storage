package memtable

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemTable(t *testing.T) {
	mt := New()

	for scenario, fn := range map[string]func(
		t *testing.T, mt *MemTable,
	){
		"Put":         test_Put,
		"Get":         test_Get,
		"Del":         test_Del,
		"Put/Get/Del": test_PutGetDel,
	} {
		fn := fn // https://github.com/golang/go/wiki/CommonMistakes
		t.Run(scenario, func(t *testing.T) {
			fn(t, mt)
		})
	}

	t.Run("Clear", func(t *testing.T) {
		test_Clear(t, mt)
	})

}

func test_Clear(t *testing.T, mt *MemTable) {
	mt.Clear()

	require.Equal(t, int(0), mt.tree.Size())
	require.Equal(t, uint64(0), mt.size)
}

func test_Put(t *testing.T, mt *MemTable) {
	mt.Put([]byte("test"), []byte("test"))
	mt.Put([]byte("void"), []byte(""))
	mt.Put([]byte("a"), []byte(strings.Repeat("a", 4*1024)))

	{

		mt.rwmu.RLock()
		value, found := mt.tree.Get("test")
		mt.rwmu.RUnlock()

		actualValue := value
		expectedValue := []byte("test")
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)
	}

	{

		mt.rwmu.RLock()
		value, found := mt.tree.Get("void")
		mt.rwmu.RUnlock()

		actualValue := value
		expectedValue := []byte("")
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)

	}

	{

		mt.rwmu.RLock()
		value, found := mt.tree.Get("a")
		mt.rwmu.RUnlock()

		actualValue := value
		expectedValue := []byte(strings.Repeat("a", 4*1024))
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)
	}

}

func test_Get(t *testing.T, mt *MemTable) {
	mt.rwmu.Lock()
	mt.tree.Put("test", []byte("test"))
	mt.rwmu.Unlock()

	{

		value, found, tombstone := mt.Get([]byte("test"))

		actualValue := value
		expectedValue := []byte("test")
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)

		actualTombstone := tombstone
		expectedTombstone := false
		require.Equal(t, expectedTombstone, actualTombstone)
	}

	{
		value, found, tombstone := mt.Get([]byte("no-entry"))

		actualValue := value
		expectedValue := []byte("")
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := false
		require.Equal(t, expectedFound, actualFound)

		actualTombstone := tombstone
		expectedTombstone := false
		require.Equal(t, expectedTombstone, actualTombstone)
	}
}

func test_Del(t *testing.T, mt *MemTable) {
	mt.rwmu.Lock()
	mt.tree.Put("test", []byte("test"))
	mt.rwmu.Unlock()

	// delete key
	{
		mt.Del([]byte("test"))

		mt.rwmu.RLock()
		value, found := mt.tree.Get("test")
		mt.rwmu.RUnlock()

		require.Equal(t, Tombstone{}, value)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)
	}

	// already deleted key
	{
		mt.Del([]byte("test"))

		mt.rwmu.RLock()
		value, found := mt.tree.Get("test")
		mt.rwmu.RUnlock()

		require.Equal(t, Tombstone{}, value)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)
	}

	// delte non-key
	{
		mt.Del([]byte("no-entry"))

		mt.rwmu.RLock()
		value, found := mt.tree.Get("no-entry")
		mt.rwmu.RUnlock()

		require.Equal(t, Tombstone{}, value)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)
	}

}

func test_PutGetDel(t *testing.T, mt *MemTable) {
	mt.Put([]byte("test"), []byte("test"))

	{
		value, found, tombstone := mt.Get([]byte("test"))

		actualValue := value
		expectedValue := []byte("test")
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := true
		require.Equal(t, expectedFound, actualFound)

		actualTombstone := tombstone
		expectedTombstone := false
		require.Equal(t, expectedTombstone, actualTombstone)
	}

	mt.Del([]byte("test"))

	{
		value, found, tombstone := mt.Get([]byte("test"))

		actualValue := value
		expectedValue := []byte("")
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := false
		require.Equal(t, expectedFound, actualFound)

		actualTombstone := tombstone
		expectedTombstone := true
		require.Equal(t, expectedTombstone, actualTombstone)
	}

	mt.Del([]byte("no-entry"))

	{
		value, found, tombstone := mt.Get([]byte("no-entry"))

		actualValue := value
		expectedValue := []byte("")
		require.Equal(t, expectedValue, actualValue)

		actualFound := found
		expectedFound := false
		require.Equal(t, expectedFound, actualFound)

		actualTombstone := tombstone
		expectedTombstone := true
		require.Equal(t, expectedTombstone, actualTombstone)
	}

}
