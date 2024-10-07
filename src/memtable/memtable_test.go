package memtable_test

import (
	"bagh/memtable"
	"bagh/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemtableGet(t *testing.T) {
	memtable := memtable.NewMemTable()

	val := value.NewValue([]byte("abc"), []byte("abc"), 0, value.Record)

	memtable.Insert(*val)

	assert.Equal(t, &val, memtable.Get([]byte("abc"), nil))
}

func TestMemtableGetHighestSeqno(t *testing.T) {
	memtable := memtable.NewMemTable()

	for i := value.SeqNo(0); i <= 4; i++ {
		memtable.Insert(*value.NewValue([]byte("abc"), []byte("abc"), i, value.Record))
	}

	expected := value.NewValue([]byte("abc"), []byte("abc"), 4, value.Record)
	assert.Equal(t, &expected, memtable.Get([]byte("abc"), nil))
}

func TestMemtableGetPrefix(t *testing.T) {
	memtable := memtable.NewMemTable()

	memtable.Insert(*value.NewValue([]byte("abc0"), []byte("abc"), 0, value.Record))
	memtable.Insert(*value.NewValue([]byte("abc"), []byte("abc"), 255, value.Record))

	expected1 := value.NewValue([]byte("abc"), []byte("abc"), 255, value.Record)
	assert.Equal(t, &expected1, memtable.Get([]byte("abc"), nil))

	expected2 := value.NewValue([]byte("abc0"), []byte("abc"), 0, value.Record)
	assert.Equal(t, &expected2, memtable.Get([]byte("abc0"), nil))
}

func TestMemtableGetOldVersion(t *testing.T) {
	memtable := memtable.NewMemTable()

	memtable.Insert(*value.NewValue([]byte("abc"), []byte("abc"), 0, value.Record))
	memtable.Insert(*value.NewValue([]byte("abc"), []byte("abc"), 99, value.Record))
	memtable.Insert(*value.NewValue([]byte("abc"), []byte("abc"), 255, value.Record))

	expected1 := value.NewValue([]byte("abc"), []byte("abc"), 255, value.Record)
	assert.Equal(t, &expected1, memtable.Get([]byte("abc"), nil))

	seqNo100 := value.SeqNo(100)
	expected2 := value.NewValue([]byte("abc"), []byte("abc"), 99, value.Record)
	assert.Equal(t, &expected2, memtable.Get([]byte("abc"), &seqNo100))

	seqNo50 := value.SeqNo(50)
	expected3 := value.NewValue([]byte("abc"), []byte("abc"), 0, value.Record)
	assert.Equal(t, &expected3, memtable.Get([]byte("abc"), &seqNo50))
}
