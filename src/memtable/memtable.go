package memtable

import (
	"bagh/value"
	"bytes"
	"sync/atomic"
)

type MemTable struct {
	Items map[string]value.UserValue
	// Items           *sync.Map
	ApproximateSize atomic.Uint32
}

// NewMemTable creates a new MemTable.
func NewMemTable() *MemTable {
	return &MemTable{
		Items: *new(map[string]value.UserValue),
	}
}

// Get returns the item with the highest sequence number for the specified key.
// @P2: seqno
// func (m *MemTable) Get(key []byte, seqno *uint64) *Value {
func (m *MemTable) Get(key []byte) *value.Value {

	parsedKey := &value.ParsedInternalKey{
		UserKey:   key,
		ValueType: 0,
	}

	v, _ := m.Items.Load(parsedKey)
	userValue := v.(value.UserValue)
	foundValue := &value.Value{
		Key:       parsedKey.UserKey,
		Value:     userValue,
		ValueType: parsedKey.ValueType,
	}
	return foundValue
}

// Iter returns all Items in the memtable as a slice of Value.
func (m *MemTable) Iter() []value.Value {
	var result []value.Value
	m.Items.Range(func(key, value interface{}) bool {
		parsedKey := key.(value.ParsedInternalKey)
		userValue := value.(value.UserValue)
		result = append(result, value.Value{
			Key:       parsedKey.UserKey,
			Value:     userValue,
			ValueType: parsedKey.ValueType,
		})
		return true
	})
	return result
}

// Range returns Items in the specified range.
func (m *MemTable) Range(lowerBound, upperBound value.ParsedInternalKey) []value.Value {
	var result []value.Value
	m.Items.Range(func(key, value interface{}) bool {
		parsedKey := key.(value.ParsedInternalKey)
		if bytes.Compare(parsedKey.UserKey, lowerBound.UserKey) >= 0 && bytes.Compare(parsedKey.UserKey, upperBound.UserKey) < 0 {
			userValue := value.(value.UserValue)
			result = append(result, value.Value{
				Key:       parsedKey.UserKey,
				Value:     userValue,
				ValueType: parsedKey.ValueType,
			})
		}
		return true
	})
	return result
}

// Size returns the approximate size of the memtable in bytes.
func (m *MemTable) Size() uint32 {
	return m.ApproximateSize.Load()
}

// Len returns the number of Items in the memtable.
func (m *MemTable) Len() int {
	count := 0
	m.Items.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// IsEmpty checks whether the memtable is empty.
func (m *MemTable) IsEmpty() bool {
	m.Items.Range(func(_, _ interface{}) bool {
		return false
	})
	return true
}

// Insert adds an item to the memtable and returns the new and old sizes.
func (m *MemTable) Insert(value value.Value) (uint32, uint32) {
	itemSize := uint32(len(value.Key) + len(value.Value))
	sizeAfter := m.ApproximateSize.Add(itemSize)
	key := &value.ParsedInternalKey{UserKey: value.Key, ValueType: value.ValueType}
	m.Items.Store(key, value.Value)
	return itemSize, sizeAfter
}

// @P2
// func (m *MemTable) GetLSN() *uint64 {
// 	var maxSeqNo uint64
// 	m.Items.Range(func(k, _ interface{}) bool {
// 		parsedKey := k.(ParsedInternalKey)
// 		if parsedKey.SeqNo > maxSeqNo {
// 			maxSeqNo = parsedKey.SeqNo
// 		}
// 		return true
// 	})
// 	return &maxSeqNo
// }
