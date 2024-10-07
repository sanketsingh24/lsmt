package memtable

import (
	"bagh/value"
	"encoding/json"
	"sync/atomic"
)

type MemTable struct {
	// @p2: use a skiplist here instead :)
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
func (m *MemTable) Get(key []byte, seqno *value.SeqNo) *value.Value {

	parsedKey := &value.ParsedInternalKey{
		UserKey:   key,
		ValueType: value.Record,
		SeqNo:     *seqno,
	}

	v, _ := m.Items[parsedKey.String()]
	userValue := (value.UserValue)(v)
	foundValue := &value.Value{
		Key:       parsedKey.UserKey,
		Value:     userValue,
		ValueType: parsedKey.ValueType,
		SeqNo:     parsedKey.SeqNo,
	}
	return foundValue
}

// Iter returns all Items in the memtable as a slice of Value.
func (m *MemTable) Iter() ([]value.Value, error) {
	var result []value.Value
	for k, v := range m.Items {
		var parsedKey value.ParsedInternalKey
		if err := json.Unmarshal([]byte(k), &parsedKey); err != nil {
			return nil, err
		}
		result = append(result, value.Value{
			Key:       parsedKey.UserKey,
			Value:     value.UserValue(v),
			ValueType: parsedKey.ValueType,
			SeqNo:     parsedKey.SeqNo,
		})
	}
	return result, nil
}

// Range returns Items in the specified range. @TODO:
// func (m *MemTable) Range(lowerBound, upperBound segment.Bound[value.ParsedInternalKey]) ([]value.Value, error) {
// 	var result []value.Value
// 	for k, v := range m.Items {
// 		var parsedKey value.ParsedInternalKey
// 		if err := json.Unmarshal([]byte(k), &parsedKey); err != nil {
// 			return nil, err
// 		}
// 		if bytes.Compare(parsedKey.UserKey, lowerBound.UserKey) >= 0 && bytes.Compare(parsedKey.UserKey, upperBound.UserKey) < 0 {
// 			result = append(result, value.Value{
// 				Key:       parsedKey.UserKey,
// 				Value:     value.UserValue(v),
// 				ValueType: parsedKey.ValueType,
// 			})
// 		}
// 	}
// 	return result, nil
// }
/**
segmentLo := s.Metadata.KeyRange[0]
segmentHi := s.Metadata.KeyRange[1]

// If both bounds are unbounded, the range overlaps with everything
if lo.Unbounded == true && hi.Unbounded == true {
	return true
}

// If upper bound is unbounded
if hi.Unbounded == true {
	if lo.Unbounded == true {
		panic("Invalid key range check")
	}
	if lo.Included != nil {
		return bytes.Compare(*lo.Included, segmentHi) <= 0
	}
	return bytes.Compare(*lo.Excluded, segmentHi) < 0
}

// If lower bound is unbounded
if lo.Unbounded == true {
	if hi.Unbounded == true {
		panic("Invalid key range check")
	}
	if hi.Included != nil {
		return bytes.Compare(*hi.Included, segmentLo) >= 0
	}
	return bytes.Compare(*hi.Excluded, segmentLo) > 0
}

// Both bounds are bounded
loIncluded := false
if lo.Included != nil {
	loIncluded = bytes.Compare(*lo.Included, segmentHi) <= 0
} else {
	loIncluded = bytes.Compare(*lo.Excluded, segmentHi) < 0
}

hiIncluded := false
if hi.Included != nil {
	hiIncluded = bytes.Compare(*hi.Included, segmentLo) >= 0
} else {
	hiIncluded = bytes.Compare(*hi.Excluded, segmentLo) > 0
}

return loIncluded && hiIncluded

*/

// func (m *MemTable) Range(start, end segment.Bound[value.ParsedInternalKey]) *ranger.RangeIterator {

// 	keys := make([]string, 0, len(m.Items))
// 	for k := range m.Items {
// 		var parsedKey value.ParsedInternalKey
// 		if err := json.Unmarshal([]byte(k), &parsedKey); err != nil {
// 			return nil
// 		}

// 		if k >= start.UserKey && k <= end.UserKey {
// 			keys = append(keys, k)
// 		}
// 	}
// 	sort.Strings(keys)

// 	return &merge.Iterator{
// 		m:          m,
// 		keys:       keys,
// 		start:      start.UserKey,
// 		end:        end.UserKey,
// 		includeEnd: true, // Adjust this if you need to exclude the end key
// 	}
// }

// func (m *MemTable) RangeFrom(start segment.Bound[value.ParsedInternalKey]) *ranger.RangeIterator {
// 	return m.Range(start, segment.Bound[value.ParsedInternalKey]{
// 		Included: &value.ParsedInternalKey{
// 			UserKey: "\xff\xff\xff\xff",
// 		},
// 	}) // Max possible key
// }

// func (m *MemTable) RangeFull() *ranger.RangeIterator {
// 	return m.Range(ParsedInternalKey{UserKey: ""}, ParsedInternalKey{UserKey: "\xff\xff\xff\xff"})
// }

// Size returns the approximate size of the memtable in bytes.
func (m *MemTable) Size() uint32 {
	return m.ApproximateSize.Load()
}

// Len returns the number of Items in the memtable.
func (m *MemTable) Len() int {
	return len(m.Items)
}

// IsEmpty checks whether the memtable is empty.
func (m *MemTable) IsEmpty() bool {
	return len(m.Items) == 0
}

// Insert adds an item to the memtable and returns the new and old sizes.
func (m *MemTable) Insert(v value.Value) (uint32, uint32, error) {
	itemSize := uint32(len(v.Key) + len(v.Value))
	sizeAfter := m.ApproximateSize.Add(itemSize)
	key := &value.ParsedInternalKey{UserKey: v.Key, ValueType: v.ValueType, SeqNo: v.SeqNo}
	parsedKey, err := json.Marshal(key)
	if err != nil {
		return 0, 0, err
	}
	m.Items[string(parsedKey)] = v.Value
	return itemSize, sizeAfter, nil
}

func (m *MemTable) GetLSN() (*value.SeqNo, error) {
	var maxSeqNo value.SeqNo
	// m.Items.Range(func(k, _ interface{}) bool {
	// 	parsedKey := k.(ParsedInternalKey)
	// 	if parsedKey.SeqNo > maxSeqNo {
	// 		maxSeqNo = parsedKey.SeqNo
	// 	}
	// 	return true
	// })
	for k, _ := range m.Items {
		var parsedKey value.ParsedInternalKey
		if err := json.Unmarshal([]byte(k), &parsedKey); err != nil {
			return nil, err
		}
		if parsedKey.SeqNo > maxSeqNo {
			maxSeqNo = parsedKey.SeqNo
		}
	}
	return &maxSeqNo, nil
}

func (m *MemTable) Clone() (*MemTable, error) {
	newMemTable := NewMemTable()
	// @TODO: lmao copy by value here
	newMemTable.ApproximateSize = m.ApproximateSize

	for k, v := range m.Items {
		var parsedOldKey value.ParsedInternalKey
		if err := json.Unmarshal([]byte(k), &parsedOldKey); err != nil {
			return nil, err
		}
		newKey := value.ParsedInternalKey{
			UserKey:   parsedOldKey.UserKey,
			SeqNo:     parsedOldKey.SeqNo,
			ValueType: parsedOldKey.ValueType,
		}
		parsedKey, err := json.Marshal(newKey)
		if err != nil {
			return nil, err
		}

		newValue := make(value.UserValue, len(v))
		copy(newValue, v)
		newMemTable.Items[string(parsedKey)] = newValue
	}

	return newMemTable, nil
}
