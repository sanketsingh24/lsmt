package prefix

import (
	"bagh/memtable"
	"bagh/merge"
	"bagh/ranger"
	"bagh/segment"
	"bagh/value"
	"bytes"
	"encoding/json"
)

type Prefix struct {
	Guard    ranger.MemTableGuard
	Prefix   value.UserKey
	Segments []*segment.Segment
	SeqNo    *value.SeqNo
}

func NewPrefix(guard ranger.MemTableGuard, prefix value.UserKey, segments []*segment.Segment, seqno *value.SeqNo) *Prefix {
	return &Prefix{
		Guard:    guard,
		Prefix:   prefix,
		Segments: segments,
		SeqNo:    seqno,
	}
}

// type Iterator interface {
// 	Next() (*value.Value, error)
// 	NextBack() (*value.Value, error)
// }

type PrefixIterator struct {
	// @p2 create a boxed iterator maybe? a struct instead of interface
	Iter merge.Iterator
}

func NewPrefixIterator(lock *Prefix, seqno *value.SeqNo) *PrefixIterator {
	var segmentIters []merge.Iterator

	for _, segment := range lock.Segments {
		reader := segment.Prefix(lock.Prefix)
		segmentIters = append(segmentIters, reader)
	}
	// @TODO: who tf is inserting iterators in here
	// very expensive memory wise
	iters := []merge.Iterator{merge.NewMergeIterator(segmentIters)}

	for _, memtable := range *lock.Guard.Sealed.Obj {
		iters = append(iters, newMemTableIterator(memtable, lock.Prefix))
	}

	memtableIter := newMemTableIterator(lock.Guard.Active.Obj, lock.Prefix)
	iters = append(iters, memtableIter)

	mergedIter := merge.NewMergeIterator(iters).EvictOldVersion(true)

	if seqno != nil {
		mergedIter = mergedIter.SnapshotSeq(*seqno)
	}

	filteredIter := NewFilterIterator(mergedIter, func(value *value.Value) bool {
		return value.IsTombstone()
	})

	return &PrefixIterator{Iter: filteredIter}
}

func (pi *PrefixIterator) Next() (*value.UserKey, *value.UserValue, error) {
	value, err := pi.Iter.Next()
	if err != nil || value == nil {
		return nil, nil, err
	}
	return &value.Key, &value.Value, nil
}

func (pi *PrefixIterator) NextBack() (*value.UserKey, *value.UserValue, error) {
	value, err := pi.Iter.NextBack()
	if err != nil || value == nil {
		return nil, nil, err
	}
	return &value.Key, &value.Value, nil
}

type MemTableIterator struct {
	// @TODO: fixdis
	items  map[string]value.UserValue
	prefix value.UserKey
}

func newMemTableIterator(memtable *memtable.MemTable, prefix value.UserKey) merge.Iterator {
	return &MemTableIterator{
		items:  memtable.Items,
		prefix: prefix,
	}
}

func (mti *MemTableIterator) Next() (*value.Value, error) {
	// Iterate over the memtable's items
	for k, v := range mti.items {
		var parsedKey value.ParsedInternalKey
		if err := json.Unmarshal([]byte(k), &parsedKey); err != nil {
			return nil, err
		}

		if bytes.Equal(parsedKey.UserKey, mti.prefix) {
			result := &value.Value{
				Key:       parsedKey.UserKey,
				Value:     value.UserValue(v),
				ValueType: parsedKey.ValueType,
				SeqNo:     parsedKey.SeqNo,
			}
			return result, nil
		}
	}
	return nil, nil
}

func (mti *MemTableIterator) NextBack() (*value.Value, error) {
	// Iterate backwards over the memtable's items
	return nil, nil
}

func NewFilterIterator(iter merge.Iterator, filterFunc func(*value.Value) bool) merge.Iterator {
	return &FilterIterator{
		iter:       iter,
		filterFunc: filterFunc,
	}
}

type FilterIterator struct {
	iter       merge.Iterator
	filterFunc func(*value.Value) bool
}

func (fi *FilterIterator) Next() (*value.Value, error) {
	for {
		value, err := fi.iter.Next()
		if err != nil || value == nil {
			return nil, err
		}
		if fi.filterFunc(value) {
			return value, nil
		}
	}
}

func (fi *FilterIterator) NextBack() (*value.Value, error) {
	for {
		value, err := fi.iter.NextBack()
		if err != nil || value == nil {
			return nil, err
		}
		if fi.filterFunc(value) {
			return value, nil
		}
	}
}
