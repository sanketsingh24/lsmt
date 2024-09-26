package prefix

import (
	"bagh/merge"
	"bagh/value"
)

type Prefix struct {
	Guard    MemTableGuard
	Prefix   value.UserKey
	Segments []*Segment
	SeqNo    *value.SeqNo
}

func NewPrefix(guard MemTableGuard, prefix UserKey, segments []*Segment, seqno *SeqNo) *Prefix {
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
	iter merge.BoxedIterator
}

func NewPrefixIterator(lock *Prefix, seqno *SeqNo) *PrefixIterator {
	var segmentIters []merge.BoxedIterator

	for _, segment := range lock.Segments {
		reader := segment.Prefix(lock.Prefix)
		segmentIters = append(segmentIters, reader)
	}

	iters := []Iterator{NewMergeIterator(segmentIters)}

	for _, memtable := range lock.Guard.Sealed {
		iters = append(iters, newMemTableIterator(memtable, lock.Prefix))
	}

	memtableIter := newMemTableIterator(lock.Guard.Active, lock.Prefix)
	iters = append(iters, memtableIter)

	mergedIter := NewMergeIterator(iters).EvictOldVersions(true)

	if seqno != nil {
		mergedIter = mergedIter.SnapshotSeqNo(*seqno)
	}

	filteredIter := NewFilterIterator(mergedIter, func(value *Value) bool {
		return value.ValueType != Tombstone
	})

	return &PrefixIterator{iter: filteredIter}
}

func (pi *PrefixIterator) Next() (*UserKey, *UserValue, error) {
	value, err := pi.iter.Next()
	if err != nil || value == nil {
		return nil, nil, err
	}
	return &value.Key.UserKey, &value.Value, nil
}

func (pi *PrefixIterator) NextBack() (*UserKey, *UserValue, error) {
	value, err := pi.iter.NextBack()
	if err != nil || value == nil {
		return nil, nil, err
	}
	return &value.Key.UserKey, &value.Value, nil
}

func newMemTableIterator(memtable *MemTable, prefix UserKey) Iterator {
	return &MemTableIterator{
		items:  memtable.Items,
		prefix: prefix,
	}
}

type MemTableIterator struct {
	items  map[ParsedInternalKey]UserValue
	prefix UserKey
}

func (mti *MemTableIterator) Next() (*Value, error) {
	// Iterate over the memtable's items
	for key, value := range mti.items {
		if key.UserKey == mti.prefix {
			return &Value{Key: key, Value: value, ValueType: key.ValueType}, nil
		}
	}
	return nil, nil
}

func (mti *MemTableIterator) NextBack() (*Value, error) {
	// Iterate backwards over the memtable's items
	return nil, nil
}

func NewFilterIterator(iter Iterator, filterFunc func(*Value) bool) Iterator {
	return &FilterIterator{
		iter:       iter,
		filterFunc: filterFunc,
	}
}

type FilterIterator struct {
	iter       Iterator
	filterFunc func(*Value) bool
}

func (fi *FilterIterator) Next() (*Value, error) {
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

func (fi *FilterIterator) NextBack() (*Value, error) {
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
