package ranger

import (
	"bagh/memtable"
	"bagh/merge"
	"bagh/segment"
	"bagh/value"
	"sync"
)

// @TODO: do this for Diskblock as well, try here as well
type RwLockGuard[T any] struct {
	Mu  sync.RWMutex
	Obj *T
}

type MemTableGuard struct {
	Active *RwLockGuard[memtable.MemTable]
	Sealed *RwLockGuard[map[string]*memtable.MemTable]
}

type Range struct {
	Guard    MemTableGuard
	Bounds   [2]segment.Bound[value.UserKey]
	Segments []*segment.Segment
	Seqno    value.SeqNo
}

func NewRange(
	guard MemTableGuard,
	bounds [2]segment.Bound[value.UserKey],
	segments []*segment.Segment,
	seqno value.SeqNo,
) *Range {
	return &Range{
		Guard:    guard,
		Bounds:   bounds,
		Segments: segments,
		Seqno:    seqno,
	}
}

type RangeIterator struct {
	iter merge.Iterator
}

func NewRangeIterator(lock *Range, seqno *value.SeqNo) *RangeIterator {
	// lo := segment.Bound[value.ParsedInternalKey]{}
	// switch {
	// case lock.Bounds[0].Included != nil:
	// 	lo = segment.Bound[value.ParsedInternalKey]{
	// 		Included: &value.ParsedInternalKey{
	// 			UserKey:   *lock.Bounds[0].Included,
	// 			SeqNo:     math.MaxUint64,
	// 			ValueType: value.Tombstone,
	// 		},
	// 	}
	// case lock.Bounds[0].Excluded != nil:
	// 	lo = segment.Bound[value.ParsedInternalKey]{
	// 		Excluded: &value.ParsedInternalKey{
	// 			UserKey:   *lock.Bounds[0].Excluded,
	// 			SeqNo:     0,
	// 			ValueType: value.Tombstone,
	// 		},
	// 	}
	// default:
	// 	lo = segment.Bound[value.ParsedInternalKey]{Unbounded: true}
	// }

	// hi := segment.Bound[value.ParsedInternalKey]{}
	// switch {
	// case lock.Bounds[1].Included != nil:
	// 	hi = segment.Bound[value.ParsedInternalKey]{
	// 		Included: &value.ParsedInternalKey{
	// 			UserKey:   *lock.Bounds[0].Included,
	// 			SeqNo:     0,
	// 			ValueType: value.Record,
	// 		},
	// 	}
	// case lock.Bounds[1].Excluded != nil:
	// 	hi = segment.Bound[value.ParsedInternalKey]{
	// 		Excluded: &value.ParsedInternalKey{
	// 			UserKey:   *lock.Bounds[0].Excluded,
	// 			SeqNo:     0,
	// 			ValueType: value.Record,
	// 		},
	// 	}
	// default:
	// 	hi = segment.Bound[value.ParsedInternalKey]{Unbounded: true}
	// }

	segmentIters := make([]merge.Iterator, 0)

	for _, segment := range lock.Segments {
		// Assuming the Segment.range() method is available in Go
		reader := segment.Range(lock.Bounds[0], lock.Bounds[1])
		segmentIters = append(segmentIters, reader)
	}

	iters := []merge.Iterator{merge.NewMergeIterator(segmentIters)}

	// // for _, mt := range *lock.Guard.Sealed.Obj {
	// // 	v := mt.Range(lo, hi)
	// // 	iters = append(iters, *v)
	// // }

	// // activeMemtableIter := lock.Guard.Active.Obj.Range(lo, hi)

	// iters = append(iters, *activeMemtableIter)

	mergeIter := merge.NewMergeIterator(iters)
	mergeIter.EvictOldVersion(true)

	if seqno != nil {
		mergeIter.SnapshotSeq(*seqno)
	}

	// remvoe tombstones @TODO:
	// iter := merge.BoxedIterator(mergeIter.filter(func(value value.Value) bool {
	// 	return value.IsTombstone()
	// }))

	return &RangeIterator{iter: mergeIter}
}

func (r *RangeIterator) Next() (*value.UserKey, *value.UserValue, bool) {
	// This mimics the Rust Option and Result pattern using tuple (UserKey, UserValue, bool)
	// where bool indicates if a value was returned or not
	nextValue, err := r.iter.Next()
	if err != nil {
		return nil, nil, false
	}
	return &nextValue.Key, &nextValue.Value, true
}

func (r *RangeIterator) NextBack() (*value.UserKey, *value.UserValue, bool) {
	// Same as above, mimicking Rust's DoubleEndedIterator next_back
	nextBackValue, err := r.iter.NextBack()
	if err != nil {
		return nil, nil, false
	}
	return &nextBackValue.Key, &nextBackValue.Value, true

}

func (r *Range) IntoIter() *RangeIterator {
	return NewRangeIterator(r, &r.Seqno)
}
