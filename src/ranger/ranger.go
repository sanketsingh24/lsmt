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
	mu  sync.RWMutex
	obj *T
}

type MemTableGuard struct {
	active *RwLockGuard[memtable.MemTable]
	sealed *RwLockGuard[map[string]*memtable.MemTable]
}

// type MemTableGuard struct {
// 	Active *memtable.MemTable
// 	Sealed map[string]*memtable.MemTable
// 	mu     *sync.RWMutex
// }

type Bound[T any] struct {
	Included  *T
	Excluded  *T
	Unbounded bool
}

type Range struct {
	guard    MemTableGuard
	bounds   [2]Bound[value.UserKey]
	segments []*segment.Segment
	seqno    *value.SeqNo
}

func NewRange(
	guard MemTableGuard,
	bounds [2]Bound[value.UserKey],
	segments []*segment.Segment,
	seqno *value.SeqNo,
) *Range {
	return &Range{
		guard:    guard,
		bounds:   bounds,
		segments: segments,
		seqno:    seqno,
	}
}

type RangeIterator struct {
	iter merge.BoxedIterator
}

func NewRangeIterator(lock *Range, seqno *value.SeqNo) *RangeIterator {
	lo := Bound[value.ParsedInternalKey]{}
	switch {
	case lock.bounds[0].Included != nil:
		lo = Bound[value.ParsedInternalKey]{
			Included: &value.ParsedInternalKey{
				// create new ParsedInternalKey using SeqNo::MAX, ValueType::Tombstone
			},
		}
	case lock.bounds[0].Excluded != nil:
		lo = Bound[value.ParsedInternalKey]{
			Excluded: &value.ParsedInternalKey{
				// create new ParsedInternalKey using seqno 0, ValueType::Tombstone
			},
		}
	default:
		lo = Bound[value.ParsedInternalKey]{Unbounded: true}
	}

	hi := Bound[value.ParsedInternalKey]{}
	switch {
	case lock.bounds[1].Included != nil:
		hi = Bound[value.ParsedInternalKey]{
			Included: &value.ParsedInternalKey{
				// create new ParsedInternalKey using seqno 0, ValueType::Value
			},
		}
	case lock.bounds[1].Excluded != nil:
		hi = Bound[value.ParsedInternalKey]{
			Excluded: &value.ParsedInternalKey{
				// create new ParsedInternalKey using seqno 0, ValueType::Value
			},
		}
	default:
		hi = Bound[value.ParsedInternalKey]{Unbounded: true}
	}

	segmentIters := make([]merge.BoxedIterator, 0)

	for _, segment := range lock.segments {
		// Assuming the Segment.range() method is available in Go
		reader := segment.rangeMethod(lock.bounds)
		segmentIters = append(segmentIters, reader)
	}

	iters := []merge.BoxedIterator{MergeIteratorNew(segmentIters)}

	for _, memtable := range lock.guard.sealed.obj {
		iters = append(iters, memtable.rangeMethod(lo, hi))
	}

	activeMemtableIter := lock.guard.active.obj.rangeMethod(lo, hi)

	iters = append(iters, activeMemtableIter)

	mergeIter := MergeIteratorNew(iters)
	mergeIter.evictOldVersions(true)

	if seqno != nil {
		mergeIter.snapshotSeqNo(*seqno)
	}

	iter := merge.BoxedIterator(mergeIter.filter(func(value value.Value) bool {
		return value.IsTombstone()
	}))

	return &RangeIterator{iter: iter}
}

func (r *RangeIterator) Next() (value.UserKey, value.UserValue, bool) {
	// This mimics the Rust Option and Result pattern using tuple (UserKey, UserValue, bool)
	// where bool indicates if a value was returned or not
	if nextValue, ok := r.iter.Next(); ok {
		return nextValue.key, nextValue.value, true
	}
	return "", "", false
}

func (r *RangeIterator) NextBack() (value.UserKey, value.UserValue, bool) {
	// Same as above, mimicking Rust's DoubleEndedIterator next_back
	if nextBackValue, ok := r.iter.NextBack(); ok {
		return nextBackValue.key, nextBackValue.value, true
	}
	return "", "", false
}

func (r *Range) IntoIter() *RangeIterator {
	return NewRangeIterator(r, r.seqno)
}
