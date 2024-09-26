package merge

import (
	"bagh/value"
	"container/heap"
	"errors"
)

type IteratorIndex int
type BoxedIterator func() (value.Value, error)
type MinMaxHeap []*IteratorValue

// @TODO: get a better way to implement heap
func (h MinMaxHeap) Len() int           { return len(h) }
func (h MinMaxHeap) Less(i, j int) bool { return h[i].Value.Less(h[j].Value) }
func (h MinMaxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinMaxHeap) Push(x interface{}) {
	*h = append(*h, x.(*IteratorValue))
}
func (h *MinMaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type IteratorValue struct {
	Index int
	Value value.Value
}

func (v *IteratorValue) Less(other *value.Value) bool {
	return string(v.Value.Key) < string(other.Key)
}

type MergeIterator struct {
	iterators        []BoxedIterator
	heap             MinMaxHeap
	evictOldVersions bool
	snapshotSeqNo    *uint64
}

func NewMergeIterator(iterators []BoxedIterator) *MergeIterator {
	return &MergeIterator{
		iterators:        iterators,
		heap:             MinMaxHeap{},
		evictOldVersions: false,
		snapshotSeqNo:    nil,
	}
}

func (it *MergeIterator) EvictOldVersions(v bool) *MergeIterator {
	it.evictOldVersions = v
	return it
}

func (it *MergeIterator) SnapshotSeqNo(v uint64) *MergeIterator {
	it.snapshotSeqNo = &v
	return it
}

func (it *MergeIterator) advanceIter(idx int) error {
	value, err := it.iterators[idx]()
	if err != nil {
		return err
	}
	heap.Push(&it.heap, &IteratorValue{Index: idx, Value: value})
	return nil
}

func (it *MergeIterator) advanceIterBackwards(idx int) error {
	value, err := it.iterators[idx]()
	if err != nil {
		return err
	}
	heap.Push(&it.heap, &IteratorValue{Index: idx, Value: value})
	return nil
}

func (it *MergeIterator) pushNext() error {
	for idx := range it.iterators {
		if err := it.advanceIter(idx); err != nil {
			return err
		}
	}
	return nil
}

func (it *MergeIterator) pushNextBack() error {
	for idx := range it.iterators {
		if err := it.advanceIterBackwards(idx); err != nil {
			return err
		}
	}
	return nil
}

func (it *MergeIterator) Next() (*value.Value, error) {
	if len(it.heap) == 0 {
		if err := it.pushNext(); err != nil {
			return nil, err
		}
	}

	for len(it.heap) > 0 {
		head := heap.Pop(&it.heap).(*IteratorValue)
		idx := head.Index
		if err := it.advanceIter(idx); err != nil {
			return nil, err
		}

		if head.Value.ValueType == value.Tombstone || it.evictOldVersions {
			for len(it.heap) > 0 {
				next := heap.Pop(&it.heap).(*IteratorValue)
				if string(next.Value.Key) == string(head.Value.Key) {
					if err := it.advanceIter(next.Index); err != nil {
						return nil, err
					}

					if it.snapshotSeqNo != nil && head.Value.SeqNo >= *it.snapshotSeqNo {
						head = next
					}
				} else {
					heap.Push(&it.heap, next)
					break
				}
			}
		}

		if it.snapshotSeqNo != nil && head.Value.SeqNo >= *it.snapshotSeqNo {
			continue
		}

		return &head.Value, nil
	}

	return nil, errors.New("iterator exhausted")
}
