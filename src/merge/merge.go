// @TODO: check if Iterators work as expected

package merge

import (
	"bagh/value"
	"bytes"
	"container/heap"
	"errors"
)

// @TODO: bro check these
type IteratorIndex int
type MinMaxHeap struct {
	minHeap *minHeap
	maxHeap *maxHeap
}

func NewMinMaxHeap() *MinMaxHeap {
	return &MinMaxHeap{
		minHeap: &minHeap{},
		maxHeap: &maxHeap{},
	}
}

func (h *MinMaxHeap) Push(x IteratorValue) {
	heap.Push(h.minHeap, x)
	heap.Push(h.maxHeap, x)
}

func (h *MinMaxHeap) PopMin() IteratorValue {
	min := heap.Pop(h.minHeap).(IteratorValue)
	h.maxHeap.remove(min)
	return min
}

func (h *MinMaxHeap) PopMax() IteratorValue {
	max := heap.Pop(h.maxHeap).(IteratorValue)
	h.minHeap.remove(max)
	return max
}

func (h *MinMaxHeap) Len() int {
	return h.minHeap.Len()
}

type minHeap []IteratorValue

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return bytes.Compare(h[i].Value.Key, h[j].Value.Key) < 0 }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(IteratorValue))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *minHeap) remove(x IteratorValue) {
	for i, v := range *h {
		if v.Index == x.Index {
			heap.Remove(h, i)
			return
		}
	}
}

type maxHeap []IteratorValue

func (h maxHeap) Len() int           { return len(h) }
func (h maxHeap) Less(i, j int) bool { return bytes.Compare(h[i].Value.Key, h[j].Value.Key) > 0 }
func (h maxHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *maxHeap) Push(x interface{}) {
	*h = append(*h, x.(IteratorValue))
}

func (h *maxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// @TODO: check if remove works
func (h *maxHeap) remove(x IteratorValue) {
	for i, v := range *h {
		if v.Index == x.Index {
			heap.Remove(h, i)
			return
		}
	}
}

type Iterator interface {
	Next() (*value.Value, error)
	NextBack() (*value.Value, error)
}

type IteratorValue struct {
	Index int
	Value value.Value
}

func (v *IteratorValue) Less(other *value.Value) bool {
	return string(v.Value.Key) < string(other.Key)
}

// type BoxedIterator struct {
// 	data []value.Value
// 	pos  int
// }

// func NewBoxedIterator(values []value.Value) *BoxedIterator {
// 	return &BoxedIterator{data: values, pos: 0}
// }

// func (it *BoxedIterator) Next() (*value.Value, error) {
// 	if it.pos >= len(it.data) {
// 		return nil, errors.New("out of range")
// 	}
// 	value := it.data[it.pos]
// 	it.pos++
// 	return &value, nil
// }

// func (it *BoxedIterator) NextBack() (*value.Value, error) {
// 	if it.pos <= 0 {
// 		return nil, errors.New("out of range")
// 	}
// 	it.pos--
// 	value := it.data[it.pos]
// 	return &value, nil
// }

type MergeIterator struct {
	Iterators        []Iterator
	Heap             MinMaxHeap
	EvictOldVersions bool
	SnapshotSeqNo    *value.SeqNo
}

func NewMergeIterator(Iterators []Iterator) *MergeIterator {
	return &MergeIterator{
		Iterators:        Iterators,
		Heap:             MinMaxHeap{},
		EvictOldVersions: false,
		SnapshotSeqNo:    nil,
	}
}

func (it *MergeIterator) EvictOldVersion(v bool) *MergeIterator {
	it.EvictOldVersions = v
	return it
}

func (it *MergeIterator) SnapshotSeq(v value.SeqNo) *MergeIterator {
	it.SnapshotSeqNo = &v
	return it
}

// check @TODO:
func (it *MergeIterator) advanceIter(idx int) error {
	value, err := it.Iterators[idx].Next()
	if err != nil {
		return err
	}
	// @TODO: ?? why u using global heap here
	// heap.Push(&IteratorValue{Index: idx, Value: *value})
	it.Heap.Push(IteratorValue{Index: idx, Value: *value})
	return nil
}

// @TODO: do this
func (it *MergeIterator) advanceIterBackwards(idx int) error {
	value, err := it.Iterators[idx].NextBack()
	if err != nil {
		return err
	}
	it.Heap.Push(IteratorValue{Index: idx, Value: *value})
	return nil
}

func (it *MergeIterator) pushNext() error {
	for idx := range it.Iterators {
		if err := it.advanceIter(idx); err != nil {
			return err
		}
	}
	return nil
}

func (it *MergeIterator) pushNextBack() error {
	for idx := range it.Iterators {
		if err := it.advanceIterBackwards(idx); err != nil {
			return err
		}
	}
	return nil
}

func (it *MergeIterator) Next() (*value.Value, error) {
	// @TODO: lengths can diverge
	if it.Heap.minHeap.Len() == 0 {
		if err := it.pushNext(); err != nil {
			return nil, err
		}
	}

	for it.Heap.minHeap.Len() > 0 {
		head := it.Heap.PopMax()
		idx := head.Index
		if err := it.advanceIter(idx); err != nil {
			return nil, err
		}

		if head.Value.IsTombstone() || it.EvictOldVersions {
			//does this cache like js? @TODO: check if len updates on pop
			for it.Heap.minHeap.Len() > 0 {
				next := it.Heap.PopMin()
				if string(next.Value.Key) == string(head.Value.Key) {
					if err := it.advanceIter(next.Index); err != nil {
						return nil, err
					}

					if it.SnapshotSeqNo != nil && head.Value.SeqNo >= *it.SnapshotSeqNo {
						head = next
					}
				} else {
					it.Heap.Push(next)
					break
				}
			}
		}

		if it.SnapshotSeqNo != nil && head.Value.SeqNo >= *it.SnapshotSeqNo {
			continue
		}

		return &head.Value, nil
	}

	return nil, errors.New("iterator exhausted")
}

func (it *MergeIterator) NextBack() (*value.Value, error) {
	if it.Heap.maxHeap.Len() == 0 {
		if err := it.pushNextBack(); err != nil {
			return nil, err
		}
	}

	for it.Heap.maxHeap.Len() > 0 {
		head := it.Heap.PopMax()
		if err := it.advanceIterBackwards(head.Index); err != nil {
			return nil, err
		}

		reachedTombstone := false

		if head.Value.IsTombstone() || it.EvictOldVersions {
			next := it.Heap.PopMax()
			for it.Heap.maxHeap.Len() > 0 && bytes.Equal(next.Value.Key, head.Value.Key) {
				if reachedTombstone {
					continue
				}

				next := it.Heap.PopMax()
				if err := it.advanceIterBackwards(next.Index); err != nil {
					return nil, err
				}

				if next.Value.ValueType == value.Tombstone {
					if it.SnapshotSeqNo != nil {
						if next.Value.SeqNo < *it.SnapshotSeqNo {
							reachedTombstone = true
						}
					} else {
						reachedTombstone = true
					}
				}

				if it.SnapshotSeqNo != nil {
					if next.Value.SeqNo < *it.SnapshotSeqNo {
						head = next
					}
				} else {
					head = next
				}
			}
		}

		if it.SnapshotSeqNo != nil && head.Value.SeqNo >= *it.SnapshotSeqNo {
			continue
		}

		if reachedTombstone {
			continue
		}

		return &head.Value, nil
	}

	return nil, nil
}
