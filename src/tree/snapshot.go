package tree

import (
	"bagh/prefix"
	"bagh/ranger"
	"bagh/segment"
	"bagh/value"
	"errors"
	"io"
	"log"
	"sync/atomic"
)

type SnapshotCounter struct {
	counter *atomic.Uint32
}

func NewSnapshotCounter() *SnapshotCounter {
	return &SnapshotCounter{
		counter: new(atomic.Uint32),
	}
}

func (sc *SnapshotCounter) Increment() uint32 {
	return sc.counter.Add(1)
}

func (sc *SnapshotCounter) Decrement() uint32 {
	return sc.counter.Add(^uint32(0))
}

func (sc *SnapshotCounter) HasOpenSnapshots() bool {
	return sc.counter.Load() > 0
}

type Snapshot struct {
	tree  *Tree
	seqno value.SeqNo
}

func NewSnapshot(tree *Tree, seqno value.SeqNo) *Snapshot {
	tree.TreeInner.OpenSnapshots.Increment()
	log.Printf("Opening snapshot with seqno: %d", seqno)
	return &Snapshot{
		tree:  tree,
		seqno: seqno,
	}
}

func (s *Snapshot) Get(key []byte) (value.UserValue, error) {
	entry, err := s.tree.GetInternalEntry(key, true, &s.seqno)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	return entry.Value, nil
}

func (s *Snapshot) Iter() *ranger.Range {
	return s.tree.CreateIter(&s.seqno)
}

func (s *Snapshot) Range(start, end *segment.Bound[value.UserKey]) *ranger.Range {
	return s.tree.CreateRange(start, end, &s.seqno)
}

func (s *Snapshot) Prefix(prefix []byte) *prefix.Prefix {
	return s.tree.CreatePrefix(prefix, &s.seqno)
}

func (s *Snapshot) FirstKeyValue() (value.UserKey, value.UserValue, error) {
	iter := s.Iter().IntoIter()
	a, b, c := iter.Next()
	if c {
		return *a, *b, nil
	}
	return nil, nil, errors.ErrUnsupported
}

func (s *Snapshot) LastKeyValue() (value.UserKey, value.UserValue, error) {
	a, b, c := s.Iter().IntoIter().NextBack()
	if c {
		return *a, *b, nil
	}
	return nil, nil, errors.ErrUnsupported
}

func (s *Snapshot) ContainsKey(key []byte) (bool, error) {
	_, err := s.Get(key)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, io.EOF) {
		return false, nil
	}
	return false, err
}

func (s *Snapshot) IsEmpty() (bool, error) {
	_, _, err := s.FirstKeyValue()
	if err == nil {
		return false, nil
	}
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	return false, err
}

func (s *Snapshot) Len() (int, error) {
	count := 0
	iter := s.Iter().IntoIter()
	ok := true
	for ok {
		_, _, ok = iter.Next()
		count++
	}
	return count, errors.ErrUnsupported
}

func (s *Snapshot) Drop() {
	log.Println("Closing snapshot")
	s.tree.TreeInner.OpenSnapshots.Decrement()
}
