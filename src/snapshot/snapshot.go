package snapshot

import (
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
	tree.openSnapshots.Increment()
	log.Printf("Opening snapshot with seqno: %d", seqno)
	return &Snapshot{
		tree:  tree,
		seqno: seqno,
	}
}

func (s *Snapshot) Get(key []byte) (value.UserValue, error) {
	// Implementation of get...
	return nil, nil
}

func (s *Snapshot) Iter() *Range {
	return s.tree.createIter(s.seqno)
}

func (s *Snapshot) Range(start, end []byte) *Range {
	return s.tree.createRange(start, end, s.seqno)
}

func (s *Snapshot) Prefix(prefix []byte) *Prefix {
	return s.tree.createPrefix(prefix, s.seqno)
}

func (s *Snapshot) FirstKeyValue() (value.UserKey, value.UserValue, error) {
	// Implementation of first_key_value...
	return nil, nil, nil
}

func (s *Snapshot) LastKeyValue() (value.UserKey, value.UserValue, error) {
	// Implementation of last_key_value...
	return nil, nil, nil
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
	// Implementation of len...
	return 0, nil
}

func (s *Snapshot) Drop() {
	log.Println("Closing snapshot")
	s.tree.openSnapshots.Decrement()
}
