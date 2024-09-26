package seqno

import (
	"bagh/value"
	"sync/atomic"
)

/// Thread-safe sequence number generator
///
/// # Examples
///
/// ```
/// # use lsm_tree::{Config, SequenceNumberCounter};
/// #
/// # let path = tempfile::tempdir()?;
/// let tree = Config::new(path).open()?;
///
/// let seqno = SequenceNumberCounter::default();
///
/// // Do some inserts...
/// tree.insert("a".as_bytes(), "abc", seqno.next());
/// tree.insert("b".as_bytes(), "abc", seqno.next());
/// tree.insert("c".as_bytes(), "abc", seqno.next());
///
/// // Maybe create a snapshot
/// let snapshot = tree.snapshot(seqno.get());
///
/// // Create a batch
/// let batch_seqno = seqno.next();
/// tree.remove("a".as_bytes(), batch_seqno);
/// tree.remove("b".as_bytes(), batch_seqno);
/// tree.remove("c".as_bytes(), batch_seqno);
/// #
/// # assert!(tree.is_empty()?);
/// # Ok::<(), lsm_tree::Error>(())
/// ```

// SequenceNumberCounter is a thread-safe sequence number generator
type SequenceNumberCounter struct {
	counter *atomic.Uint64
}

// NewSequenceNumberCounter creates a new counter, setting it to some previous value
func NewSequenceNumberCounter(prev value.SeqNo) *SequenceNumberCounter {
	counter := new(atomic.Uint64)
	counter.Store(uint64(prev))
	return &SequenceNumberCounter{counter: counter}
}

// Get retrieves the current sequence number.
// This should only be used when creating a snapshot.
func (s *SequenceNumberCounter) Get() value.SeqNo {
	return value.SeqNo(s.counter.Load())
}

// Next gets the next sequence number.
func (s *SequenceNumberCounter) Next() value.SeqNo {
	return value.SeqNo(s.counter.Add(1) - 1)
}

// Example usage
// func main() {
// 	// Create a new SequenceNumberCounter
// 	seqno := NewSequenceNumberCounter(0)

// 	// Do some inserts...
// 	// tree.Insert([]byte("a"), "abc", seqno.Next())
// 	// tree.Insert([]byte("b"), "abc", seqno.Next())
// 	// tree.Insert([]byte("c"), "abc", seqno.Next())

// 	// Maybe create a snapshot
// 	snapshot := seqno.Get()

// 	// Create a batch
// 	batchSeqno := seqno.Next()
// 	// tree.Remove([]byte("a"), batchSeqno)
// 	// tree.Remove([]byte("b"), batchSeqno)
// 	// tree.Remove([]byte("c"), batchSeqno)

// 	// Use snapshot and batchSeqno as needed
// 	_ = snapshot
// 	_ = batchSeqno
// }
