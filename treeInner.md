package main

import (
	. "bagh/config"
	. "bagh/memtable"
	"sync"
	"sync/atomic"
)

// Type definitions
type TreeId uint64
type MemtableId uint64

// Global Tree ID counter
var treeIdCounter atomic.Uint64

// SealedMemtables stores references to sealed memtables
type SealedMemtables struct {
	memtables []struct {
		id       MemtableId
		memtable *MemTable
	}
}

// Add a memtable to SealedMemtables
func (s *SealedMemtables) Add(id MemtableId, memtable *MemTable) {
	s.memtables = append(s.memtables, struct {
		id       MemtableId
		memtable *MemTable
	}{id, memtable})
}

// Remove a memtable by ID
func (s *SealedMemtables) Remove(idToRemove MemtableId) {
	filtered := s.memtables[:0]
	for _, mt := range s.memtables {
		if mt.id != idToRemove {
			filtered = append(filtered, mt)
		}
	}
	s.memtables = filtered
}

// // Iterate through SealedMemtables
//
//	func (s *SealedMemtables) Iter() <-chan struct {
//		id       MemtableId
//		memtable *MemTable
//	} {
//
//		ch := make(chan struct {
//			id       MemtableId
//			memtable *MemTable
//		})
//		go func() {
//			for _, mt := range s.memtables {
//				ch <- mt
//			}
//			close(ch)
//		}()
//		return ch
//	}
//
// GetNextTreeId returns the next unique Tree ID
func GetNextTreeId() TreeId {
	return TreeId(treeIdCounter.Add(1) - 1)
}

type TreeInner struct {
	id TreeId

	// monotonically increasing segment ID
	segmentIdCounter atomic.Uint64

	//@TODO: need these to make threadsafe

	// Active memtable that is being written to
	activeMemtable     *MemTable
	activeMemtableLock *sync.RWMutex

	// Frozen memtables that are being flushed
	sealedMemtables     *SealedMemtables
	sealedMemtablesLock *sync.RWMutex

	// Levels
	// levels     *LevelManifest
	// levelsLock *sync.RWMutex

	config Config

	// @P2
	// snapshots
	// stop tracker
}

// CreateNew creates a new TreeInner instance
func CreateNew(config Config) (*TreeInner, error) {
	// levels, err := CreateLevelManifest(config.Inner.LevelCount, filepath.Join(config.Path, LevelsManifestFile))
	// if err != nil {
	// 	return nil, err
	// }

	return &TreeInner{
		id:                  GetNextTreeId(),
		segmentIdCounter:    *new(atomic.Uint64),
		config:              config,
		activeMemtable:      &MemTable{},
		activeMemtableLock:  &sync.RWMutex{},
		sealedMemtables:     &SealedMemtables{},
		sealedMemtablesLock: &sync.RWMutex{},
		// levels:              levels,
		// levelsLock:          &sync.RWMutex{},
	}, nil
}

// GetNextSegmentId returns the next unique segment ID
// @p2
// func (t *TreeInner) GetNextSegmentId() *SegmentId {
// 	return &SegmentId(t.segmentIdCounter.Add(1) - 1)
// }

// Drop stops the compaction process
// @p2
// func (t *TreeInner) Drop() {
// 	log.Println("Dropping TreeInner")
// 	log.Println("Sending stop signal to compactors")
// }
