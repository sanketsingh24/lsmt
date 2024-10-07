package tree

import (
	"bagh/config"
	"bagh/descriptor"
	"bagh/file"
	"bagh/levels"
	"bagh/memtable"
	"bagh/segment"
	"bagh/stop"
	"log"
	"path/filepath"
	"sync"
)

// @TODO: how to use this in sync map?
// type SealedMemtables map[string]*memtable.MemTable

type TreeInner struct {
	ActiveMemtable  *memtable.MemTable
	SealedMemtables map[string]*memtable.MemTable
	Levels          *levels.Levels
	Config          *config.PersistedConfig
	BlockCache      *segment.BlockCache
	DescriptorTable *descriptor.FileDescriptorTable
	OpenSnapshots   *SnapshotCounter
	StopSignal      *stop.StopSignal

	ActiveMutex sync.RWMutex
	SealedMutex sync.RWMutex
	LevelsMutex sync.RWMutex
}

func CreateNewTreeInner(config *config.Config) (*TreeInner, error) {
	levels, err := levels.NewLevels(
		config.Inner.LevelCount,
		filepath.Join(config.Inner.Path, file.LevelsManifestFile),
	)
	if err != nil {
		return nil, err
	}

	return &TreeInner{
		ActiveMemtable:  memtable.NewMemTable(),
		SealedMemtables: *new(map[string]*memtable.MemTable),
		Levels:          levels,
		Config:          config.Inner,
		BlockCache:      config.BlockCache,
		DescriptorTable: config.DescriptorTable,
		OpenSnapshots:   NewSnapshotCounter(),
		StopSignal:      stop.NewStopSignal(),
	}, nil
}

func (t *TreeInner) Drop() {
	log.Println("Dropping TreeInner")
	log.Println("Sending stop signal to compactors")
	t.StopSignal.Send()
}

// func main() {
// 	// Example usage
// 	config := &Config{
// 		Inner: &PersistedConfig{
// 			LevelCount: 3,
// 			Path:       "/tmp/db",
// 		},
// 		BlockCache:      &BlockCache{},
// 		DescriptorTable: &FileDescriptorTable{},
// 	}

// 	treeInner, err := CreateNewTreeInner(config)
// 	if err != nil {
// 		log.Fatalf("Failed to create TreeInner: %v", err)
// 	}
// 	defer treeInner.Close()

// 	// Use treeInner...
// }
