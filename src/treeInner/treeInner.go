package treeinner

import (
	"bagh/config"
	"bagh/descriptor"
	"bagh/file"
	"bagh/memtable"
	"bagh/segment"
	"bagh/snapshot"
	"log"
	"path/filepath"
	"sync"
)

// @TODO: how to use this in sync map?
// type SealedMemtables map[string]*memtable.MemTable

type TreeInner struct {
	ActiveMemtable  *memtable.MemTable
	SealedMemtables *sync.Map
	Levels          *Levels
	Config          *PersistedConfig
	BlockCache      *segment.BlockCache
	DescriptorTable *descriptor.FileDescriptorTable
	OpenSnapshots   *snapshot.SnapshotCounter
	StopSignal      *StopSignal

	ActiveMutex sync.RWMutex
	LevelsMutex sync.RWMutex
}

func CreateNewTreeInner(config *config.Config) (*TreeInner, error) {
	levels, err := CreateNewLevels(
		config.Inner.LevelCount,
		filepath.Join(config.Inner.Path, file.LevelsManifestFile),
	)
	if err != nil {
		return nil, err
	}

	return &TreeInner{
		activeMemtable:  memtable.NewMemTable(),
		sealedMemtables: &sync.Map{},
		levels:          levels,
		config:          config.Inner,
		blockCache:      config.BlockCache,
		descriptorTable: config.DescriptorTable,
		openSnapshots:   snapshot.NewSnapshotCounter(),
		stopSignal:      NewStopSignal(),
	}, nil
}

func (t *TreeInner) Drop() {
	log.Println("Dropping TreeInner")
	log.Println("Sending stop signal to compactors")
	t.stopSignal.Send()
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
