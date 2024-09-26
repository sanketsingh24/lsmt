```go
package main

import (
	"sync"
)

// ValueBlock represents a block of values in a segment
type ValueBlock struct {
	DiskBlock
}

// Size calculates the total size of the ValueBlock
func (vb *ValueBlock) Size() int {
	size := 0
	for _, item := range vb.Items {
		size += item.Size()
	}
	return size + int(unsafe.Sizeof(vb))
}

// LoadAndCacheByBlockHandle loads and caches a ValueBlock by its BlockHandle
func LoadAndCacheByBlockHandle(
	descriptorTable *FileDescriptorTable,
	blockCache *BlockCache,
	segmentID string,
	blockHandle *BlockHandle,
) (*ValueBlock, error) {
	if block, ok := blockCache.GetDiskBlock(segmentID, blockHandle.StartKey); ok {
		return block, nil
	}

	fileGuard, err := descriptorTable.Access(segmentID)
	if err != nil {
		return nil, err
	}
	if fileGuard == nil {
		return nil, fmt.Errorf("should acquire file handle")
	}

	file := fileGuard.File.Lock()
	defer file.Unlock()

	block, err := ValueBlock{}.FromFileCompressed(file, blockHandle.Offset, blockHandle.Size)
	if err != nil {
		return nil, err
	}

	blockCache.InsertDiskBlock(segmentID, blockHandle.StartKey, block)

	return block, nil
}

// LoadAndCacheBlockByItemKey loads and caches a ValueBlock by an item key
func LoadAndCacheBlockByItemKey(
	descriptorTable *FileDescriptorTable,
	blockIndex *BlockIndex,
	blockCache *BlockCache,
	segmentID string,
	itemKey []byte,
) (*ValueBlock, error) {
	blockHandle, err := blockIndex.GetLowerBoundBlockInfo(itemKey)
	if err != nil {
		return nil, err
	}
	if blockHandle == nil {
		return nil, nil
	}

	return LoadAndCacheByBlockHandle(descriptorTable, blockCache, segmentID, blockHandle)
}
