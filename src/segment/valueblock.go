package segment

import (
	"unsafe"

	"bagh/descriptor"
	"bagh/disk"
	"bagh/value"
)

// ValueBlock is a type alias for DiskBlock<Value>
type ValueBlock struct {
	disk.DiskBlock[value.Value]
}

// @TODO: this might be wrong
func (vb *ValueBlock) Size() int {
	size := int(unsafe.Sizeof(*vb))
	for _, item := range vb.Items {
		size += int(unsafe.Sizeof(item))
	}
	return size
}

func LoadAndCacheByBlockHandle(
	descriptorTable *descriptor.FileDescriptorTable,
	blockCache *BlockCache,
	segmentID string,
	blockHandle *BlockHandle,
) (*ValueBlock, error) {
	if block := blockCache.GetDiskBlock(segmentID, blockHandle.StartKey); block != nil {
		// Cache hit: Copy from block
		return block, nil
	}

	// Cache miss: load from disk
	fileGuard, err := descriptorTable.Access(segmentID)
	if err != nil {
		return nil, err
	}
	if fileGuard == nil {
		return nil, err
	}

	file := fileGuard.File()
	// might not work? @TODO:
	block := new(ValueBlock)
	// @TODO: file? is it same as io.readseeker?
	err = block.FromFileCompressed(file, int64(blockHandle.Offset), blockHandle.Size)
	if err != nil {
		return nil, err
	}
	blockCache.InsertDiskBlock(segmentID, blockHandle.StartKey, block)

	return block, nil
}

func LoadAndCacheBlockByItemKey(
	descriptorTable *descriptor.FileDescriptorTable,
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

// // Helper functions (to be implemented)
// func ValueBlockFromFileCompressed(file *os.File, offset int64, size int64) (*ValueBlock, error) {
// 	// Implementation
// }

// func (v *Value) Size() usize {
// 	// Implementation
// }

// type usize int
