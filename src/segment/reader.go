package segment

import (
	"bagh/descriptor"
	"bagh/value"
	"container/list"
	"fmt"
)

// Reader struct
type Reader struct {
	descriptorTable *descriptor.FileDescriptorTable
	blockIndex      *BlockIndex

	segmentID  string
	blockCache *BlockCache

	blocks    map[string]*list.List
	currentLo value.UserKey
	currentHi value.UserKey

	startOffset   value.UserKey
	endOffset     value.UserKey
	isInitialized bool
}

// NewReader creates a new Reader
func NewReader(
	descriptorTable *descriptor.FileDescriptorTable,
	segmentID string,
	blockCache *BlockCache,
	blockIndex *BlockIndex,
	startOffset value.UserKey,
	endOffset value.UserKey,
) *Reader {
	return &Reader{
		descriptorTable: descriptorTable,
		segmentID:       segmentID,
		blockCache:      blockCache,
		blockIndex:      blockIndex,
		blocks:          make(map[string]*list.List),
		startOffset:     startOffset,
		endOffset:       endOffset,
	}
}

// initialize initializes the Reader
func (r *Reader) initialize() error {
	if r.startOffset != nil {
		r.currentLo = r.startOffset
		if err := r.loadBlock(r.startOffset); err != nil {
			return err
		}
	}

	if r.endOffset != nil {
		r.currentHi = r.endOffset
		if !equal(r.currentLo, r.endOffset) {
			if err := r.loadBlock(r.endOffset); err != nil {
				return err
			}
		}
	}

	r.isInitialized = true
	return nil
}

// loadBlock loads a block into memory
func (r *Reader) loadBlock(key value.UserKey) error {
	if r.blockCache != nil {
		block, err := LoadAndCacheBlockByItemKey(
			r.descriptorTable,
			r.blockIndex,
			r.blockCache,
			r.segmentID,
			key,
		)
		if err != nil {
			return err
		}
		if block != nil {
			items := list.New()
			for _, item := range block.Items {
				items.PushBack(item)
			}
			r.blocks[string(key)] = items
			return nil
		}
	} else {
		blockHandle, err := r.blockIndex.GetLowerBoundBlockInfo(key)
		if err != nil {
			return err
		}
		if blockHandle != nil {
			fileGuard, err := r.descriptorTable.Access(r.segmentID)
			if err != nil {
				return err
			}
			if fileGuard == nil {
				return fmt.Errorf("failed to acquire file handle")
			}
			block := new(ValueBlock)
			err = block.FromFileCompressed(
				fileGuard.File(),
				int64(blockHandle.Offset),
				blockHandle.Size,
			)
			if err != nil {
				return err
			}

			items := list.New()
			for _, item := range block.Items {
				items.PushBack(item)
			}
			r.blocks[string(key)] = items
			return nil
		}
	}
	return nil
}

// Next returns the next item
func (r *Reader) Next() (*value.Value, error) {
	if !r.isInitialized {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	if r.currentLo == nil {
		newBlockOffset, err := r.blockIndex.GetFirstBlockKey()
		if err != nil {
			return nil, err
		}
		r.currentLo = newBlockOffset.StartKey

		if !equal(newBlockOffset.StartKey, r.currentHi) {
			if err := r.loadBlock(newBlockOffset.StartKey); err != nil {
				return nil, err
			}
		}
	}

	if equal(r.currentHi, r.currentLo) {
		block := r.blocks[string(r.currentLo)]
		if block.Len() > 0 {
			return block.Remove(block.Front()).(*value.Value), nil
		}
		return nil, nil
	}

	block := r.blocks[string(r.currentLo)]
	if block != nil && block.Len() > 0 {
		item := block.Remove(block.Front()).(*value.Value)
		if block.Len() == 0 {
			delete(r.blocks, string(r.currentLo))
			newBlockOffset, err := r.blockIndex.GetNextBlockKey(r.currentLo)
			if err != nil {
				return nil, err
			}
			if newBlockOffset != nil {
				r.currentLo = newBlockOffset.StartKey
				if !equal(newBlockOffset.StartKey, r.currentHi) {
					if err := r.loadBlock(newBlockOffset.StartKey); err != nil {
						return nil, err
					}
				}
			}
		}
		return item, nil
	}

	return nil, nil
}

// NextBack returns the previous item
func (r *Reader) NextBack() (*value.Value, error) {
	if !r.isInitialized {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	if r.currentHi == nil {
		newBlockOffset, err := r.blockIndex.GetLastBlockKey()
		if err != nil {
			return nil, err
		}
		r.currentHi = newBlockOffset.StartKey

		if !equal(newBlockOffset.StartKey, r.currentLo) {
			if err := r.loadBlock(newBlockOffset.StartKey); err != nil {
				return nil, err
			}
		}
	}

	if equal(r.currentHi, r.currentLo) {
		block := r.blocks[string(r.currentHi)]
		if block.Len() > 0 {
			return block.Remove(block.Back()).(*value.Value), nil
		}
		return nil, nil
	}

	block := r.blocks[string(r.currentHi)]
	if block != nil && block.Len() > 0 {
		item := block.Remove(block.Back()).(*value.Value)
		if block.Len() == 0 {
			delete(r.blocks, string(r.currentHi))
			newBlockOffset, err := r.blockIndex.GetPreviousBlockKey(r.currentHi)
			if err != nil {
				return nil, err
			}
			if newBlockOffset != nil {
				r.currentHi = newBlockOffset.StartKey
				if !equal(newBlockOffset.StartKey, r.currentLo) {
					if err := r.loadBlock(newBlockOffset.StartKey); err != nil {
						return nil, err
					}
				}
			}
		}
		return item, nil
	}

	return nil, nil
}
