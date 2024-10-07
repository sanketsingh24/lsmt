package segment

import (
	"bagh/descriptor"
	"bagh/value"
	"bytes"
	"container/list"
	"fmt"
)

// Reader struct
type Reader struct {
	DescriptorTable *descriptor.FileDescriptorTable
	BlockIndex      *BlockIndex

	SegmentID  string
	BlockCache *BlockCache

	Blocks    map[string]*list.List
	CurrentLo value.UserKey
	CurrentHi value.UserKey

	StartOffset   value.UserKey
	EndOffset     value.UserKey
	IsInitialized bool
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
		DescriptorTable: descriptorTable,
		SegmentID:       segmentID,
		BlockCache:      blockCache,
		BlockIndex:      blockIndex,
		Blocks:          make(map[string]*list.List),
		StartOffset:     startOffset,
		EndOffset:       endOffset,
	}
}

// initialize initializes the Reader
func (r *Reader) initialize() error {
	if r.StartOffset != nil {
		r.CurrentLo = r.StartOffset
		if err := r.loadBlock(r.StartOffset); err != nil {
			return err
		}
	}

	if r.EndOffset != nil {
		r.CurrentHi = r.EndOffset
		if !bytes.Equal(r.CurrentLo, r.EndOffset) {
			if err := r.loadBlock(r.EndOffset); err != nil {
				return err
			}
		}
	}

	r.IsInitialized = true
	return nil
}

// loadBlock loads a block into memory
func (r *Reader) loadBlock(key value.UserKey) error {
	if r.BlockCache != nil {
		block, err := LoadAndCacheBlockByItemKey(
			r.DescriptorTable,
			r.BlockIndex,
			r.BlockCache,
			r.SegmentID,
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
			r.Blocks[string(key)] = items
			return nil
		}
	} else {
		blockHandle, err := r.BlockIndex.GetLowerBoundBlockInfo(key)
		if err != nil {
			return err
		}
		if blockHandle != nil {
			fileGuard, err := r.DescriptorTable.Access(r.SegmentID)
			if err != nil {
				return err
			}
			if fileGuard == nil {
				return fmt.Errorf("failed to acquire file handle")
			}
			// @TODO: this works??
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
			r.Blocks[string(key)] = items
			return nil
		}
	}
	return nil
}

// Next returns the next item
func (r *Reader) Next() (*value.Value, error) {
	if !r.IsInitialized {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	if r.CurrentLo == nil {
		newBlockOffset, err := r.BlockIndex.GetFirstBlockKey()
		if err != nil {
			return nil, err
		}
		r.CurrentLo = newBlockOffset.StartKey

		if !bytes.Equal(newBlockOffset.StartKey, r.CurrentHi) {
			if err := r.loadBlock(newBlockOffset.StartKey); err != nil {
				return nil, err
			}
		}
	}

	if bytes.Equal(r.CurrentHi, r.CurrentLo) {
		block := r.Blocks[string(r.CurrentLo)]
		if block.Len() > 0 {
			return block.Remove(block.Front()).(*value.Value), nil
		}
		return nil, nil
	}

	block := r.Blocks[string(r.CurrentLo)]
	if block != nil && block.Len() > 0 {
		item := block.Remove(block.Front()).(*value.Value)
		if block.Len() == 0 {
			// why u delete here ? @TODO:
			delete(r.Blocks, string(r.CurrentLo))
			newBlockOffset, err := r.BlockIndex.GetNextBlockKey(r.CurrentLo)
			if err != nil {
				return nil, err
			}
			if newBlockOffset != nil {
				r.CurrentLo = newBlockOffset.StartKey
				if !bytes.Equal(newBlockOffset.StartKey, r.CurrentHi) {
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
	if !r.IsInitialized {
		// should initialize here ? @TODO:
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	if r.CurrentHi == nil {
		newBlockOffset, err := r.BlockIndex.GetLastBlockKey()
		if err != nil {
			return nil, err
		}
		r.CurrentHi = newBlockOffset.StartKey

		if !bytes.Equal(newBlockOffset.StartKey, r.CurrentLo) {
			if err := r.loadBlock(newBlockOffset.StartKey); err != nil {
				return nil, err
			}
		}
	}

	if bytes.Equal(r.CurrentHi, r.CurrentLo) {
		block := r.Blocks[string(r.CurrentHi)]
		if block.Len() > 0 {
			return block.Remove(block.Back()).(*value.Value), nil
		}
		return nil, nil
	}

	block := r.Blocks[string(r.CurrentHi)]
	if block != nil && block.Len() > 0 {
		item := block.Remove(block.Back()).(*value.Value)
		if block.Len() == 0 {
			delete(r.Blocks, string(r.CurrentHi))
			newBlockOffset, err := r.BlockIndex.GetPreviousBlockKey(r.CurrentHi)
			if err != nil {
				return nil, err
			}
			if newBlockOffset != nil {
				r.CurrentHi = newBlockOffset.StartKey
				if !bytes.Equal(newBlockOffset.StartKey, r.CurrentLo) {
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
