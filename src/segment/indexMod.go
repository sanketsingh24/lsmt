package segment

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"bagh/descriptor"
	"bagh/disk"
	"bagh/file"
	"bagh/value"
)

type BlockHandleBlock struct {
	disk.DiskBlock[BlockHandle]
}

func (bhb *BlockHandleBlock) GetPreviousBlockInfo(key []byte) *BlockHandle {
	for i := len(bhb.Items) - 1; i >= 0; i-- {
		if bytes.Compare(bhb.Items[i].StartKey, key) < 0 {
			return &bhb.Items[i]
		}
	}
	return nil
}

func (bhb *BlockHandleBlock) GetNextBlockInfo(key []byte) *BlockHandle {
	for _, item := range bhb.Items {
		if bytes.Compare(item.StartKey, key) > 0 {
			return &item
		}
	}
	return nil
}

func (bhb *BlockHandleBlock) GetLowerBoundBlockInfo(key []byte) *BlockHandle {
	for i := len(bhb.Items) - 1; i >= 0; i-- {
		if bytes.Compare(bhb.Items[i].StartKey, key) <= 0 {
			return &bhb.Items[i]
		}
	}
	return nil
}

type BlockHandleBlockIndex struct {
	cache *BlockCache
}

func (bhbi *BlockHandleBlockIndex) Insert(segmentID string, key value.UserKey, value *BlockHandleBlock) {
	bhbi.cache.InsertBlockHandleBlock(segmentID, key, value)
}

func (bhbi *BlockHandleBlockIndex) Get(segmentID string, key value.UserKey) *BlockHandleBlock {
	return bhbi.cache.GetBlockHandleBlock(segmentID, key)
}

type BlockIndex struct {
	descriptorTable *descriptor.FileDescriptorTable
	segmentID       string
	topLevelIndex   *TopLevelIndex
	blocks          *BlockHandleBlockIndex
}

func (bi *BlockIndex) GetPrefixUpperBound(key []byte) (*BlockHandle, error) {
	blockKey, blockHandle, found := bi.topLevelIndex.GetPrefixUpperBound(key)
	if found == false {
		return nil, nil
	}

	indexBlock, err := bi.LoadAndCacheIndexBlock(blockKey, blockHandle)
	if err != nil {
		return nil, err
	}

	if len(indexBlock.Items) > 0 {
		return &indexBlock.Items[0], nil
	}
	return nil, nil
}

func (bi *BlockIndex) GetUpperBoundBlockInfo(key []byte) (*BlockHandle, error) {
	blockKey, blockHandle, found := bi.topLevelIndex.GetBlockContainingItem(key)
	if found == false {
		return nil, nil
	}

	indexBlock, err := bi.LoadAndCacheIndexBlock(blockKey, blockHandle)
	if err != nil {
		return nil, err
	}

	nextBlock := indexBlock.GetNextBlockInfo(key)
	if nextBlock != nil {
		return nextBlock, nil
	}

	nextBlockKey, nextBlockHandle, found := bi.topLevelIndex.GetNextBlockHandle(key)
	if found == false {
		return nil, nil
	}

	return &BlockHandle{
		Offset:   nextBlockHandle.Offset,
		Size:     nextBlockHandle.Size,
		StartKey: nextBlockKey,
	}, nil
}

func (bi *BlockIndex) GetLowerBoundBlockInfo(key []byte) (*BlockHandle, error) {
	blockKey, blockHandle, found := bi.topLevelIndex.GetBlockContainingItem(key)
	if found == false {
		return nil, nil
	}

	indexBlock, err := bi.LoadAndCacheIndexBlock(blockKey, blockHandle)
	if err != nil {
		return nil, err
	}

	return indexBlock.GetLowerBoundBlockInfo(key), nil
}

func (b *BlockIndex) GetPreviousBlockKey(key []byte) (*BlockHandle, error) {
	firstBlockKey, firstBlockHandle, found := b.topLevelIndex.GetBlockContainingItem(key)
	if found == false {
		return nil, nil
	}

	indexBlock, err := b.LoadAndCacheIndexBlock(firstBlockKey, firstBlockHandle)
	if err != nil {
		return nil, err
	}

	maybePrev := indexBlock.GetPreviousBlockInfo(key)

	// @TODO: : Might have to clone here, not sure and all subsequent places in this file
	if maybePrev != nil {
		return maybePrev, nil
	}

	prevBlockKey, prevBlockHandle, found := b.topLevelIndex.GetPreviousBlockHandle(firstBlockKey)
	if found == false {
		return nil, nil
	}

	indexBlock, err = b.LoadAndCacheIndexBlock(prevBlockKey, prevBlockHandle)
	if err != nil {
		return nil, err
	}

	// @TODO: : Might have to clone here, not sure
	return &indexBlock.Items[len(indexBlock.Items)-1], nil
}

func (b *BlockIndex) GetNextBlockKey(key []byte) (*BlockHandle, error) {
	firstBlockKey, firstBlockHandle, found := b.topLevelIndex.GetBlockContainingItem(key)
	if found == false {
		return nil, nil
	}

	indexBlock, err := b.LoadAndCacheIndexBlock(firstBlockKey, firstBlockHandle)
	if err != nil {
		return nil, err
	}

	maybeNext := indexBlock.GetNextBlockInfo(key)

	if maybeNext != nil {
		return maybeNext, nil
	}

	nextBlockKey, nextBlockHandle, found := b.topLevelIndex.GetNextBlockHandle(firstBlockKey)
	if found == false {
		return nil, nil
	}

	indexBlock, err = b.LoadAndCacheIndexBlock(nextBlockKey, nextBlockHandle)
	if err != nil {
		return nil, err
	}

	return &indexBlock.Items[0], nil
}

func (b *BlockIndex) GetFirstBlockKey() (*BlockHandle, error) {
	blockKey, blockHandle := b.topLevelIndex.GetFirstBlockHandle()
	indexBlock, err := b.LoadAndCacheIndexBlock(blockKey, blockHandle)
	if err != nil {
		return nil, err
	}

	if len(indexBlock.Items) == 0 {
		return nil, fmt.Errorf("block should not be empty")
	}

	return &indexBlock.Items[0], nil
}

func (b *BlockIndex) GetLastBlockKey() (*BlockHandle, error) {
	blockKey, blockHandle := b.topLevelIndex.GetLastBlockHandle()
	indexBlock, err := b.LoadAndCacheIndexBlock(blockKey, blockHandle)
	if err != nil {
		return nil, err
	}

	if len(indexBlock.Items) == 0 {
		return nil, fmt.Errorf("block should not be empty")
	}

	return &indexBlock.Items[len(indexBlock.Items)-1], nil
}

// loads a block from disk
func (b *BlockIndex) LoadAndCacheIndexBlock(blockKey []byte, blockHandle *BlockHandleBlockHandle) (*BlockHandleBlock, error) {
	if block := b.blocks.Get(b.segmentID, blockKey); block != nil {
		// cache hit, so return :)
		return block, nil
	}
	// cache miss, load from disk :(
	fileGuard, err := b.descriptorTable.Access(b.segmentID)
	if err != nil {
		return nil, err
	}
	defer fileGuard.Release() // defer or release earlier? @TODO:
	db := new(BlockHandleBlock)

	if err := db.FromFileCompressed(fileGuard.File(), int64(blockHandle.Offset), blockHandle.Size); err != nil {
		return nil, err
	}

	b.blocks.Insert(b.segmentID, blockKey, db)

	return db, nil
}

func (b *BlockIndex) GetLatest(key []byte) (*BlockHandle, error) {
	blockKey, indexBlockHandle, found := b.topLevelIndex.GetBlockContainingItem(key)
	if found == false {
		return nil, nil
	}

	indexBlock, err := b.LoadAndCacheIndexBlock(blockKey, indexBlockHandle)
	if err != nil {
		return nil, err
	}

	return indexBlock.GetLowerBoundBlockInfo(key), nil
}

func NewBlockIndex(segmentID string, blockCache *BlockCache) *BlockIndex {
	indexBlockIndex := &BlockHandleBlockIndex{
		cache: blockCache,
	}

	return &BlockIndex{
		descriptorTable: descriptor.NewFileDescriptorTable(512, 1),
		segmentID:       segmentID,
		blocks:          indexBlockIndex,
		topLevelIndex:   NewTopLevelIndex(make(map[string]*BlockHandleBlockHandle)),
	}
}

func (b *BlockIndex) FromFile(segmentID string, descriptorTable *descriptor.FileDescriptorTable, path string, blockCache *BlockCache) error {
	log.Printf("Reading block index from %s", path)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return err
	}
	if _, err := os.Stat(filepath.Join(path, file.TopLevelIndexFile)); os.IsNotExist(err) {
		return err
	}
	if _, err := os.Stat(filepath.Join(path, file.BlocksFile)); os.IsNotExist(err) {
		return err
	}

	fileInfo, err := os.Stat(filepath.Join(path, file.TopLevelIndexFile))
	if err != nil {
		return err
	}

	file, err := os.Open(filepath.Join(path, file.TopLevelIndexFile))
	if err != nil {
		return err
	}
	defer file.Close()
	indexBlock := new(BlockHandleBlock)
	if err := indexBlock.FromFileCompressed(file, 0, uint32(fileInfo.Size())); err != nil {
		return err
	}

	if len(indexBlock.Items) == 0 {
		return fmt.Errorf("index is empty")
	}
	// @P2: using normal map, should use some red black tree for faster range queries
	tree := make(map[string]*BlockHandleBlockHandle)
	for _, item := range indexBlock.Items {
		tree[string(item.StartKey)] = &BlockHandleBlockHandle{
			Offset: item.Offset,
			Size:   item.Size,
		}
	}

	b.descriptorTable = descriptorTable
	b.blocks = &BlockHandleBlockIndex{blockCache}
	b.topLevelIndex = NewTopLevelIndex(tree)
	b.segmentID = segmentID

	//  = &BlockIndex{
	// 	descriptorTable: descriptorTable,
	// 	segmentID:       segmentID,
	// 	topLevelIndex:   NewTopLevelIndex(tree),
	// 	blocks:          &BlockHandleBlockIndex{blockCache},
	// }
	return nil
}
