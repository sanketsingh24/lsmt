package segment

import (
	"bagh/value"
	"bytes"
	"hash/fnv"
	"sync"
)

type BlockTag int

const (
	Data  BlockTag = 0
	Index BlockTag = 1
)

type Item struct {
	ValueBlock       *ValueBlock
	BlockHandleBlock *BlockHandleBlock
}

type CacheKey struct {
	Tag       BlockTag
	SegmentID string
	UserKey   value.UserKey
}

func (k CacheKey) Equal(other CacheKey) bool {
	return k.Tag == other.Tag && k.SegmentID == other.SegmentID && bytes.Equal(k.UserKey, other.UserKey)

}

func (k CacheKey) Hash() uint64 {
	h := fnv.New64a()
	h.Write([]byte{byte(k.Tag)})
	h.Write([]byte(k.SegmentID))
	h.Write(k.UserKey)
	return h.Sum64()
}

type BlockWeighter struct{}

func (w BlockWeighter) Weight(key CacheKey, item Item) uint32 {
	if item.ValueBlock != nil {
		return uint32(len(item.ValueBlock.Items)) // Assuming ValueBlock has a Data field
	}
	var sum uint32
	for _, i := range item.BlockHandleBlock.Items {
		sum += uint32(len(i.StartKey) + 16) // Assuming BlockHandle is 16 bytes
	}
	return sum
}

/// Block cache, in which blocks are cached in-memory
/// after being retrieved from disk
///
/// This speeds up consecutive queries to nearby data, improving
/// read performance for hot data.
///
/// # Examples
///
/// Sharing block cache between multiple trees
///
/// ```
/// # use lsm_tree::{Tree, Config, BlockCache};
/// # use std::sync::Arc;
/// #
/// // Provide 10'000 blocks 40 MB of cache capacity
/// let block_cache = Arc::new(BlockCache::with_capacity_bytes(40 * 1_000 * 1_000));
///
/// # let folder = tempfile::tempdir()?;
/// let tree1 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// # let folder = tempfile::tempdir()?;
/// let tree2 = Config::new(folder).block_cache(block_cache.clone()).open()?;
/// #
/// # Ok::<(), lsm_tree::Error>(())
/// ```

type BlockCache struct {
	data *sync.Map
	// capacity uint64 @p2
}

func NewBlockCache(capacityBytes uint64) *BlockCache {
	return &BlockCache{
		data: &sync.Map{},
		// capacity: capacityBytes,
	}
}

// func (c *BlockCache) Capacity() uint64 {
// 	return c.capacity
// }

func (c *BlockCache) Len() int {
	length := 0
	c.data.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}

func (c *BlockCache) IsEmpty() bool {
	return c.Len() == 0
}

func (c *BlockCache) InsertDiskBlock(segmentID string, key value.UserKey, value *ValueBlock) {
	// if c.capacity > 0 {
	cacheKey := CacheKey{Tag: Data, SegmentID: segmentID, UserKey: key}
	c.data.Store(cacheKey, Item{ValueBlock: value})
	// }
}

func (c *BlockCache) InsertBlockHandleBlock(segmentID string, key value.UserKey, value *BlockHandleBlock) {
	// if c.capacity > 0 {
	cacheKey := CacheKey{Tag: Index, SegmentID: segmentID, UserKey: key}
	c.data.Store(cacheKey, Item{BlockHandleBlock: value})
	// }
}

func (c *BlockCache) GetDiskBlock(segmentID string, key value.UserKey) *ValueBlock {
	cacheKey := CacheKey{Tag: Data, SegmentID: segmentID, UserKey: key}
	if item, ok := c.data.Load(cacheKey); ok {
		return item.(Item).ValueBlock
	}
	return nil
}

func (c *BlockCache) GetBlockHandleBlock(segmentID string, key value.UserKey) *BlockHandleBlock {
	cacheKey := CacheKey{Tag: Index, SegmentID: segmentID, UserKey: key}
	if item, ok := c.data.Load(cacheKey); ok {
		return item.(Item).BlockHandleBlock
	}
	return nil
}
