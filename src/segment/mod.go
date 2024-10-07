package segment

import (
	"bytes"
	"fmt"
	"path/filepath"

	"bagh/descriptor"
	"bagh/file"
	"bagh/value"
)

type Segment struct {
	DescriptorTable *descriptor.FileDescriptorTable
	Metadata        *Metadata
	BlockIndex      *BlockIndex
	BlockCache      *BlockCache
}

func (s *Segment) String() string {
	return fmt.Sprintf("Segment:%s", s.Metadata.ID)
}

// @p2: recover from snapshots, doesnt work rn
func RecoverSegment(folder string, blockCache *BlockCache, descriptorTable *descriptor.FileDescriptorTable) (*Segment, error) {
	metadata, err := MetadataFromDisk(filepath.Join(folder, file.SegmentMetadataFile))
	if err != nil {
		return nil, err
	}
	blockIndex := new(BlockIndex)
	if err := blockIndex.FromFile(metadata.ID, descriptorTable, folder, blockCache); err != nil {
		return nil, err
	}

	return &Segment{
		DescriptorTable: descriptorTable,
		Metadata:        metadata,
		BlockIndex:      blockIndex,
		BlockCache:      blockCache,
	}, nil
}

func (s *Segment) Get(key []byte, seqno *value.SeqNo) (*value.Value, error) {
	if seqno != nil {
		if s.Metadata.Seqnos[0] >= *seqno {
			return nil, nil
		}
	}

	if !s.keyRangeContains(key) {
		return nil, nil
	}

	if seqno == nil {
		// Fast path for non-seqno reads
		blockHandle, err := s.BlockIndex.GetLatest(key)
		if err != nil {
			return nil, err
		}
		if blockHandle == nil {
			return nil, nil
		}
		// returns valueblock
		valueBlock, err := LoadAndCacheByBlockHandle(s.DescriptorTable, s.BlockCache, s.Metadata.ID, blockHandle)
		if err != nil {
			return nil, err
		}

		if valueBlock != nil {
			for _, item := range valueBlock.Items {
				if bytes.Equal(item.Key, key) {
					return item.Clone().(*value.Value), nil
				}
			}
		}
		return nil, nil
	} else {
		// Path for seqno-based reads
		blockHandle, err := s.BlockIndex.GetLatest(key)
		if err != nil {
			return nil, err
		}
		if blockHandle != nil {
			valueBlock, err := LoadAndCacheByBlockHandle(s.DescriptorTable, s.BlockCache, s.Metadata.ID, blockHandle)
			if err != nil {
				return nil, err
			}

			if valueBlock != nil {
				for _, item := range valueBlock.Items {
					if bytes.Equal(item.Key, key) && item.SeqNo < *seqno {
						return item.Clone().(*value.Value), nil
					}
				}
			}

			// If not found in the first block, check the next block
			nextBlockHandle, err := s.BlockIndex.GetNextBlockKey(blockHandle.StartKey)
			if err != nil {
				return nil, err
			}
			if nextBlockHandle == nil {
				return nil, nil
			}

			// Placeholder: Implement Reader
			iter := NewReader(s.DescriptorTable, s.Metadata.ID, s.BlockCache, s.BlockIndex, nextBlockHandle.StartKey, nil)

			for {
				item, err := iter.Next()
				if err != nil {
					return nil, err
				}
				if item == nil {
					break
				}

				if !bytes.Equal(item.Key, key) {
					return nil, nil
				}

				if item.SeqNo < *seqno {
					return item.Clone().(*value.Value), nil
				}
			}
		}
	}

	return nil, nil
}

// @TODO: iterator??? returning reader???? tf
func (s *Segment) Iter(useCache bool) *Reader {
	var cache *BlockCache
	if useCache {
		cache = s.BlockCache
	}

	return NewReader(s.DescriptorTable, s.Metadata.ID, cache, s.BlockIndex, nil, nil)
}

func (s *Segment) Range(start, end Bound[value.UserKey]) *Range {
	// startBound := BoundedKey{
	// 	Key:  *start,
	// 	Type: Included,
	// }
	// endBound := BoundedKey{
	// 	Key:  *end,
	// 	Type: Included,
	// }
	return NewRange(s.DescriptorTable, s.Metadata.ID, s.BlockCache, s.BlockIndex, start, end)
}

func (s *Segment) Prefix(prefix []byte) *PrefixedReader {
	return NewPrefixedReader(s.DescriptorTable, s.Metadata.ID, s.BlockCache, s.BlockIndex, prefix)
}

func (s *Segment) GetLSN() value.SeqNo {
	return s.Metadata.Seqnos[1]
}

func (s *Segment) TombstoneCount() uint64 {
	return s.Metadata.TombstoneCount
}

func (s *Segment) keyRangeContains(key []byte) bool {
	return s.Metadata.KeyRangeContains(key)
}

func (s *Segment) CheckPrefixOverlap(prefix []byte) bool {
	return s.Metadata.CheckPrefixOverlap(prefix)
}

// @TODO: check if works
func (s *Segment) CheckKeyRangeOverlap(lo, hi Bound[value.UserKey]) bool {
	segmentLo := s.Metadata.KeyRange[0]
	segmentHi := s.Metadata.KeyRange[1]

	// If both bounds are unbounded, the range overlaps with everything
	if lo.Unbounded == true && hi.Unbounded == true {
		return true
	}

	// If upper bound is unbounded
	if hi.Unbounded == true {
		if lo.Unbounded == true {
			panic("Invalid key range check")
		}
		if lo.Included != nil {
			return bytes.Compare(*lo.Included, segmentHi) <= 0
		}
		return bytes.Compare(*lo.Excluded, segmentHi) < 0
	}

	// If lower bound is unbounded
	if lo.Unbounded == true {
		if hi.Unbounded == true {
			panic("Invalid key range check")
		}
		if hi.Included != nil {
			return bytes.Compare(*hi.Included, segmentLo) >= 0
		}
		return bytes.Compare(*hi.Excluded, segmentLo) > 0
	}

	// Both bounds are bounded
	loIncluded := false
	if lo.Included != nil {
		loIncluded = bytes.Compare(*lo.Included, segmentHi) <= 0
	} else {
		loIncluded = bytes.Compare(*lo.Excluded, segmentHi) < 0
	}

	hiIncluded := false
	if hi.Included != nil {
		hiIncluded = bytes.Compare(*hi.Included, segmentLo) >= 0
	} else {
		hiIncluded = bytes.Compare(*hi.Excluded, segmentLo) > 0
	}

	return loIncluded && hiIncluded
}
