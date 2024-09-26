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
	descriptorTable *descriptor.FileDescriptorTable
	metadata        *Metadata
	blockIndex      *BlockIndex
	blockCache      *BlockCache
}

func (s *Segment) String() string {
	return fmt.Sprintf("Segment:%s", s.metadata.ID)
}

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
		descriptorTable: descriptorTable,
		metadata:        metadata,
		blockIndex:      blockIndex,
		blockCache:      blockCache,
	}, nil
}

func (s *Segment) Get(key []byte, seqno *value.SeqNo) (*value.Value, error) {
	if seqno != nil {
		if s.metadata.Seqnos[0] >= *seqno {
			return nil, nil
		}
	}

	if !s.keyRangeContains(key) {
		return nil, nil
	}

	if seqno == nil {
		// Fast path for non-seqno reads
		blockHandle, err := s.blockIndex.GetLatest(key)
		if err != nil {
			return nil, err
		}
		if blockHandle == nil {
			return nil, nil
		}

		block, err := block.LoadAndCacheByBlockHandle(s.descriptorTable, s.blockCache, s.metadata.ID, blockHandle)
		if err != nil {
			return nil, err
		}

		if block != nil {
			for _, item := range block.Items {
				if bytes.Equal(item.Key, key) {
					return item.Clone(), nil
				}
			}
		}
		return nil, nil
	} else {
		// Path for seqno-based reads
		blockHandle, err := s.blockIndex.GetLatest(key)
		if err != nil {
			return nil, err
		}
		if blockHandle != nil {
			block, err := block.LoadAndCacheByBlockHandle(s.descriptorTable, s.blockCache, s.metadata.ID, blockHandle)
			if err != nil {
				return nil, err
			}

			if block != nil {
				for _, item := range block.Items {
					if bytes.Equal(item.Key, key) && item.Seqno < *seqno {
						return item.Clone(), nil
					}
				}
			}

			// If not found in the first block, check the next block
			nextBlockHandle, err := s.blockIndex.GetNextBlockKey(blockHandle.StartKey)
			if err != nil {
				return nil, err
			}
			if nextBlockHandle == nil {
				return nil, nil
			}

			// Placeholder: Implement Reader
			iter := NewReader(s.descriptorTable, s.metadata.ID, s.blockCache, s.blockIndex, nextBlockHandle.StartKey, nil)

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

				if item.Seqno < *seqno {
					return item.Clone(), nil
				}
			}
		}
	}

	return nil, nil
}

func (s *Segment) Iter(useCache bool) *reader.Reader {
	var cache *blockcache.BlockCache
	if useCache {
		cache = s.blockCache
	}

	return reader.NewReader(s.descriptorTable, s.metadata.ID, cache, s.blockIndex, nil, nil)
}

func (s *Segment) Range(start, end *value.UserKey) *Range {
	return NewRange(s.descriptorTable, s.metadata.ID, s.blockCache, s.blockIndex, start, end)
}

func (s *Segment) Prefix(prefix []byte) *prefix.PrefixedReader {
	return prefix.NewPrefixedReader(s.descriptorTable, s.metadata.ID, s.blockCache, s.blockIndex, prefix)
}

func (s *Segment) GetLSN() value.SeqNo {
	return s.metadata.Seqnos[1]
}

func (s *Segment) TombstoneCount() uint64 {
	return s.metadata.TombstoneCount
}

func (s *Segment) keyRangeContains(key []byte) bool {
	return s.metadata.KeyRangeContains(key)
}

func (s *Segment) CheckPrefixOverlap(prefix []byte) bool {
	return s.metadata.CheckPrefixOverlap(prefix)
}

func (s *Segment) CheckKeyRangeOverlap(lo, hi Bound) bool {
	segmentLo := s.metadata.KeyRange[0]
	segmentHi := s.metadata.KeyRange[1]

	// If both bounds are unbounded, the range overlaps with everything
	if lo == Unbounded && hi == Unbounded {
		return true
	}

	// If upper bound is unbounded
	if hi == Unbounded {
		if lo == Unbounded {
			panic("Invalid key range check")
		}
		if lo.Inclusive {
			return bytes.Compare(lo.Value, segmentHi) <= 0
		}
		return bytes.Compare(lo.Value, segmentHi) < 0
	}

	// If lower bound is unbounded
	if lo == Unbounded {
		if hi == Unbounded {
			panic("Invalid key range check")
		}
		if hi.Inclusive {
			return bytes.Compare(hi.Value, segmentLo) >= 0
		}
		return bytes.Compare(hi.Value, segmentLo) > 0
	}

	// Both bounds are bounded
	loIncluded := false
	if lo.Inclusive {
		loIncluded = bytes.Compare(lo.Value, segmentHi) <= 0
	} else {
		loIncluded = bytes.Compare(lo.Value, segmentHi) < 0
	}

	hiIncluded := false
	if hi.Inclusive {
		hiIncluded = bytes.Compare(hi.Value, segmentLo) >= 0
	} else {
		hiIncluded = bytes.Compare(hi.Value, segmentLo) > 0
	}

	return loIncluded && hiIncluded
}
