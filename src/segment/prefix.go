package segment

import (
	"bagh/descriptor"
	"bagh/value"
)

type PrefixedReader struct {
	descriptorTable *descriptor.FileDescriptorTable
	blockIndex      *BlockIndex
	blockCache      *BlockCache
	segmentID       string

	prefix value.UserKey

	iterator *Range
}

func NewPrefixedReader(
	descriptorTable *descriptor.FileDescriptorTable,
	segmentID string,
	blockCache *BlockCache,
	blockIndex *BlockIndex,
	prefix value.UserKey,
) *PrefixedReader {
	return &PrefixedReader{
		blockCache:      blockCache,
		blockIndex:      blockIndex,
		descriptorTable: descriptorTable,
		segmentID:       segmentID,
		iterator:        nil,
		prefix:          prefix,
	}
}

func (pr *PrefixedReader) initialize() error {
	upperBound, err := pr.blockIndex.GetPrefixUpperBound(pr.prefix)
	if err != nil {
		return err
	}

	var upperBoundBound Bound
	var upperBoundKey value.UserKey
	if upperBound != nil {
		upperBoundBound = Excluded
		upperBoundKey = upperBound.StartKey
	} else {
		upperBoundBound = Unbounded
	}

	iterator := NewRange(
		pr.descriptorTable,
		pr.segmentID,
		pr.blockCache,
		pr.blockIndex,
		Included,
		pr.prefix,
		upperBoundBound,
		upperBoundKey,
	)
	pr.iterator = iterator

	return nil
}

func (pr *PrefixedReader) Next() (*value.Value, error) {
	if pr.iterator == nil {
		if err := pr.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		entry, err := pr.iterator.Next()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, nil
		}

		if entry.Key.Less(pr.prefix) {
			continue
		}

		if !entry.Key.StartsWith(pr.prefix) {
			return nil, nil
		}

		return entry, nil
	}
}

func (pr *PrefixedReader) NextBack() (*value.Value, error) {
	if pr.iterator == nil {
		if err := pr.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		entry, err := pr.iterator.NextBack()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, nil
		}

		if entry.Key.Less(pr.prefix) {
			return nil, nil
		}

		if !entry.Key.StartsWith(pr.prefix) {
			continue
		}

		return entry, nil
	}
}
