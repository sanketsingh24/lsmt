package segment

import (
	"bagh/descriptor"
	"bagh/value"
	"bytes"
)

type PrefixedReader struct {
	DescriptorTable *descriptor.FileDescriptorTable
	BlockIndex      *BlockIndex
	blockCache      *BlockCache
	SegmentID       string

	Prefix value.UserKey

	iterator *Range
}

func NewPrefixedReader(
	DescriptorTable *descriptor.FileDescriptorTable,
	SegmentID string,
	blockCache *BlockCache,
	BlockIndex *BlockIndex,
	Prefix value.UserKey,
) *PrefixedReader {
	return &PrefixedReader{
		blockCache:      blockCache,
		BlockIndex:      BlockIndex,
		DescriptorTable: DescriptorTable,
		SegmentID:       SegmentID,
		iterator:        nil,
		Prefix:          Prefix,
	}
}

// @TODO: optimize lol
func (pr *PrefixedReader) Initialize() error {
	upperB, err := pr.BlockIndex.GetPrefixUpperBound(pr.Prefix)
	if err != nil {
		return err
	}

	var upperBound Bound[value.UserKey]
	if upperB != nil {
		upperBound.Excluded = &upperB.StartKey
	} else {
		upperBound.Unbounded = true
	}
	lowerBound := Bound[value.UserKey]{
		Included: &pr.Prefix,
	}

	iterator := NewRange(
		pr.DescriptorTable,
		pr.SegmentID,
		pr.blockCache,
		pr.BlockIndex,
		lowerBound,
		upperBound,
	)
	pr.iterator = iterator

	return nil
}

func (pr *PrefixedReader) Next() (*value.Value, error) {
	if pr.iterator == nil {
		if err := pr.Initialize(); err != nil {
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

		if bytes.Compare(entry.Key, pr.Prefix) == -1 {
			continue
		}

		if bytes.Compare(entry.Key, pr.Prefix) == 0 {
			// @TODO: reached max keys what to do??? is this even correct?
			return nil, nil
		}

		return entry, nil
	}
}

func (pr *PrefixedReader) NextBack() (*value.Value, error) {
	if pr.iterator == nil {
		if err := pr.Initialize(); err != nil {
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

		if bytes.Compare(entry.Key, pr.Prefix) == -1 {
			return nil, nil
		}

		if bytes.Compare(entry.Key, pr.Prefix) == 0 {
			continue
		}

		return entry, nil
	}
}
