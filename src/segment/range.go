package segment

import (
	"bagh/descriptor"
	value "bagh/value"
	"bytes"
)

type Bound int

const (
	Unbounded Bound = iota
	Included
	Excluded
)

type BoundedKey struct {
	Key  value.UserKey
	Type Bound
}

type Range struct {
	descriptorTable *descriptor.FileDescriptorTable
	blockIndex      *BlockIndex
	blockCache      *BlockCache
	segmentID       string

	rangeBounds struct {
		start BoundedKey
		end   BoundedKey
	}

	iterator *Reader
}

func NewRange(
	descriptorTable *descriptor.FileDescriptorTable,
	segmentID string,
	blockCache *BlockCache,
	blockIndex *BlockIndex,
	start BoundedKey,
	end BoundedKey,
) *Range {
	return &Range{
		descriptorTable: descriptorTable,
		blockCache:      blockCache,
		blockIndex:      blockIndex,
		segmentID:       segmentID,
		rangeBounds: struct {
			start BoundedKey
			end   BoundedKey
		}{
			start: start,
			end:   end,
		},
		iterator: nil,
	}
}

func (r *Range) initialize() error {
	var offsetLo, offsetHi *value.UserKey

	switch r.rangeBounds.start.Type {
	case Unbounded:
		offsetLo = nil
	case Included, Excluded:
		info, err := r.blockIndex.GetLowerBoundBlockInfo(r.rangeBounds.start.Key)
		if err != nil {
			return err
		}
		if info != nil {
			offsetLo = &info.StartKey
		}
	}

	switch r.rangeBounds.end.Type {
	case Unbounded:
		offsetHi = nil
	case Included, Excluded:
		info, err := r.blockIndex.GetUpperBoundBlockInfo(r.rangeBounds.end.Key)
		if err != nil {
			return err
		}
		if info != nil {
			offsetHi = &info.StartKey
		}
	}

	reader := NewReader(
		r.descriptorTable,
		r.segmentID,
		r.blockCache,
		r.blockIndex,
		offsetLo,
		offsetHi,
	)
	r.iterator = reader

	return nil
}

func (r *Range) Next() (*value.Value, error) {
	if r.iterator == nil {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		entry, err := r.iterator.Next()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, nil
		}

		switch r.rangeBounds.start.Type {
		case Included:
			if bytes.Compare(entry.Key, r.rangeBounds.start.Key) < 0 {
				continue
			}
		case Excluded:
			if bytes.Compare(entry.Key, r.rangeBounds.start.Key) <= 0 {
				continue
			}
		}

		switch r.rangeBounds.end.Type {
		case Included:
			if bytes.Compare(entry.Key, r.rangeBounds.end.Key) > 0 {
				return nil, nil
			}
		case Excluded:
			if bytes.Compare(entry.Key, r.rangeBounds.end.Key) >= 0 {
				return nil, nil
			}
		}

		return entry, nil
	}
}

func (r *Range) NextBack() (*value.Value, error) {
	if r.iterator == nil {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		entry, err := r.iterator.NextBack()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, nil
		}

		switch r.rangeBounds.start.Type {
		case Included:
			if bytes.Compare(entry.Key, r.rangeBounds.start.Key) < 0 {
				return nil, nil
			}
		case Excluded:
			if bytes.Compare(entry.Key, r.rangeBounds.start.Key) <= 0 {
				return nil, nil
			}
		}

		switch r.rangeBounds.end.Type {
		case Included:
			if bytes.Compare(entry.Key, r.rangeBounds.end.Key) > 0 {
				continue
			}
		case Excluded:
			if bytes.Compare(entry.Key, r.rangeBounds.end.Key) >= 0 {
				continue
			}
		}

		return entry, nil
	}
}
