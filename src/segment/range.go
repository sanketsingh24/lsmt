package segment

import (
	"bagh/descriptor"
	"bagh/value"
	"bytes"
)

// type Bound int

// const (
// 	Unbounded Bound = iota
// 	Included
// 	Excluded
// )

// @TODO: modify to this
// type Bound[T any] struct {
// }

type Bound[T any] struct {
	Included  *T
	Excluded  *T
	Unbounded bool
}

type Range struct {
	DescriptorTable *descriptor.FileDescriptorTable
	BlockIndex      *BlockIndex
	BlockCache      *BlockCache
	SegmentID       string

	Start Bound[value.UserKey]
	End   Bound[value.UserKey]

	Iterator *Reader
}

func NewRange(
	DescriptorTable *descriptor.FileDescriptorTable,
	SegmentID string,
	BlockCache *BlockCache,
	BlockIndex *BlockIndex,
	start Bound[value.UserKey],
	end Bound[value.UserKey],
) *Range {
	return &Range{
		DescriptorTable: DescriptorTable,
		BlockCache:      BlockCache,
		BlockIndex:      BlockIndex,
		SegmentID:       SegmentID,
		Start:           start,
		End:             end,
		Iterator:        nil,
	}
}

// @TODO: idk if this works
func (r *Range) initialize() error {
	var offsetLo, offsetHi value.UserKey

	if !r.Start.Unbounded {
		var startKey *value.UserKey
		if r.Start.Included != nil {
			startKey = r.Start.Included
		} else if r.Start.Excluded != nil {
			startKey = r.Start.Excluded
		}
		if startKey != nil {
			blockInfo, err := r.BlockIndex.GetLowerBoundBlockInfo(*startKey)
			if err != nil {
				return err
			}
			if blockInfo != nil {
				offsetLo = blockInfo.StartKey
			}
		}
	}

	if !r.End.Unbounded {
		var endKey *value.UserKey
		if r.End.Included != nil {
			endKey = r.End.Included
		} else if r.End.Excluded != nil {
			endKey = r.End.Excluded
		}
		if endKey != nil {
			blockInfo, err := r.BlockIndex.GetUpperBoundBlockInfo(*endKey)
			if err != nil {
				return err
			}
			if blockInfo != nil {
				offsetHi = blockInfo.StartKey
			}
		}
	}

	reader := NewReader(
		r.DescriptorTable,
		r.SegmentID,
		r.BlockCache,
		r.BlockIndex,
		offsetLo,
		offsetHi,
	)
	r.Iterator = reader

	return nil
}

func (r *Range) Next() (*value.Value, error) {
	if r.Iterator == nil {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		entry, err := r.Iterator.Next()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, nil
		}

		if !r.Start.Unbounded {
			if r.Start.Included != nil {
				if bytes.Compare(entry.Key, *r.Start.Included) < 0 {
					// Before min key
					continue
				}
			} else if r.Start.Excluded != nil {
				if bytes.Compare(entry.Key, *r.Start.Excluded) <= 0 {
					// Before or equal min key
					continue
				}
			}
		}

		if !r.End.Unbounded {
			if r.End.Included != nil {
				if bytes.Compare(entry.Key, *r.Start.Included) > 0 {
					// After max key
					return nil, nil
				}
			} else if r.End.Excluded != nil {
				if bytes.Compare(entry.Key, *r.Start.Excluded) >= 0 {
					// Reached max key
					return nil, nil
				}
			}
		}

		return entry, nil
	}
}

func (r *Range) NextBack() (*value.Value, error) {
	if r.Iterator == nil {
		if err := r.initialize(); err != nil {
			return nil, err
		}
	}

	for {
		entry, err := r.Iterator.NextBack()
		if err != nil {
			return nil, err
		}
		if entry == nil {
			return nil, nil
		}

		if !r.Start.Unbounded {
			if r.Start.Included != nil {
				if bytes.Compare(entry.Key, *r.Start.Included) < 0 {
					// Reached min key
					return nil, nil
				}
			} else if r.Start.Excluded != nil {
				if bytes.Compare(entry.Key, *r.Start.Excluded) <= 0 {
					// Before min key
					return nil, nil
				}
			}
		}

		if !r.End.Unbounded {
			if r.End.Included != nil {
				if bytes.Compare(entry.Key, *r.Start.Included) > 0 {
					// After max key
					continue
				}
			} else if r.End.Excluded != nil {
				if bytes.Compare(entry.Key, *r.Start.Excluded) >= 0 {
					// After or equal max key
					continue
				}
			}
		}

		return entry, nil
	}
}
