package levels

import (
	"bagh/segment"
	"bagh/value"
)

type Level struct {
	Segments []string
}

// func (l *Level) Deref() []string {
// 	return l.segments
// }

// func (l *Level) DerefMut() []string {
// 	return l.segments
// }

func NewLevel() *Level {
	return &Level{
		Segments: make([]string, 0, 20),
	}
}

type ResolvedLevel struct {
	Segments []segment.Segment
}

func (r *ResolvedLevel) Deref() []segment.Segment {
	return r.Segments
}

func (r *ResolvedLevel) DerefMut() []segment.Segment {
	return r.Segments
}

func NewResolvedLevel(level *Level, hiddenSet *HiddenSet, segmentMap map[string]*segment.Segment) *ResolvedLevel {
	var newLevel []segment.Segment
	for _, segmentID := range level.Segments {
		if !hiddenSet.Contains(segmentID) {
			segment, ok := segmentMap[segmentID]
			if !ok {
				panic("level.go:43")
			}
			newLevel = append(newLevel, *segment)
		}
	}
	return &ResolvedLevel{
		Segments: newLevel,
	}
}

func (r *ResolvedLevel) Size() int64 {
	var totalSize int64
	for _, segment := range r.Segments {
		totalSize += int64(segment.Metadata.FileSize)
	}
	return totalSize
}

func (r *ResolvedLevel) GetOverlappingSegments(start, end value.UserKey) []string {
	var overlappingSegments []string
	st := segment.Bound[value.UserKey]{
		Included: &start,
	}
	ed := segment.Bound[value.UserKey]{
		Included: &end,
	}

	for _, seg := range r.Segments {
		if seg.CheckKeyRangeOverlap(st, ed) {
			overlappingSegments = append(overlappingSegments, seg.Metadata.ID)
		}
	}
	return overlappingSegments
}
