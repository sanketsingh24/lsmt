package levels

import "bagh/value"

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
	Segments []Segment
}

func (r *ResolvedLevel) Deref() []Segment {
	return r.Segments
}

func (r *ResolvedLevel) DerefMut() []Segment {
	return r.Segments
}

func NewResolvedLevel(level *Level, hiddenSet *HiddenSet, segmentMap map[string]*Segment) *ResolvedLevel {
	var newLevel []Segment
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
		totalSize += segment.FileSize
	}
	return totalSize
}

func (r *ResolvedLevel) GetOverlappingSegments(start, end value.UserKey) []string {
	var overlappingSegments []string
	for _, segment := range r.Segments {
		if segment.CheckKeyRangeOverlap(start, end) {
			overlappingSegments = append(overlappingSegments, segment.ID)
		}
	}
	return overlappingSegments
}
