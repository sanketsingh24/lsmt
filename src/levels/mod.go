package levels

import (
	"bagh/segment"
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

type HiddenSet struct {
	Set map[string]struct{}
}

func (h *HiddenSet) Contains(key string) bool {
	_, ok := h.Set[key]
	return ok
}

func (h *HiddenSet) Add(key string) {
	h.Set[key] = struct{}{}
}

func (h *HiddenSet) Remove(key string) {
	delete(h.Set, key)
}

type ResolvedView struct {
	ResolvedLevel []ResolvedLevel
}

type Levels struct {
	Path string

	Segments  map[string]*segment.Segment
	Levels    []*Level
	HiddenSet *HiddenSet
	// segmentHistory *segment.SegmentHistoryWriter
}

func NewLevels(levelCount uint8, path string) (*Levels, error) {
	if levelCount == 0 {
		return nil, fmt.Errorf("level_count should be >= 1")
	}

	levels := make([]*Level, levelCount)
	for i := range levels {
		levels[i] = &Level{}
	}

	l := &Levels{
		Path:      path,
		Segments:  make(map[string]*segment.Segment, 100),
		Levels:    levels,
		HiddenSet: &HiddenSet{Set: make(map[string]struct{}, 10)},
		// segmentHistory: NewSegmentHistoryWriter(),
	}
	if err := l.writeToDisk(); err != nil {
		return nil, err
	}

	if err := l.writeSegmentHistoryEntry("create_new"); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Levels) IsCompacting() bool {
	return len(l.HiddenSet.Set) > 0
}

func (l *Levels) RecoverIds(path string) ([]string, error) {
	manifest, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var levels []*Level
	if err := json.Unmarshal(manifest, &levels); err != nil {
		return nil, err
	}

	var ids []string
	for _, level := range levels {
		ids = append(ids, level.Segments...)
	}
	return ids, nil
}

func (l *Levels) Recover(path string, segments []*segment.Segment) (*Levels, error) {
	manifest, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var levels []*Level
	if err := json.Unmarshal(manifest, &levels); err != nil {
		return nil, err
	}

	segmentMap := make(map[string]*segment.Segment, len(segments))
	for _, segment := range segments {
		segmentMap[segment.Metadata.ID] = segment
	}

	level := &Levels{
		Segments:  segmentMap,
		Levels:    levels,
		HiddenSet: &HiddenSet{Set: make(map[string]struct{}, 10)},
		Path:      path,
		// SegmentHistory: NewSegmentHistoryWriter(),
	}

	return level, nil
}

func (l *Levels) WriteToDisk() error {
	manifest, err := json.MarshalIndent(l.Levels, "", "  ")
	if err != nil {
		return err
	}

	// Rewrite the file atomically
	tempPath := l.Path + ".tmp"
	if err := os.WriteFile(tempPath, manifest, 0644); err != nil {
		return err
	}
	if err := os.Rename(tempPath, l.Path); err != nil {
		return err
	}

	return nil
}

func (l *Levels) Add(segment *segment.Segment) {
	l.insertIntoLevel(0, segment)
}

func (l *Levels) SortLevels() {
	for _, level := range l.Levels {
		sort.Slice(level.Segments, func(i, j int) bool {
			segA := l.Segments[level.Segments[i]]
			segB := l.Segments[level.Segments[j]]
			return segB.Metadata.Seqnos.Second > segA.Metadata.Seqnos.Second
		})
	}
}

func (l *Levels) InsertIntoLevel(levelNo uint8, segment *segment.Segment) {
	lastLevelIndex := len(l.Levels) - 1
	index := int(levelNo.Clamp(0, uint8(lastLevelIndex)))

	level := l.Levels[index]
	level.Segments = append(level.Segments, segment.Metadata.ID)
	l.Segments[segment.Metadata.ID] = segment

	l.SortLevels()

	// if err := l.writeSegmentHistoryEntry("insert"); err != nil {
	// 	// Log the error and continue
	// }
}

func (l *Levels) Remove(segmentID string) {
	for _, level := range l.Levels {
		var newSegments []string
		for _, id := range level.Segments {
			if id != segmentID {
				newSegments = append(newSegments, id)
			}
		}
		level.Segments = newSegments
	}
	delete(l.Segments, segmentID)

	// if err := l.writeSegmentHistoryEntry("remove"); err != nil {
	// 	// Log the error and continue
	// }
}

func (l *Levels) IsEmpty() bool {
	return l.Len() == 0
}

func (l *Levels) Depth() uint8 {
	return uint8(len(l.Levels))
}

func (l *Levels) FirstLevelSegmentCount() int {
	return len(l.Levels[0].Segments)
}

func (l *Levels) LastLevelIndex() uint8 {
	return l.Depth() - 1
}

func (l *Levels) Len() int {
	var total int
	for _, level := range l.Levels {
		total += len(level.Segments)
	}
	return total
}

func (l *Levels) Size() int64 {
	var totalSize int64
	for _, segment := range l.getAllSegmentsFlattened() {
		totalSize += segment.Metadata.FileSize
	}
	return totalSize
}

func (l *Levels) BusyLevels() map[uint8]struct{} {
	busyLevels := make(map[uint8]struct{})
	for i, level := range l.Levels {
		for _, segmentID := range level.Segments {
			if l.HiddenSet.Contains(segmentID) {
				busyLevels[uint8(i)] = struct{}{}
				break
			}
		}
	}
	return busyLevels
}

func (l *Levels) ResolvedView() []*ResolvedLevel {
	var output []*ResolvedLevel
	for _, rawLevel := range l.Levels {
		output = append(output, NewResolvedLevel(rawLevel, l.HiddenSet, l.Segments))
	}
	return output
}

func (l *Levels) getAllSegmentsFlattened() []*segment.Segment {
	var segments []*segment.Segment
	for _, level := range l.Levels {
		for _, segmentID := range level.Segments {
			segment, ok := l.Segments[segmentID]
			if !ok {
				panic("where's the segment at?")
			}
			segments = append(segments, segment)
		}
	}
	return segments
}

func (l *Levels) getAllSegments() map[string]*segment.Segment {
	segmentMap := make(map[string]*segment.Segment, l.Len())
	for _, segment := range l.getAllSegmentsFlattened() {
		segmentMap[segment.Metadata.ID] = segment
	}
	return segmentMap
}

func (l *Levels) getSegments() map[string]*segment.Segment {
	allSegments := l.getAllSegments()
	segments := make(map[string]*segment.Segment, len(allSegments))
	for id, segment := range allSegments {
		if !l.HiddenSet.Contains(id) {
			segments[id] = segment
		}
	}
	return segments
}

func (l *Levels) ShowSegments(keys []string) {
	for _, key := range keys {
		l.HiddenSet.Remove(key)
	}
}

func (l *Levels) HideSegments(keys []string) {
	for _, key := range keys {
		l.HiddenSet.Add(key)
	}
}
