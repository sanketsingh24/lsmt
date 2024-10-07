package tree

import (
	"bagh/config"
	"bagh/descriptor"
	"bagh/file"
	"bagh/flush"
	"bagh/id"
	"bagh/levels"
	"bagh/memtable"
	"bagh/prefix"
	"bagh/ranger"
	"bagh/segment"
	"bagh/stop"
	"bagh/value"
	"bagh/version"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type Tree struct {
	TreeInner *TreeInner
}

/// Opens an LSM-tree in the given directory.
///
/// Will recover previous state if the folder was previously
/// occupied by an LSM-tree, including the previous configuration.
/// If not, a new tree will be initialized with the given config.
///
/// After recovering a previous state, use [`Tree::set_active_memtable`]
/// to fill the memtable with data from a write-ahead log for full durability.
///
/// # Errors
///
/// Returns error, if an IO error occured.

func Open(config config.Config) (*Tree, error) {
	fmt.Println("Opening LSM-tree at %s", config.Inner.Path)

	var tree *Tree
	var err error

	if _, err := os.Stat(filepath.Join(config.Inner.Path, file.LSMMarker)); err == nil {
		tree, err = Recover(config.Inner.Path, config.BlockCache, config.DescriptorTable)
	} else if os.IsNotExist(err) {
		tree, err = CreateNew(config)
	}

	if err != nil {
		return nil, err
	}

	return tree, nil
}

// func (t *Tree) Compact(strategy CompactionStrategy) error {
// 	options := &CompactionOptions{
// 		Config:          t.TreeInner.Config.Clone(),
// 		SealedMemtables: t.TreeInner.SealedMemtables.Clone(),
// 		Levels:          t.TreeInner.Levels.Clone(),
// 		OpenSnapshots:   t.TreeInner.OpenSnapshots.Clone(),
// 		StopSignal:      t.TreeInner.StopSignal.Clone(),
// 		BlockCache:      t.TreeInner.BlockCache.Clone(),
// 		Strategy:        strategy,
// 		DescriptorTable: t.TreeInner.DescriptorTable.Clone(),
// 	}
// 	if err := doCompaction(options); err != nil {
// 		return err
// 	}
// 	fmt.Printf("lsm-tree: compaction run over")
// 	return nil
// }

// func (t *Tree) MajorCompact(targetSize uint64) error {
// 	fmt.Printf("Starting major compaction")
// 	strategy := NewMajorCompactionStrategy(targetSize)
// 	return t.Compact(strategy)
// }

func (t *Tree) Snapshot(seqno value.SeqNo) *Snapshot {
	return NewSnapshot(t, seqno)
}

func (t *Tree) RegisterSegments(segments []segment.Segment) error {
	t.TreeInner.LevelsMutex.Lock()
	t.TreeInner.SealedMutex.Lock()
	defer t.TreeInner.LevelsMutex.Unlock()
	defer t.TreeInner.SealedMutex.Unlock()

	for _, segment := range segments {
		t.TreeInner.Levels.Add(&segment)
	}

	for _, segment := range segments {
		delete(t.TreeInner.SealedMemtables, segment.Metadata.ID)
	}

	if err := t.TreeInner.Levels.WriteToDisk(); err != nil {
		return err
	}

	return nil
}

func (t *Tree) FlushActiveMemtable() (string, error) {
	fmt.Println("flush: flushing active memtable")

	segmentID, yankedMemtable := t.RotateMemtable()
	if segmentID == nil || yankedMemtable == nil {
		return "", nil
	}

	segmentFolder := filepath.Join(t.TreeInner.Config.Path, file.SegmentsFolder)
	fmt.Printf("flush: writing segment to %s", segmentFolder)

	sg, err := flush.FlushToSegment(flush.Options{
		BlockCache:      t.TreeInner.BlockCache,
		BlockSize:       t.TreeInner.Config.BlockSize,
		Folder:          segmentFolder,
		SegmentID:       *segmentID,
		MemTable:        yankedMemtable,
		DescriptorTable: t.TreeInner.DescriptorTable,
	})

	if err != nil {
		return "", err
	}

	resultPath := sg.Metadata.Path

	err = t.RegisterSegments([]segment.Segment{*sg})
	if err != nil {
		return "", err
	}

	print("flush: thread done")
	return resultPath, nil
}

// @TODO: dont think we need locks here and next as well
func (t *Tree) IsCompacting() bool {
	t.TreeInner.LevelsMutex.RLock()
	defer t.TreeInner.LevelsMutex.RUnlock()
	return t.TreeInner.Levels.IsCompacting()
}

// FirstLevelSegmentCount returns the amount of disk segments in the first level
func (t *Tree) FirstLevelSegmentCount() int {
	t.TreeInner.LevelsMutex.RLock()
	defer t.TreeInner.LevelsMutex.RUnlock()
	return t.TreeInner.Levels.FirstLevelSegmentCount()
}

// SegmentCount returns the amount of disk segments currently in the tree
func (t *Tree) SegmentCount() int {
	t.TreeInner.LevelsMutex.RLock()
	defer t.TreeInner.LevelsMutex.RUnlock()
	return t.TreeInner.Levels.Len()
}

// ApproximateLen approximates the amount of items in the tree
func (t *Tree) ApproximateLen() uint64 {
	t.TreeInner.ActiveMutex.RLock()
	memtableLen := uint64(t.TreeInner.ActiveMemtable.Len())
	t.TreeInner.ActiveMutex.RUnlock()

	t.TreeInner.LevelsMutex.RLock()
	segments := t.TreeInner.Levels.GetAllSegmentsFlattened()
	t.TreeInner.LevelsMutex.RUnlock()

	var segmentsLen uint64
	for _, segment := range segments {
		segmentsLen += segment.Metadata.ItemCount
	}

	return memtableLen + segmentsLen
}

func (t *Tree) ActiveMemtableSize() uint32 {
	t.TreeInner.ActiveMutex.RLock()
	defer t.TreeInner.ActiveMutex.RUnlock()
	return t.TreeInner.ActiveMemtable.ApproximateSize.Load()
}

func (t *Tree) RotateMemtable() (*string, *memtable.MemTable) {
	fmt.Printf("rotate: acquiring active memtable write lock")
	t.TreeInner.ActiveMutex.Lock()
	defer t.TreeInner.ActiveMutex.Unlock()
	activeMemtable := t.TreeInner.ActiveMemtable
	if len(activeMemtable.Items) == 0 {
		return nil, nil
	}

	fmt.Printf("rotate: acquiring sealed memtables write lock")
	t.TreeInner.SealedMutex.Lock()
	defer t.TreeInner.SealedMutex.Unlock()
	sealedMemtables := t.TreeInner.SealedMemtables

	yankedMemtable, _ := activeMemtable.Clone()
	tmpMemtableID := id.GenerateSegmentID()
	sealedMemtables[tmpMemtableID] = yankedMemtable

	return &tmpMemtableID, yankedMemtable
}

func (t *Tree) SetActiveMemtable(memtable *memtable.MemTable) {
	t.TreeInner.ActiveMutex.Lock()
	defer t.TreeInner.ActiveMutex.Unlock()
	t.TreeInner.ActiveMemtable = memtable
}

func (t *Tree) FreeSealedMemtable(id string) {
	t.TreeInner.SealedMutex.Lock()
	defer t.TreeInner.SealedMutex.Unlock()
	delete(t.TreeInner.SealedMemtables, id)
}

func (t *Tree) AddSealedMemtable(id string, memtable *memtable.MemTable) {
	t.TreeInner.SealedMutex.Lock()
	defer t.TreeInner.SealedMutex.Unlock()
	t.TreeInner.SealedMemtables[id] = memtable
}

func (t *Tree) Len() (int, error) {
	var count int
	items := t.Iter().Segments
	for _, item := range items {
		if item != nil {
			count++
		}
	}
	return count, nil
}

func (t *Tree) IsEmpty() (bool, error) {
	_, _, ok := t.FirstKeyValue()
	return !ok, nil
}

func IgnoreTombstoneValue(item *value.Value) *value.Value {
	if item.IsTombstone() {
		return nil
	}
	return item
}

func (t *Tree) GetInternalEntry(key []byte, evictTombstone bool, seqno *value.SeqNo) (*value.Value, error) {
	t.TreeInner.ActiveMutex.Lock()

	if item := t.TreeInner.ActiveMemtable.Get(key, seqno); item != nil {
		if evictTombstone {
			return IgnoreTombstoneValue(item), nil
		}
		return item, nil
	}

	t.TreeInner.ActiveMutex.Unlock()
	t.TreeInner.SealedMutex.Lock()
	sealedMemtables := t.TreeInner.SealedMemtables

	for _, memtable := range sealedMemtables {
		if item := memtable.Get(key, seqno); item != nil {
			if evictTombstone {
				return IgnoreTombstoneValue(item), nil
			}
			return item, nil
		}
	}
	t.TreeInner.SealedMutex.Unlock()

	t.TreeInner.LevelsMutex.Lock()
	segmentsLock := t.TreeInner.Levels

	for _, segment := range segmentsLock.GetAllSegmentsFlattened() {
		if item, err := segment.Get(key, seqno); err == nil && item != nil {
			if evictTombstone {
				return IgnoreTombstoneValue(item), nil
			}
			return item, nil
		}
	}
	t.TreeInner.LevelsMutex.Unlock()

	return nil, nil
}

func (t *Tree) Get(key []byte) (value.UserValue, error) {
	item, err := t.GetInternalEntry(key, true, nil)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

func (t *Tree) Insert(key, val []byte, seqno value.SeqNo) (uint32, uint32, error) {
	item := value.NewValue(key, val, seqno, value.Record)
	a, b, c := t.AppendEntry(*item)
	return *a, *b, c
}

func (t *Tree) Remove(key []byte, seqno value.SeqNo) (uint32, uint32, error) {
	item := value.NewValue(key, nil, seqno, value.Tombstone)
	a, b, c := t.AppendEntry(*item)
	return *a, *b, c
}

func (t *Tree) ContainsKey(key []byte) (bool, error) {
	item, err := t.Get(key)
	if err != nil {
		return false, err
	}
	return item != nil, nil
}

func (t *Tree) CreateIter(seqno *value.SeqNo) *ranger.Range {
	return t.CreateRange(nil, nil, seqno)
}

// @TODO: wtf u doin implementing dis shit like this ?? its a map lmao
func (t *Tree) Iter() *ranger.Range {
	return t.CreateIter(nil)
}

func (t *Tree) CreateRange(lo, hi *segment.Bound[value.UserKey], seqno *value.SeqNo) *ranger.Range {
	levels := t.TreeInner.Levels
	// @TODO: add level lock
	segmentArr := levels.GetAllSegmentsFlattened()
	segments := []*segment.Segment{}
	for _, v := range segmentArr {
		if v.CheckKeyRangeOverlap(*lo, *hi) {
			segments = append(segments, v)
		}
	}

	return ranger.NewRange(
		ranger.MemTableGuard{
			Active: &ranger.RwLockGuard[memtable.MemTable]{Obj: t.TreeInner.ActiveMemtable},
			Sealed: &ranger.RwLockGuard[map[string]*memtable.MemTable]{Obj: &t.TreeInner.SealedMemtables},
		},
		[2]segment.Bound[value.UserKey]{*lo, *hi},
		segments,
		*seqno,
	)
}

func (t *Tree) Range(start, end []byte) *ranger.Range {
	st := &segment.Bound[value.UserKey]{
		Included: &start,
	}

	ed := &segment.Bound[value.UserKey]{
		Included: &end,
	}
	return t.CreateRange(st, ed, nil)
}

func (t *Tree) CreatePrefix(pfix []byte, seqno *value.SeqNo) *prefix.Prefix {
	levels := t.TreeInner.Levels
	// @TODO: add level lock
	segmentArr := levels.GetAllSegmentsFlattened()
	segments := []*segment.Segment{}
	for _, v := range segmentArr {
		if v.CheckPrefixOverlap(pfix) {
			segments = append(segments, v)
		}
	}
	// segmentInfo := t.Levels.ReadLock().GetAllSegments().Filter(func(s *Segment) bool {
	// 	return s.CheckPrefixOverlap(pfix)
	// })
	return prefix.NewPrefix(
		ranger.MemTableGuard{
			Active: &ranger.RwLockGuard[memtable.MemTable]{Obj: t.TreeInner.ActiveMemtable},
			Sealed: &ranger.RwLockGuard[map[string]*memtable.MemTable]{Obj: &t.TreeInner.SealedMemtables},
		},
		pfix,
		segments,
		seqno,
	)
}

func (t *Tree) Prefix(pfix []byte) *prefix.Prefix {
	return t.CreatePrefix(pfix, nil)
}

func (t *Tree) FirstKeyValue() (value.UserKey, value.UserValue, bool) {
	key, val, ok := t.Iter().IntoIter().Next()
	if !ok {
		return nil, nil, false
	}
	return *key, *val, true
}

func (t *Tree) LastKeyValue() (value.UserKey, value.UserValue, bool) {
	key, val, ok := t.Iter().IntoIter().NextBack()
	if !ok {
		return nil, nil, false
	}
	return *key, *val, true
}

func (t *Tree) AppendEntry(value value.Value) (*uint32, *uint32, error) {
	t.TreeInner.ActiveMutex.Lock()
	defer t.TreeInner.ActiveMutex.Unlock()

	itemSize, sizeAfter, err := t.TreeInner.ActiveMemtable.Insert(value)
	if err != nil {
		return nil, nil, err
	}
	return &itemSize, &sizeAfter, nil
}

func Recover(path string, blockCache *segment.BlockCache, descriptorTable *descriptor.FileDescriptorTable) (*Tree, error) {
	fmt.Printf("Recovering LSM-tree at %s", path)

	if bytes, err := os.ReadFile(strings.Join([]string{path, file.LSMMarker}, "")); err != nil {
		return nil, err
	} else if vs := version.ParseFileHeader(bytes); vs != version.VersionV0 {
		return nil, fmt.Errorf("invalid version: %v", vs)
	}

	lvl, err := RecoverLevels(path, blockCache, descriptorTable)
	if err != nil {
		return nil, err
	}
	lvl.SortLevels()

	configStr, err := os.ReadFile(strings.Join([]string{path, file.ConfigFile}, ""))
	if err != nil {
		return nil, err
	}
	var cfg config.PersistedConfig
	if err := json.Unmarshal(configStr, &cfg); err != nil {
		return nil, err
	}

	inner := &TreeInner{
		ActiveMemtable:  &memtable.MemTable{},
		SealedMemtables: make(map[string]*memtable.MemTable),
		Levels:          &levels.Levels{},
		OpenSnapshots:   &SnapshotCounter{},
		StopSignal:      &stop.StopSignal{},
		Config:          &cfg,
		BlockCache:      blockCache,
		DescriptorTable: descriptorTable,
	}

	return &Tree{TreeInner: inner}, nil
}

func CreateNew(config config.Config) (*Tree, error) {
	path := config.Inner.Path
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	markerPath := strings.Join([]string{path, file.LSMMarker}, "")
	if _, err := os.Stat(markerPath); err == nil {
		return nil, fmt.Errorf("marker file %s already exists", markerPath)
	}

	// 0755 is ---rwxr-x http://permissions-calculator.org/
	// 0755 Commonly used on web servers. The owner can read, write, execute. Everyone else can read and execute but not modify the file.
	if err := os.MkdirAll(strings.Join([]string{path, file.SegmentsFolder}, ""), 0755); err != nil {
		return nil, err
	}

	// probably doesnt work, @TODO:
	configStr, err := json.MarshalIndent(config.Inner, "", "  ")
	if err != nil {
		return nil, err
	}

	// 0644 Only the owner can read and write. Everyone else can only read. No one can execute the file.
	if err := os.WriteFile(strings.Join([]string{path, file.ConfigFile}, ""), configStr, 0644); err != nil {
		return nil, err
	}

	inner, err := CreateNewTreeInner(&config)
	if err != nil {
		return nil, err
	}

	file, err := os.Create(markerPath)
	if err != nil {
		return nil, err
	}
	if _, err := version.VersionV0.WriteFileHeader(file); err != nil {
		return nil, err
	}
	// fsync here? @TODO: really?
	if err := file.Sync(); err != nil {
		return nil, err
	}

	return &Tree{TreeInner: inner}, nil
}

func (t *Tree) DiskSpace() uint64 {
	segments := t.TreeInner.Levels.GetAllSegmentsFlattened()
	var totalSize uint64
	for _, segment := range segments {
		totalSize += segment.Metadata.FileSize
	}
	return totalSize
}

func (t *Tree) GetSegmentLSN() value.SeqNo {
	segments := t.TreeInner.Levels.GetAllSegmentsFlattened()
	var maxLSN value.SeqNo
	for _, segment := range segments {
		lsn := segment.GetLSN()
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}
	return maxLSN
}

func (t *Tree) GetMemtableLSN() (*value.SeqNo, error) {
	t.TreeInner.ActiveMutex.Lock()
	defer t.TreeInner.ActiveMutex.Unlock()
	return t.TreeInner.ActiveMemtable.GetLSN()
}

func RecoverLevels(treePath string, blockCache *segment.BlockCache, descriptorTable *descriptor.FileDescriptorTable) (*levels.Levels, error) {
	fmt.Printf("Recovering disk segments from %s", treePath)

	manifestPath := filepath.Join(treePath, file.LevelsManifestFile)

	segmentIDsToRecover, err := (&levels.Levels{}).RecoverIds(manifestPath)
	if err != nil {
		return nil, err
	}

	var segments []*segment.Segment

	err = filepath.Walk(filepath.Join(treePath, file.SegmentsFolder), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			return nil
		}

		segmentID := filepath.Base(path)

		fmt.Printf("Recovering segment from %s", path)

		if slices.Contains(segmentIDsToRecover, segmentID) {
			sg, err := segment.RecoverSegment(path, blockCache, descriptorTable)
			if err != nil {
				return err
			}

			descriptorTable.Insert(
				filepath.Join(sg.Metadata.Path, file.BlocksFile),
				sg.Metadata.ID,
			)

			segments = append(segments, sg)
			fmt.Printf("Recovered segment from %s", path)
		} else {
			fmt.Printf("Deleting unfinished segment (not part of level manifest): %s", path)
			err := os.RemoveAll(path)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(segments) < len(segmentIDsToRecover) {
		fmt.Printf("Expected segments: %v", segmentIDsToRecover)
		return nil, fmt.Errorf("some segments were not recovered")
	}

	fmt.Printf("Recovered %d segments", len(segments))

	return (&levels.Levels{}).Recover(manifestPath, segments)
}
