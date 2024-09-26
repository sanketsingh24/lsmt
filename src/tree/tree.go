package tree

import (
	"bagh/config"
	"bagh/snapshot"
	treeinner "bagh/treeInner"
	"bagh/value"
	"encoding/json"
	"fmt"
	"os"
)

type Tree struct {
	TreeInner *treeinner.TreeInner
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
	fmt.Printf("Opening LSM-tree at %s", config.Inner.Path)

	// @P2::::: @TODO:
	// if exists, err := config.Inner.Path.Join(file.LSMMarker).Exists(); err != nil {
	// 	return nil, err
	// } else if exists {
	// 	return Recover(config.Inner.Path, config.BlockCache, config.DescriptorTable)
	// } else {
	return CreateNew(config)
	// }
}

func (t *Tree) Compact(strategy CompactionStrategy) error {
	options := &CompactionOptions{
		Config:          t.TreeInner.Config.Clone(),
		SealedMemtables: t.TreeInner.SealedMemtables.Clone(),
		Levels:          t.TreeInner.Levels.Clone(),
		OpenSnapshots:   t.TreeInner.OpenSnapshots.Clone(),
		StopSignal:      t.TreeInner.StopSignal.Clone(),
		BlockCache:      t.TreeInner.BlockCache.Clone(),
		Strategy:        strategy,
		DescriptorTable: t.TreeInner.DescriptorTable.Clone(),
	}
	if err := doCompaction(options); err != nil {
		return err
	}
	fmt.Printf("lsm-tree: compaction run over")
	return nil
}

func (t *Tree) MajorCompact(targetSize uint64) error {
	fmt.Printf("Starting major compaction")
	strategy := NewMajorCompactionStrategy(targetSize)
	return t.Compact(strategy)
}

func (t *Tree) Snapshot(seqno value.SeqNo) *snapshot.Snapshot {
	return snapshot.NewSnapshot(t.Clone(), seqno)
}

func (t *Tree) RegisterSegments(segments []segment.Segment) error {
	levels := t.TreeInner.Levels.WriteLock()
	defer levels.Unlock()

	for _, segment := range segments {
		levels.Add(segment)
	}

	sealedMemtables := t.TreeInner.SealedMemtables.WriteLock()
	defer sealedMemtables.Unlock()

	for _, segment := range segments {
		sealedMemtables.Remove(segment.Metadata.ID)
	}

	if err := levels.WriteToDisk(); err != nil {
		return err
	}

	return nil
}

func (t *Tree) FlushActiveMemtable() (PathBuf, error) {
	log.Debug("flush: flushing active memtable")

	if segmentID, yankedMemtable := t.RotateMemtable(); segmentID != nil && yankedMemtable != nil {
		segmentFolder := t.Config.Path.Join(SEGMENTS_FOLDER)
		log.Debugf("flush: writing segment to %s", segmentFolder.String())

		segment := FlushToSegment(&FlushOptions{
			Memtable:        yankedMemtable,
			BlockCache:      t.BlockCache,
			BlockSize:       t.Config.BlockSize,
			Folder:          segmentFolder,
			SegmentID:       segmentID,
			DescriptorTable: t.DescriptorTable,
		})
		if segment != nil {
			segment = &Segment{Metadata: segment.Metadata}
			if err := t.RegisterSegments([]Segment{*segment}); err != nil {
				return nil, err
			}
			return segment.Metadata.Path, nil
		}
	}
	return nil, nil
}

func (t *Tree) IsCompacting() bool {
	levels := t.Levels.ReadLock()
	defer levels.Unlock()
	return levels.IsCompacting()
}

func (t *Tree) FirstLevelSegmentCount() int {
	levels := t.Levels.ReadLock()
	defer levels.Unlock()
	return levels.FirstLevelSegmentCount()
}

func (t *Tree) SegmentCount() int {
	levels := t.Levels.ReadLock()
	defer levels.Unlock()
	return len(levels.GetAllSegments())
}

func (t *Tree) ApproximateLen() uint64 {
	memtable := t.ActiveMemtable.ReadLock()
	defer memtable.Unlock()
	levels := t.Levels.ReadLock()
	defer levels.Unlock()

	var totalCount uint64
	totalCount += uint64(memtable.Len())
	for _, segment := range levels.GetAllSegmentsFlattened() {
		totalCount += segment.Metadata.ItemCount
	}
	return totalCount
}

func (t *Tree) ActiveMemtableSize() uint32 {
	memtable := t.ActiveMemtable.ReadLock()
	defer memtable.Unlock()
	return memtable.ApproximateSize.Load()
}

func (t *Tree) LockActiveMemtable() *MemTable {
	return t.ActiveMemtable.WriteLock()
}

func (t *Tree) LockSealedMemtables() *SealedMemtables {
	return t.SealedMemtables.WriteLock()
}

func (t *Tree) RotateMemtable() (Arc[str], *MemTable) {
	log.Trace("rotate: acquiring active memtable write lock")
	activeMemtable := t.LockActiveMemtable()
	if len(activeMemtable.Items) == 0 {
		return nil, nil
	}

	log.Trace("rotate: acquiring sealed memtables write lock")
	sealedMemtables := t.LockSealedMemtables()
	defer sealedMemtables.Unlock()

	yankedMemtable := activeMemtable.Clone()
	tmpMemtableID := generateSegmentID()
	sealedMemtables.Insert(tmpMemtableID, yankedMemtable)

	return tmpMemtableID, yankedMemtable
}

func (t *Tree) SetActiveMemtable(memtable MemTable) {
	activeMemtable := t.ActiveMemtable.WriteLock()
	*activeMemtable = memtable
	activeMemtable.Unlock()
}

func (t *Tree) FreeSealedMemtable(id Arc[str]) {
	sealedMemtables := t.SealedMemtables.WriteLock()
	defer sealedMemtables.Unlock()
	sealedMemtables.Remove(id)
}

func (t *Tree) AddSealedMemtable(id Arc[str], memtable Arc[MemTable]) {
	sealedMemtables := t.SealedMemtables.WriteLock()
	defer sealedMemtables.Unlock()
	sealedMemtables.Insert(id, memtable)
}

func (t *Tree) Len() (int, error) {
	var count int
	for item := range t.Iter() {
		if item != nil {
			count++
		}
	}
	return count, nil
}

func (t *Tree) IsEmpty() (bool, error) {
	_, ok := t.FirstKeyValue()
	return !ok, nil
}

func (t *Tree) GetInternalEntry(key []byte, evictTombstone bool, seqno SeqNo) (Value, error) {
	memtableLock := t.ActiveMemtable.ReadLock()
	defer memtableLock.Unlock()

	if item := memtableLock.Get(key, seqno); item != nil {
		if evictTombstone {
			return ignoreTombstoneValue(item), nil
		}
		return item, nil
	}

	memtableLock.Unlock()

	sealedMemtablesLock := t.SealedMemtables.ReadLock()
	defer sealedMemtablesLock.Unlock()

	for _, memtable := range sealedMemtablesLock.GetAll() {
		if item := memtable.Get(key, seqno); item != nil {
			if evictTombstone {
				return ignoreTombstoneValue(item), nil
			}
			return item, nil
		}
	}

	segmentsLock := t.Levels.ReadLock()
	defer segmentsLock.Unlock()

	for _, segment := range segmentsLock.GetAllSegmentsFlattened() {
		if item, err := segment.Get(key, seqno); err == nil && item != nil {
			if evictTombstone {
				return ignoreTombstoneValue(item), nil
			}
			return item, nil
		}
	}

	return nil, nil
}

func (t *Tree) Get(key []byte) (UserValue, error) {
	if item, err := t.GetInternalEntry(key, true, nil); err == nil {
		return item.Value, nil
	}
	return nil, err
}

func (t *Tree) Insert(key, value []byte, seqno SeqNo) (uint32, uint32) {
	item := NewValue(key, value, seqno, ValueTypeValue)
	return t.AppendEntry(item)
}

func (t *Tree) Remove(key []byte, seqno SeqNo) (uint32, uint32) {
	item := NewValue(key, nil, seqno, ValueTypeTombstone)
	return t.AppendEntry(item)
}

func (t *Tree) ContainsKey(key []byte) (bool, error) {
	if item, err := t.Get(key); err == nil {
		return item != nil, nil
	}
	return false, err
}

func (t *Tree) CreateIter(seqno SeqNo) *Range {
	return t.CreateRange(nil, nil, seqno)
}

func (t *Tree) Iter() *Range {
	return t.CreateIter(nil)
}

func (t *Tree) CreateRange(lo, hi Bound[UserKey], seqno SeqNo) *Range {
	segmentInfo := t.Levels.ReadLock().GetAllSegments().Filter(func(s *Segment) bool {
		return s.CheckKeyRangeOverlap(lo, hi)
	})
	return NewRange(
		&MemTableGuard{
			Active: t.ActiveMemtable.ReadLock(),
			Sealed: t.SealedMemtables.ReadLock(),
		},
		lo, hi,
		segmentInfo,
		seqno,
	)
}

func (t *Tree) Range(start, end []byte) *Range {
	return t.CreateRange(Included(start), Included(end), nil)
}

func (t *Tree) CreatePrefix(prefix []byte, seqno SeqNo) *Prefix {
	segmentInfo := t.Levels.ReadLock().GetAllSegments().Filter(func(s *Segment) bool {
		return s.CheckPrefixOverlap(prefix)
	})
	return NewPrefix(
		&MemTableGuard{
			Active: t.ActiveMemtable.ReadLock(),
			Sealed: t.SealedMemtables.ReadLock(),
		},
		prefix,
		segmentInfo,
		seqno,
	)
}

func (t *Tree) Prefix(prefix []byte) *Prefix {
	return t.CreatePrefix(prefix, nil)
}

func (t *Tree) FirstKeyValue() (UserKey, UserValue, bool) {
	item, ok := <-t.Iter()
	if !ok {
		return nil, nil, false
	}
	return item.Key, item.Value, true
}

func (t *Tree) LastKeyValue() (UserKey, UserValue, bool) {
	item, ok := <-t.Iter().Reverse()
	if !ok {
		return nil, nil, false
	}
	return item.Key, item.Value, true
}

func (t *Tree) AppendEntry(value Value) (uint32, uint32) {
	memtable := t.ActiveMemtable.ReadLock()
	defer memtable.Unlock()
	return memtable.Insert(value)
}

func Recover(path Path, blockCache Arc[BlockCache], descriptorTable Arc[FileDescriptorTable]) (*Tree, error) {
	log.Infof("Recovering LSM-tree at %s", path.String())

	if bytes, err := os.ReadFile(path.Join(LSM_MARKER)); err != nil {
		return nil, err
	} else if version := ParseVersionHeader(bytes); version != VersionV0 {
		return nil, fmt.Errorf("invalid version: %v", version)
	}

	levels := RecoverLevels(path, blockCache, descriptorTable)
	levels.SortLevels()

	configStr, err := os.ReadFile(path.Join(CONFIG_FILE))
	if err != nil {
		return nil, err
	}
	var config Config
	if err := json.Unmarshal(configStr, &config); err != nil {
		return nil, err
	}

	inner := &TreeInner{
		ActiveMemtable:  Arc[MemTable]{},
		SealedMemtables: Arc[SealedMemtables]{},
		Levels:          Arc[RWMutex[Levels]]{Value: levels},
		OpenSnapshots:   SnapshotCounter{},
		StopSignal:      StopSignal{},
		Config:          config,
		BlockCache:      blockCache,
		DescriptorTable: descriptorTable,
	}

	return &Tree{TreeInner: inner}, nil
}

func CreateNew(config Config) (*Tree, error) {
	path := config.Inner.Path
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}

	markerPath := path.Join(LSM_MARKER)
	if exists, _ := markerPath.Exists(); exists {
		return nil, fmt.Errorf("marker file %s already exists", markerPath.String())
	}

	if err := os.MkdirAll(path.Join(SEGMENTS_FOLDER), 0755); err != nil {
		return nil, err
	}

	configStr, err := json.MarshalIndent(config.Inner, "", "  ")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path.Join(CONFIG_FILE), configStr, 0644); err != nil {
		return nil, err
	}

	inner := CreateTreeInner(config)

	file, err := os.Create(markerPath)
	if err != nil {
		return nil, err
	}
	if _, err := VersionV0.WriteHeader(file); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}

	if err := syncFolder(path.Join(SEGMENTS_FOLDER)); err != nil {
		return nil, err
	}
	if err := syncFolder(path); err != nil {
		return nil, err
	}

	return &Tree{TreeInner: inner}, nil
}

func (t *Tree) DiskSpace() uint64 {
	segments := t.Levels.ReadLock().GetAllSegmentsFlattened()
	var totalSize uint64
	for _, segment := range segments {
		totalSize += segment.Metadata.FileSize
	}
	return totalSize
}

func (t *Tree) GetSegmentLSN() SeqNo {
	segments := t.Levels.ReadLock().GetAllSegmentsFlattened()
	var maxLSN SeqNo
	for _, segment := range segments {
		lsn := segment.GetLSN()
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}
	return maxLSN
}

func (t *Tree) GetLSN() Se
