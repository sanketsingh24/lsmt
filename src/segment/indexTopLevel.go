package segment

import (
	. "bagh/value"
	"bytes"
	"encoding/binary"
	"io"
	"sort"
)

/// A reference to a block handle block on disk
///
/// Stores the block's position and size in bytes
/// The start key is stored in the in-memory search tree, see [`TopLevelIndex`] below.
///
/// # Disk representation
///
/// \[offset; 8 bytes] - \[size; 4 bytes]
//
// NOTE: Yes the name is absolutely ridiculous, but it's not the
// same as a regular BlockHandle (to a data block), because the
// start key is not required (it's already in the index, see below)

// BlockHandleBlockHandle is a reference to a block handle block on disk
type BlockHandleBlockHandle struct {
	Offset uint64
	Size   uint32
}

func (bh *BlockHandleBlockHandle) Serialize(writer io.Writer) error {
	if err := binary.Write(writer, binary.BigEndian, bh.Offset); err != nil {
		return err
	}
	return binary.Write(writer, binary.BigEndian, bh.Size)
}

func (bh *BlockHandleBlockHandle) Deserialize(reader io.Reader) error {
	if err := binary.Read(reader, binary.BigEndian, &bh.Offset); err != nil {
		return err
	}
	return binary.Read(reader, binary.BigEndian, &bh.Size)
}

// TopLevelIndex stores references to the positions of blocks on a file and their position
type TopLevelIndex struct {
	Data map[string]*BlockHandleBlockHandle
}

func NewTopLevelIndex(data map[string]*BlockHandleBlockHandle) *TopLevelIndex {
	return &TopLevelIndex{Data: data}
}

func (tli *TopLevelIndex) GetPrefixUpperBound(prefix []byte) (UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for key := range tli.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if !bytes.HasPrefix([]byte(key), prefix) {
			bh := tli.Data[key]
			return UserKey(key), bh, true
		}
	}
	return nil, nil, false
}

func (tli *TopLevelIndex) GetBlockContainingItem(key []byte) (UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for k := range tli.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := len(keys) - 1; i >= 0; i-- {
		if bytes.Compare([]byte(keys[i]), key) <= 0 {
			bh := tli.Data[keys[i]]
			return UserKey(keys[i]), bh, true
		}
	}
	return nil, nil, false
}

func (tli *TopLevelIndex) GetFirstBlockHandle() (UserKey, *BlockHandleBlockHandle) {
	var minKey string
	for key := range tli.Data {
		if minKey == "" || key < minKey {
			minKey = key
		}
	}
	bh := tli.Data[minKey]
	return UserKey(minKey), bh
}

func (tli *TopLevelIndex) GetLastBlockHandle() (UserKey, *BlockHandleBlockHandle) {
	var maxKey string
	for key := range tli.Data {
		if maxKey == "" || key > maxKey {
			maxKey = key
		}
	}
	bh := tli.Data[maxKey]
	return UserKey(maxKey), bh
}

func (tli *TopLevelIndex) GetPreviousBlockHandle(key []byte) (UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for k := range tli.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := len(keys) - 1; i >= 0; i-- {
		if bytes.Compare([]byte(keys[i]), key) < 0 {
			bh := tli.Data[keys[i]]
			return UserKey(keys[i]), bh, true
		}
	}
	return nil, nil, false
}

func (tli *TopLevelIndex) GetNextBlockHandle(key []byte) (UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for k := range tli.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if bytes.Compare([]byte(k), key) > 0 {
			bh := tli.Data[k]
			return UserKey(k), bh, true
		}
	}
	return nil, nil, false
}
