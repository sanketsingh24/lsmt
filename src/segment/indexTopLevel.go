package segment

import (
	"bagh/value"
	"bytes"
	"encoding/binary"
	"io"
	"sort"
)

// / A reference to a block handle block on disk
// /
// / Stores the block's position and size in bytes
// / The start key is stored in the in-memory search tree, see [`TopLevelIndex`] below.
// /
// / # Disk representation
// /
// / \[offset; 8 bytes] - \[size; 4 bytes]
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

func (tli *TopLevelIndex) GetPrefixUpperBound(prefix []byte) (value.UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for key := range tli.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if !bytes.HasPrefix([]byte(key), prefix) {
			bh := tli.Data[key]
			return value.UserKey(key), bh, true
		}
	}
	return nil, nil, false
}

func (tli *TopLevelIndex) GetBlockContainingItem(key []byte) (value.UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for k := range tli.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := len(keys) - 1; i >= 0; i-- {
		if bytes.Compare([]byte(keys[i]), key) <= 0 {
			bh := tli.Data[keys[i]]
			return value.UserKey(keys[i]), bh, true
		}
	}
	return nil, nil, false
}

func (tli *TopLevelIndex) GetFirstBlockHandle() (value.UserKey, *BlockHandleBlockHandle) {
	var minKey string
	for key := range tli.Data {
		if minKey == "" || key < minKey {
			minKey = key
		}
	}
	bh := tli.Data[minKey]
	return value.UserKey(minKey), bh
}

func (tli *TopLevelIndex) GetLastBlockHandle() (value.UserKey, *BlockHandleBlockHandle) {
	var maxKey string
	for key := range tli.Data {
		if maxKey == "" || key > maxKey {
			maxKey = key
		}
	}
	bh := tli.Data[maxKey]
	return value.UserKey(maxKey), bh
}

func (tli *TopLevelIndex) GetPreviousBlockHandle(key []byte) (value.UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for k := range tli.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for i := len(keys) - 1; i >= 0; i-- {
		if bytes.Compare([]byte(keys[i]), key) < 0 {
			bh := tli.Data[keys[i]]
			return value.UserKey(keys[i]), bh, true
		}
	}
	return nil, nil, false
}

func (tli *TopLevelIndex) GetNextBlockHandle(key []byte) (value.UserKey, *BlockHandleBlockHandle, bool) {
	var keys []string
	for k := range tli.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if bytes.Compare([]byte(k), key) > 0 {
			bh := tli.Data[k]
			return value.UserKey(k), bh, true
		}
	}
	return nil, nil, false
}
