package segment

import (
	value "bagh/value"
	"encoding/binary"
	"io"
)

// BlockHandle points to a block on file
//
// Disk representation:
// [offset; 8 bytes] - [size; 4 bytes] - [key length; 2 bytes] - [key; N bytes]
type BlockHandle struct {
	// Key of first item in block
	StartKey value.UserKey

	// Position of block in file
	Offset uint64

	// Size of block in bytes
	Size uint32
}

func (bh BlockHandle) Serialize(writer io.Writer) error {
	if err := binary.Write(writer, binary.BigEndian, bh.Offset); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, bh.Size); err != nil {
		return err
	}

	// NOTE: Truncation is okay and actually needed
	if err := binary.Write(writer, binary.BigEndian, uint16(len(bh.StartKey))); err != nil {
		return err
	}

	_, err := writer.Write(bh.StartKey)
	return err
}

func (bh BlockHandle) Deserialize(reader io.Reader) error {
	if err := binary.Read(reader, binary.BigEndian, &bh.Offset); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &bh.Size); err != nil {
		return err
	}

	var keyLen uint16
	if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
		return err
	}

	bh.StartKey = make([]byte, keyLen)
	_, err := io.ReadFull(reader, bh.StartKey)
	return err
}

// NewBlockHandle creates a new BlockHandle
func NewBlockHandle(startKey value.UserKey, offset uint64, size uint32) *BlockHandle {
	return &BlockHandle{
		StartKey: startKey,
		Offset:   offset,
		Size:     size,
	}
}

// Clone returns a deep copy of the BlockHandle
func (bh BlockHandle) Clone() value.SerDeClone {
	startKeyCopy := make(value.UserKey, len(bh.StartKey))
	copy(startKeyCopy, bh.StartKey)
	return NewBlockHandle(startKeyCopy, bh.Offset, bh.Size)
}
