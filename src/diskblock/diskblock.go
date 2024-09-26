package diskblock

import (
	"bytes"
	"encoding/binary"
	"io"

	"hash/crc32"

	value "bagh/value"

	"github.com/pierrec/lz4/v4"
)

// DiskBlock contains the items of a block after decompressing & deserializing.
type DiskBlock[T value.SerDeClone] struct {
	// type DiskBlock[T any] struct {
	Items []T // @TODO: find a way to do this shit
	CRC   uint32
}

// @TODO: wtf i wrote here its 4am
// func (db *DiskBlock) Clone() *DiskBlock {
// 	newItems := make([]value.SerDeClone, len(db.Items))
// 	for i, item := range db.Items {
// 		newItems[i] = item.Clone()
// 	}
// 	return &DiskBlock{
// 		Items: newItems,
// 		CRC:   db.CRC,
// 	}
// }

// FromReaderCompressed creates a DiskBlock from a compressed reader
func (db *DiskBlock[T]) FromReaderCompressed(reader io.Reader, size uint32) error {
	byt := make([]byte, size)
	if _, err := io.ReadFull(reader, byt); err != nil {
		return err
	}
	dest := make([]byte, size)
	if _, err := lz4.UncompressBlock(byt, dest); err != nil {
		return err
	}

	return db.Deserialize(bytes.NewReader(dest))
}

// FromFileCompressed creates a DiskBlock from a compressed file
// @TODO: check and remove io.readseeker as it is stupid
func (db *DiskBlock[T]) FromFileCompressed(reader io.ReadSeeker, offset int64, size uint32) error {
	if _, err := reader.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return db.FromReaderCompressed(reader, size)
}

// CreateCRC calculates the CRC from a list of values
func (db *DiskBlock[T]) CreateCRC() (uint32, error) {
	hasher := crc32.NewIEEE()

	if err := binary.Write(hasher, binary.BigEndian, uint32(len(db.Items))); err != nil {
		return 0, err
	}

	for _, value := range db.Items {
		var buf bytes.Buffer
		if err := value.Serialize(&buf); err != nil {
			return 0, err
		}
		if _, err := hasher.Write(buf.Bytes()); err != nil {
			return 0, err
		}
	}

	return hasher.Sum32(), nil
}

// CheckCRC checks if the calculated CRC matches the expected CRC
func (db *DiskBlock[T]) CheckCRC(expectedCRC uint32) (bool, error) {
	crc, err := db.CreateCRC()
	if err != nil {
		return false, err
	}
	return crc == expectedCRC, nil
}

// Serialize serializes the DiskBlock
func (db *DiskBlock[T]) Serialize(writer io.Writer) error {
	if err := binary.Write(writer, binary.BigEndian, db.CRC); err != nil {
		return err
	}

	if err := binary.Write(writer, binary.BigEndian, uint32(len(db.Items))); err != nil {
		return err
	}

	for _, value := range db.Items {
		if err := value.Serialize(writer); err != nil {
			return err
		}
	}

	return nil
}

// Deserialize deserializes the DiskBlock
func (db *DiskBlock[T]) Deserialize(reader io.Reader) error {
	if err := binary.Read(reader, binary.BigEndian, &db.CRC); err != nil {
		return err
	}

	var itemCount uint32
	if err := binary.Read(reader, binary.BigEndian, &itemCount); err != nil {
		return err
	}

	db.Items = make([]T, itemCount)
	for i := uint32(0); i < itemCount; i++ {
		item := *new(value.SerDeClone)                   // Replace with your actual type
		if err := item.Deserialize(reader); err != nil { // nil deref @TODO:
			return err
		}
		db.Items[i] = item.(T)
	}

	return nil
}
