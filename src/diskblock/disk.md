```go
package readme

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/pierrec/lz4"
)

// DiskBlock represents a block of items stored on disk
type DiskBlock struct {
	Items []Value
	CRC   uint32
}

// FromReaderCompressed creates a DiskBlock from a compressed reader
func (db *DiskBlock) FromReaderCompressed(reader io.Reader, size uint32) error {
	bytes := make([]byte, size)
	if _, err := io.ReadFull(reader, bytes); err != nil {
		return err
	}

	decompressed, err := lz4.UncompressBlock(bytes, nil)
	if err != nil {
		return err
	}

	return db.Deserialize(bytes.NewReader(decompressed))
}

// FromFileCompressed creates a DiskBlock from a compressed file
func (db *DiskBlock) FromFileCompressed(file io.ReadSeeker, offset int64, size uint32) error {
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	return db.FromReaderCompressed(file, size)
}

// CreateCRC calculates the CRC for a list of values
func (db *DiskBlock) CreateCRC() uint32 {
	hasher := crc32.NewIEEE()

	binary.Write(hasher, binary.BigEndian, uint32(len(db.Items)))

	for _, value := range db.Items {
		serialized, _ := value.Serialize()
		hasher.Write(serialized)
	}

	return hasher.Sum32()
}

// CheckCRC verifies the CRC of the DiskBlock
func (db *DiskBlock) CheckCRC(expectedCRC uint32) bool {
	return db.CreateCRC() == expectedCRC
}

// Serialize converts the DiskBlock to bytes
func (db *DiskBlock) Serialize() ([]byte, error) {
	var buf bytes.Buffer

	binary.Write(&buf, binary.BigEndian, db.CRC)
	binary.Write(&buf, binary.BigEndian, uint32(len(db.Items)))

	for _, value := range db.Items {
		serialized, err := value.Serialize()
		if err != nil {
			return nil, err
		}
		buf.Write(serialized)
	}

	return buf.Bytes(), nil
}

// Deserialize creates a DiskBlock from bytes
func (db *DiskBlock) Deserialize(reader io.Reader) error {
	if err := binary.Read(reader, binary.BigEndian, &db.CRC); err != nil {
		return err
	}

	var itemCount uint32
	if err := binary.Read(reader, binary.BigEndian, &itemCount); err != nil {
		return err
	}

	db.Items = make([]Value, itemCount)
	for i := uint32(0); i < itemCount; i++ {
		if err := db.Items[i].Deserialize(reader); err != nil {
			return err
		}
	}

	return nil
}
```

Please note that this is a direct translation and may require some adjustments:

1. Some Rust-specific features (like `Arc`) have been removed or replaced with Go equivalents.
2. Error handling is slightly different in Go, so some adjustments were made there.
3. The `lz4_flex` crate used in Rust has been replaced with the `github.com/pierrec/lz4` package in Go.
4. Some types (like `Value`, `BlockHandle`, `FileDescriptorTable`, `BlockCache`, `BlockIndex`) are assumed to exist in the Go code and may need to be defined separately.
5. The test cases were not included in the Go version, but you may want to add them using Go's testing framework.

You may need to adjust imports and ensure that all referenced types and functions are properly defined in your Go project.