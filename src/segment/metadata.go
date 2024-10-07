package segment

import (
	"bagh/file"
	"bagh/value"
	"bagh/version"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type CompressionType string

const (
	CompressionTypeLz4 CompressionType = "lz4"
)

func (c CompressionType) String() string {
	return string(c)
}

type Metadata struct {
	Version          version.Version
	Path             string
	ID               string
	CreatedAt        uint64
	ItemCount        uint64
	KeyCount         uint64
	BlockSize        uint32
	BlockCount       uint32
	Compression      CompressionType
	FileSize         uint64
	UncompressedSize uint64
	KeyRange         [2]value.UserKey
	Seqnos           [2]value.SeqNo
	TombstoneCount   uint64
}

func MetadataFromWriter(id string, writer *Writer) (*Metadata, error) {
	return &Metadata{
		ID:               id,
		Version:          version.VersionV0,
		Path:             writer.Opts.Path,
		BlockCount:       uint32(writer.BlockCount),
		BlockSize:        writer.Opts.BlockSize,
		CreatedAt:        uint64(time.Now().UnixMicro()),
		FileSize:         writer.FilePos,
		Compression:      CompressionTypeLz4,
		ItemCount:        uint64(writer.ItemCount),
		KeyCount:         uint64(writer.KeyCount),
		KeyRange:         [2]value.UserKey{writer.FirstKey, writer.LastKey},
		Seqnos:           [2]value.SeqNo{writer.LowestSeqNo, writer.HighestSeqNo},
		TombstoneCount:   uint64(writer.TombstoneCount),
		UncompressedSize: writer.UncompressedSize,
	}, nil
}

func (m *Metadata) KeyRangeContains(key []byte) bool {
	return bytes.Compare(key, m.KeyRange[0]) >= 0 && bytes.Compare(key, m.KeyRange[1]) <= 0
}

func (m *Metadata) WriteToFile() error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize to JSON: %v", err)
	}

	file := filepath.Join(m.Path, file.SegmentMetadataFile)
	err = os.WriteFile(file, data, 0644)
	if err != nil {
		return err
	}

	// Sync the file
	f, err := os.OpenFile(file, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return err
	}

	// Sync the directory on Unix-like systems
	if runtime.GOOS != "windows" {
		dir, err := os.Open(m.Path)
		if err != nil {
			return err
		}
		defer dir.Close()
		if err := dir.Sync(); err != nil {
			return err
		}
	}

	return nil
}

func MetadataFromDisk(path string) (*Metadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var metadata Metadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (m *Metadata) CheckPrefixOverlap(prefix []byte) bool {
	if len(prefix) == 0 {
		return true
	}

	start, end := m.KeyRange[0], m.KeyRange[1]
	return (bytes.Compare(start, prefix) <= 0 && bytes.Compare(prefix, end) <= 0) ||
		bytes.HasPrefix(start, prefix) ||
		bytes.HasPrefix(end, prefix)
}
