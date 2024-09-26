package flush

import (
	"log"
	"path/filepath"

	"bagh/descriptor"
	"bagh/file"
	"bagh/memtable"
	"bagh/segment"
	"bagh/value"
	// "github.com/pkg/errors"
)

// Options defines the flush options.
type Options struct {
	// MemTable to flush
	MemTable *memtable.MemTable

	// Unique segment ID
	SegmentID string

	// Base folder of segments
	Folder string

	// Block size in bytes
	BlockSize uint32

	// Block cache
	BlockCache *segment.BlockCache

	// Descriptor table
	DescriptorTable *descriptor.FileDescriptorTable
}

// flushToSegment flushes a memtable, creating a segment in the given folder.
func flushToSegment(opts Options) (*segment.Segment, error) {
	segmentFolder := filepath.Join(opts.Folder, opts.SegmentID)
	log.Printf("Flushing segment to %s", segmentFolder)

	segmentWriter, err := segment.NewWriter(segment.Options{
		Path:            segmentFolder,
		EvictTombstones: false,
		BlockSize:       opts.BlockSize,
	})
	if err != nil {
		return nil, err
	}

	opts.MemTable.Items.Range(func(key value.ParsedInternalKey, value value.UserValue) bool {
		err := segmentWriter.Write(segment.Value{
			Key:   key,
			Value: value,
		})
		if err != nil {
			return nil, err
		}
	})

	if err := segmentWriter.Finish(); err != nil {
		return nil, err
	}

	metadata, err := segment.NewMetadataFromWriter(opts.SegmentID, segmentWriter)
	if err != nil {
		return nil, err
	}
	if err := metadata.WriteToFile(); err != nil {
		return nil, err
	}

	log.Printf("Finalized segment write at %s", segmentFolder)

	blockIndex, err := segment.NewBlockIndexFromFile(
		opts.SegmentID,
		opts.DescriptorTable,
		segmentFolder,
		opts.BlockCache,
	)
	if err != nil {
		return nil, err
	}

	createdSegment := &segment.Segment{
		DescriptorTable: opts.DescriptorTable,
		Metadata:        metadata,
		BlockIndex:      blockIndex,
		BlockCache:      opts.BlockCache,
	}

	opts.DescriptorTable.Insert(
		filepath.Join(metadata.Path, file.BlocksFile),
		metadata.ID,
	)

	log.Printf("Flushed segment to %s", segmentFolder)

	return createdSegment, nil
}

// func main() {
// Example usage of the flushToSegment function would go here
// }
