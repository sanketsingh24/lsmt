package flush_test

import (
	"bagh/flush"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

// Test structure
func TestFlushToSegment(t *testing.T) {
	// Setup
	tempDir, err := ioutil.TempDir("", "segment_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	memtable := NewMemTable()
	blockCache := NewBlockCache()
	descriptorTable := NewFileDescriptorTable()

	opts := flush.Options{
		Memtable:        memtable,
		SegmentID:       "test_segment",
		Folder:          tempDir,
		BlockSize:       4096,
		BlockCache:      blockCache,
		DescriptorTable: descriptorTable,
	}

	// Test
	segment, err := flush.FlushToSegment(opts)
	if err != nil {
		t.Fatalf("FlushToSegment failed: %v", err)
	}

	// Assertions
	if segment == nil {
		t.Fatal("Expected non-nil segment")
	}

	if segment.Metadata.ID != opts.SegmentID {
		t.Errorf("Expected segment ID %s, got %s", opts.SegmentID, segment.Metadata.ID)
	}

	if segment.BlockCache != opts.BlockCache {
		t.Error("Block cache mismatch")
	}

	if segment.DescriptorTable != opts.DescriptorTable {
		t.Error("Descriptor table mismatch")
	}

	// Check if files were created
	if _, err := os.Stat(filepath.Join(tempDir, opts.SegmentID, BlocksFile)); os.IsNotExist(err) {
		t.Error("Blocks file was not created")
	}

	if _, err := os.Stat(filepath.Join(tempDir, opts.SegmentID, BloomFilterFile)); os.IsNotExist(err) {
		t.Error("Bloom filter file was not created")
	}

	// Additional checks can be added here based on your specific requirements
}
