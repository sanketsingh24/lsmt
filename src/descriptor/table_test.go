package descriptor_test

import (
	"bagh/descriptor"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestDescriptorTableLimit(t *testing.T) {
	// Create a temporary directory
	tempDir, err := ioutil.TempDir("", "descriptor_table_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test files
	for _, name := range []string{"1", "2", "3"} {
		path := filepath.Join(tempDir, name)
		if _, err := os.Create(path); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	// Create FileDescriptorTable
	table := descriptor.NewFileDescriptorTable(2, 1)

	// Initial size should be 0
	if size := table.Size(); size != 0 {
		t.Errorf("Expected initial size 0, got %d", size)
	}

	// Insert first file
	table.Insert(filepath.Join(tempDir, "1"), "1")
	if size := table.Size(); size != 0 {
		t.Errorf("Expected size 0 after insert, got %d", size)
	}

	// Access first file
	guard, err := table.Access("1")
	if err != nil {
		t.Fatalf("Failed to access file: %v", err)
	}
	if size := table.Size(); size != 1 {
		t.Errorf("Expected size 1 after access, got %d", size)
	}
	guard.Release()

	// Insert second file
	table.Insert(filepath.Join(tempDir, "2"), "2")
	if size := table.Size(); size != 1 {
		t.Errorf("Expected size 1 after second insert, got %d", size)
	}

	// Access first file again
	guard, err = table.Access("1")
	if err != nil {
		t.Fatalf("Failed to access file: %v", err)
	}
	guard.Release()

	// Access second file
	guard, err = table.Access("2")
	if err != nil {
		t.Fatalf("Failed to access file: %v", err)
	}
	if size := table.Size(); size != 2 {
		t.Errorf("Expected size 2 after accessing both files, got %d", size)
	}
	guard.Release()

	// Insert third file
	table.Insert(filepath.Join(tempDir, "3"), "3")
	if size := table.Size(); size != 2 {
		t.Errorf("Expected size 2 after third insert, got %d", size)
	}

	// Access third file
	guard, err = table.Access("3")
	if err != nil {
		t.Fatalf("Failed to access file: %v", err)
	}
	if size := table.Size(); size != 2 {
		t.Errorf("Expected size 2 after accessing third file, got %d", size)
	}
	guard.Release()

	// Remove third file
	table.Remove("3")
	if size := table.Size(); size != 1 {
		t.Errorf("Expected size 1 after removing third file, got %d", size)
	}

	// Remove second file
	table.Remove("2")
	if size := table.Size(); size != 0 {
		t.Errorf("Expected size 0 after removing second file, got %d", size)
	}

	// Access first file again
	guard, err = table.Access("1")
	if err != nil {
		t.Fatalf("Failed to access file: %v", err)
	}
	if size := table.Size(); size != 1 {
		t.Errorf("Expected size 1 after accessing first file again, got %d", size)
	}
	guard.Release()
}
