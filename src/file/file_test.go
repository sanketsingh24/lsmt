package file_test

import (
	"bagh/file"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestAtomicRewrite(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "test.txt")

	// Create initial file
	if err := ioutil.WriteFile(path, []byte("asdasdasdasdasd"), 0644); err != nil {
		t.Fatalf("Failed to write initial file: %v", err)
	}

	// Rewrite file atomically
	if err := file.RewriteAtomic(path, []byte("newcontent")); err != nil {
		t.Fatalf("Failed to rewrite file atomically: %v", err)
	}

	// Read content and verify
	content, err := ioutil.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if string(content) != "newcontent" {
		t.Errorf("Expected content 'newcontent', got '%s'", string(content))
	}
}
