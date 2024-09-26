package file

import (
	"os"
	"path/filepath"
)

// Constants
const (
	LSMMarker           = ".lsm"
	SegmentsFolder      = "segments"
	LevelsManifestFile  = "levels.json"
	ConfigFile          = "config.json"
	BlocksFile          = "blocks"
	IndexBlocksFile     = "index_blocks"
	TopLevelIndexFile   = "index"
	SegmentMetadataFile = "meta.json"
)

// future optimization
// const BloomFilterFile = "bloom"

// RewriteAtomic atomically rewrites a file
// @TODO: check if works
func RewriteAtomic(path string, content []byte) error {
	dir := filepath.Dir(path)

	tempFile, err := os.CreateTemp(dir, "temp-*")
	if err != nil {
		return err
	}
	defer os.Remove(tempFile.Name())

	if _, err := tempFile.Write(content); err != nil {
		return err
	}

	// TODO: Not sure if the fsync is really required, but just for the sake of it...
	if err := tempFile.Sync(); err != nil {
		return err
	}

	if err := tempFile.Close(); err != nil {
		return err
	}

	if err := os.Rename(tempFile.Name(), path); err != nil {
		return err
	}

	// On non-Windows systems, we might want to sync the directory as well
	if os.PathSeparator != '\\' { // Check if not Windows
		dir, err := os.Open(filepath.Dir(path))
		if err != nil {
			return err
		}
		defer dir.Close()
		// TODO: Not sure if the fsync is really required, but just for the sake of it...
		if err := dir.Sync(); err != nil {
			return err
		}
	}

	return nil
}
