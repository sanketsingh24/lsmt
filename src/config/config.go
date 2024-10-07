package config

import (
	"bagh/descriptor"
	"bagh/segment"
)

// TreeType represents the type of the tree
type TreeType int

const (
	Standard TreeType = iota
)

// PersistedConfig represents the tree configuration
type PersistedConfig struct {
	Path       string   `json:"path"`
	BlockSize  uint32   `json:"block_size"`
	LevelCount uint8    `json:"level_count"`
	LevelRatio uint8    `json:"level_ratio"`
	Type       TreeType `json:"type"`
}

const DEFAULT_FILE_FOLDER = ".lsm.data"

// DefaultPersistedConfig creates a new PersistedConfig with default values
func DefaultPersistedConfig() *PersistedConfig {
	return &PersistedConfig{
		Path:       DEFAULT_FILE_FOLDER,
		BlockSize:  4096,
		LevelCount: 7,
		LevelRatio: 8,
		Type:       Standard,
	}
}

// Config represents the tree configuration builder
type Config struct {
	Inner           *PersistedConfig
	BlockCache      *segment.BlockCache
	DescriptorTable *descriptor.FileDescriptorTable
}

// NewDefaultConfig creates a new Config with default values
func DefaultConfig() *Config {
	return &Config{
		Inner:           DefaultPersistedConfig(),
		BlockCache:      segment.NewBlockCache(8 * 1024 * 1024),
		DescriptorTable: descriptor.NewFileDescriptorTable(960, 4),
	}
}

// NewConfig creates a new Config with a specified path
func NewConfig(path string) *Config {
	config := DefaultConfig()
	config.Inner.Path = path
	return config
}

// LevelCount sets the amount of levels of the LSM tree
// / Sets the amount of levels of the LSM tree (depth of tree).
// /
// / Defaults to 7, like `LevelDB` and `RocksDB`.
// /
// / # Panics
// /
// / Panics if `n` is 0.
func (c *Config) LevelCount(n uint8) *Config {
	if n == 0 {
		panic("level count must be greater than 0")
	}
	c.Inner.LevelCount = n
	return c
}

// / Sets the size ratio between levels of the LSM tree (a.k.a. fanout, growth rate).
// /
// / Defaults to 10.
// /
// / # Panics
// /
// / Panics if `n` is less than 2.func (c *Config) LevelRatio(n uint8) *Config {
func (c *Config) LevelRatio(n uint8) *Config {
	if n <= 1 {
		panic("level ratio must be greater than 1")
	}
	c.Inner.LevelRatio = n
	return c
}

// / Sets the block size.
// /
// / Defaults to 4 KiB (4096 bytes).
// /
// / # Panics
// /
// / Panics if the block size is smaller than 1 KiB (1024 bytes).
func (c *Config) BlockSize(blockSize uint32) *Config {
	if blockSize < 1024 {
		panic("block size must be at least 1024 bytes")
	}
	c.Inner.BlockSize = blockSize
	return c
}

// / Sets the block cache.
// /
// / You can create a global [`BlockCache`] and share it between multiple
// / trees to cap global cache memory usage.
// /
// / Defaults to a block cache with 8 MiB of capacity *per tree*.
func (c *Config) SetBlockCache(blockCache *segment.BlockCache) *Config {
	c.BlockCache = blockCache
	return c
}

// SetDescriptorTable sets the descriptor table
func (c *Config) SetDescriptorTable(descriptorTable *descriptor.FileDescriptorTable) *Config {
	c.DescriptorTable = descriptorTable
	return c
}
