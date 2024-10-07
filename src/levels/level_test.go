package levels_test

import (
	"bagh/descriptor"
	"bagh/levels"
	"bagh/segment"
	"bagh/value"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func fixtureSegment(id string, keyRange [2]value.UserKey) *segment.Segment {
	block_cache := segment.NewBlockCache(math.MaxUint64)
	return &segment.Segment{
		DescriptorTable: descriptor.NewFileDescriptorTable(512, 1),
		Metadata: &segment.Metadata{
			ID:       id,
			KeyRange: keyRange,
		},
		BlockIndex: segment.NewBlockIndex(id, block_cache),
		BlockCache: block_cache,
	}
}

func TestLevelOverlaps(t *testing.T) {
	seg0 := fixtureSegment("1", [2]value.UserKey{[]byte("c"), []byte("k")})
	seg1 := fixtureSegment("2", [2]value.UserKey{[]byte("l"), []byte("z")})

	level := levels.ResolvedLevel{Segments: []segment.Segment{*seg0, *seg1}}

	assert.Equal(t, []string{}, level.GetOverlappingSegments([]byte("a"), []byte("b")))

	assert.Equal(t, []string{"1"}, level.GetOverlappingSegments([]byte("d"), []byte("k")))

	assert.Equal(t, []string{"1", "2"}, level.GetOverlappingSegments([]byte("f"), []byte("x")))
}
