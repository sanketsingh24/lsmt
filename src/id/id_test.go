package id_test

import (
	"bagh/id"
	"sort"
	"testing"
)

func TestIDMonotonicOrder(t *testing.T) {
	for i := 0; i < 1000; i++ {
		var ids []string
		for j := 0; j < 100; j++ {
			ids = append(ids, id.GenerateSegmentID())
		}

		sorted := make([]string, len(ids))
		copy(sorted, ids)
		sort.Strings(sorted)

		if !equal(ids, sorted) {
			t.Errorf("ID is not monotonic in iteration %d", i)
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
