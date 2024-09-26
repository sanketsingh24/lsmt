package descriptor

import (
	"os"
	"sync"
	"sync/atomic"

	lru "bagh/lru"
)

type FileGuard struct {
	wrapper *FileDescriptorWrapper
}

func (fg *FileGuard) File() *os.File {
	fg.wrapper.fileMutex.Lock()
	defer fg.wrapper.fileMutex.Unlock()
	return fg.wrapper.file
}

func (fg *FileGuard) Release() {
	fg.wrapper.isUsed.Store(0)
}

// file access struct, stores a file descriptor(os.file) with its mutex
// and tells if this descriptor is being used or not
type FileDescriptorWrapper struct {
	// descriptor wrapper provided by golang
	file      *os.File
	fileMutex sync.Mutex
	isUsed    atomic.Uint32
}

// stores array of file descriptors for each file for concurrent access (file is stored at 'path'),
// array size is fdt.concurrency
type FileHandle struct {
	descriptors      []*FileDescriptorWrapper
	descriptorsMutex sync.RWMutex
	path             string
}

type FileDescriptorTableInner struct {
	// maps file id (segmentId) to the file
	table map[string]*FileHandle
	// file cache @TODO: explain
	lru      *lru.LruList[string]
	lruMutex sync.Mutex
	// size of all the descriptors across files
	size atomic.Int64
}

type FileDescriptorTable struct {
	inner      *FileDescriptorTableInner
	innerMutex sync.RWMutex
	// no. of concurrent access allowed per file, basically size of FileHanlde.descriptors
	concurrency int
	// lru size
	limit int
}

func NewFileDescriptorTable(limit, concurrency int) *FileDescriptorTable {
	return &FileDescriptorTable{
		inner: &FileDescriptorTableInner{
			table: make(map[string]*FileHandle, 100),
			lru:   lru.NewLruListWithCapacity[string](100),
			size:  *new(atomic.Int64),
		},
		concurrency: concurrency,
		limit:       limit,
	}
}

func (fdt *FileDescriptorTable) Clear() {
	fdt.innerMutex.Lock()
	defer fdt.innerMutex.Unlock()
	fdt.inner.table = make(map[string]*FileHandle)
	fdt.inner.size.Store(0)
}

func (fdt *FileDescriptorTable) Len() int {
	return len(fdt.inner.table)
}

func (fdt *FileDescriptorTable) IsEmpty() bool {
	return fdt.Len() == 0
}

func (fdt *FileDescriptorTable) Size() int64 {
	return fdt.inner.size.Load()
}

// @TODO: on access, adjust hotness of ID -> lock contention though
func (fdt *FileDescriptorTable) Access(segmentId string) (*FileGuard, error) {
	fdt.innerMutex.RLock()
	// Look up the given segmentId and return its file
	item, ok := fdt.inner.table[segmentId]
	fdt.innerMutex.RUnlock()

	if !ok {
		// given segmentId is invalid as its file doesnt exist
		return nil, nil
	}

	item.descriptorsMutex.RLock()
	if len(item.descriptors) == 0 {
		// There are no existing descriptors for this file, so we create an array of file descriptors
		// size of this array is fdt.concurrency, so we can do 'fdt.concurrency' accesses at a moment
		item.descriptorsMutex.RUnlock()

		fdt.innerMutex.Lock()
		defer fdt.innerMutex.Unlock()

		fdt.inner.lruMutex.Lock()
		defer fdt.inner.lruMutex.Unlock() // @TODO: is defer good enough here? should we unlock earlier than defer?
		fdt.inner.lru.Refresh(segmentId)

		item = fdt.inner.table[segmentId]
		item.descriptorsMutex.Lock()
		defer item.descriptorsMutex.Unlock()

		// creates file descriptors
		for i := 0; i < fdt.concurrency; i++ {
			file, err := os.Open(item.path)
			if err != nil {
				return nil, err
			}
			wrapper := &FileDescriptorWrapper{
				file:   file,
				isUsed: *new(atomic.Uint32),
			}
			// marking last one as being used
			if i == fdt.concurrency-1 {
				wrapper.isUsed.Store(1)
			}
			item.descriptors = append(item.descriptors, wrapper)
		}

		newSize := fdt.inner.size.Load() + int64(fdt.concurrency)
		// adds to lru until limit is reached
		if newSize > int64(fdt.limit) {
			if oldest, ok := fdt.inner.lru.GetLeastRecentlyUsed(); oldest != segmentId && ok {
				if oldestItem, ok := fdt.inner.table[oldest]; ok {
					oldestItem.descriptorsMutex.Lock()
					// Decrease the size by the number of descriptors in the oldest item
					fdt.inner.size.Add(-int64(len(oldestItem.descriptors)))
					for _, fd := range oldestItem.descriptors {
						fd.file.Close()
					}
					oldestItem.descriptors = nil
					oldestItem.descriptorsMutex.Unlock()
				}
			}
		}
		// return the last descriptor as it has already been marked as being used
		return &FileGuard{wrapper: item.descriptors[fdt.concurrency-1]}, nil
	}

	// There are existing descriptors for this file, so we loop to find an unused one
	defer item.descriptorsMutex.RUnlock()
	for {
		for _, shard := range item.descriptors {
			// Try to atomically set isUsed from 0 to 1
			if swapped := shard.isUsed.CompareAndSwap(0, 1); swapped {
				// If successful, return a new FileGuard with this descriptor
				return &FileGuard{wrapper: shard}, nil
			}
		}
		// If all descriptors are in use, loop again :)
		// @TODO: find a better way to do this :)
	}
}

func (fdt *FileDescriptorTable) Insert(path string, id string) {
	fdt.innerMutex.Lock()
	defer fdt.innerMutex.Unlock()

	fdt.inner.table[id] = &FileHandle{
		descriptors: []*FileDescriptorWrapper{},
		path:        path,
	}
	fdt.inner.lruMutex.Lock()
	defer fdt.inner.lruMutex.Unlock()
	fdt.inner.lru.Refresh(id)
}

func (fdt *FileDescriptorTable) Remove(id string) {
	fdt.innerMutex.Lock()
	defer fdt.innerMutex.Unlock()

	if item, ok := fdt.inner.table[id]; ok {
		item.descriptorsMutex.Lock()
		fdt.inner.size.Add(-int64(len(item.descriptors)))
		for _, desc := range item.descriptors {
			desc.file.Close()
		}
		item.descriptorsMutex.Unlock()
		delete(fdt.inner.table, id)
	}

	fdt.inner.lruMutex.Lock()
	defer fdt.inner.lruMutex.Unlock()
	fdt.inner.lru.Remove(id)
}

// func main() {
// 	// Example usage
// 	table := NewFileDescriptorTable(10, 2)
// 	table.Insert("/path/to/file1", "file1")
// 	guard, err := table.Access("file1")
// 	if err != nil {
// 		panic(err)
// 	}
// 	if guard != nil {
// 		// Use the file
// 		guard.Release()
// 	}
// 	table.Remove("file1")
// }
