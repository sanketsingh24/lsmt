package main

import (
	"bagh/config"
	"bagh/seqno"
	"bagh/tree"
	"bagh/value"
	"bagh/wal"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type KvStore struct {
	tree  *tree.Tree
	wal   *wal.Wal
	seqno *seqno.SequenceNumberCounter
}

func OpenKvStore(path string) (*KvStore, error) {
	start := time.Now()
	cfg := config.NewConfig(path)
	tree, err := tree.Open(*cfg)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Recovered LSM-tree in %fs\n", time.Since(start).Seconds())

	start = time.Now()
	wal, memtable, err := wal.OpenWal(path)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Recovered WAL + memtable in %fs\n", time.Since(start).Seconds())
	lsn, err := memtable.GetLSN()
	if err != nil {
		return nil, err
	}
	seqno := seqno.NewSequenceNumberCounter(*lsn)

	tree.SetActiveMemtable(memtable)

	kv := &KvStore{
		tree:  tree,
		wal:   wal,
		seqno: seqno,
	}

	go func() {
		for {
			time.Sleep(time.Second)
			err := kv.wal.Sync()
			if err != nil {
				fmt.Printf("WAL sync error: %v\n", err)
			}
		}
	}()

	return kv, nil
}

func (kv *KvStore) Insert(key, v string) error {
	keyBytes := []byte(key)
	valueBytes := []byte(v)
	seqno := kv.seqno.Next()

	err := kv.wal.Write(value.Value{
		Key:       keyBytes,
		Value:     valueBytes,
		SeqNo:     seqno,
		ValueType: value.Record,
	})
	if err != nil {
		return err
	}

	_, memtableSize, err := kv.tree.Insert(keyBytes, valueBytes, seqno)
	if err != nil {
		return err
	}

	return kv.maintenance(memtableSize)
}

func (kv *KvStore) Remove(key string) error {
	keyBytes := []byte(key)
	seqno := kv.seqno.Next()

	err := kv.wal.Write(value.Value{
		Key:       keyBytes,
		Value:     []byte{},
		SeqNo:     seqno,
		ValueType: value.Tombstone,
	})
	if err != nil {
		return err
	}

	_, memtableSize, err := kv.tree.Remove(keyBytes, seqno)
	if err != nil {
		return err
	}
	return kv.maintenance(memtableSize)
}

func (kv *KvStore) ForceFlush() error {
	fmt.Println("Flushing memtable")
	_, err := kv.tree.FlushActiveMemtable()
	return err
}

func (kv *KvStore) maintenance(memtableSize uint32) error {
	if memtableSize > 8*1024*1024 {
		err := kv.ForceFlush()
		if err != nil {
			return err
		}

		err = kv.wal.Truncate()
		if err != nil {
			return err
		}
	}

	if kv.tree.FirstLevelSegmentCount() > 16 {
		fmt.Println("Stalling writes...")
		time.Sleep(100 * time.Millisecond)
	}

	for kv.tree.FirstLevelSegmentCount() > 20 {
		fmt.Println("Halting writes until L0 is cleared up...")
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

func (kv *KvStore) Get(key string) (string, bool, error) {
	value, err := kv.tree.Get([]byte(key))
	if err != nil {
		return "", false, err
	}
	if value == nil {
		return "", false, nil
	}
	return string(value), true, nil
}

func (kv *KvStore) ContainsKey(key string) (bool, error) {
	return kv.tree.ContainsKey([]byte(key))
}

func (kv *KvStore) IsEmpty() (bool, error) {
	return kv.tree.IsEmpty()
}

func (kv *KvStore) Len() (int, error) {
	return kv.tree.Len()
}

const ITEM_COUNT = 1_000_000

func main() {
	kv, err := OpenKvStore(".data")
	if err != nil {
		fmt.Printf("Error opening KvStore: %v\n", err)
		return
	}

	fmt.Println("Counting items")
	count, err := kv.Len()
	if err != nil {
		fmt.Printf("Error getting length: %v\n", err)
		return
	}
	fmt.Printf("Recovered LSM-tree with %d items\n", count)

	keys := []string{"my-key-1", "my-key-2", "my-key-3"}
	for _, key := range keys {
		exists, err := kv.ContainsKey(key)
		if err != nil {
			fmt.Printf("Error checking key existence: %v\n", err)
			return
		}
		if !exists {
			err = kv.Insert(key, "my-value-"+key[7:])
			if err != nil {
				fmt.Printf("Error inserting: %v\n", err)
				return
			}
		}
	}

	fmt.Println("Getting items")
	for _, key := range keys {
		value, exists, err := kv.Get(key)
		if err != nil {
			fmt.Printf("Error getting value: %v\n", err)
			return
		}
		if !exists {
			fmt.Printf("Key %s not found\n", key)
		} else {
			fmt.Printf("%s: %s\n", key, value)
		}
	}

	fmt.Println("Remove 3 items")
	for _, key := range keys {
		err = kv.Remove(key)
		if err != nil {
			fmt.Printf("Error removing: %v\n", err)
			return
		}
	}

	for _, key := range keys {
		exists, err := kv.ContainsKey(key)
		if err != nil {
			fmt.Printf("Error checking key existence: %v\n", err)
			return
		}
		if exists {
			fmt.Printf("Key %s still exists\n", key)
		}
	}

	fmt.Println("Counting items")
	count, err = kv.Len()
	if err != nil {
		fmt.Printf("Error getting length: %v\n", err)
		return
	}
	remainingItemCount := ITEM_COUNT - count

	fmt.Printf("Bulk loading %d items\n", remainingItemCount)
	start := time.Now()

	for i := 0; i < remainingItemCount; i++ {
		err = kv.Insert(uuid.New().String(), uuid.New().String())
		if err != nil {
			fmt.Printf("Error inserting: %v\n", err)
			return
		}

		if i%1_000_000 == 0 {
			fmt.Printf("Written %d items\n", i)
		}
	}
	fmt.Printf("Took: %fs\n", time.Since(start).Seconds())

	fmt.Println("Counting items")
	count, err = kv.Len()
	if err != nil {
		fmt.Printf("Error getting length: %v\n", err)
		return
	}
	if count != ITEM_COUNT {
		fmt.Printf("Expected %d items, but got %d\n", ITEM_COUNT, count)
		return
	}

	for kv.tree.IsCompacting() {
		fmt.Println("Waiting for compaction...")
		time.Sleep(time.Second)
	}

	fmt.Println("Counting items")
	count, err = kv.Len()
	if err != nil {
		fmt.Printf("Error getting length: %v\n", err)
		return
	}
	if count != ITEM_COUNT {
		fmt.Printf("Expected %d items, but got %d\n", ITEM_COUNT, count)
		return
	}

	fmt.Println("All good")
}
