package disk_test

import (
	"bagh/disk"
	"bagh/value"
	"bytes"
	"testing"
)

// Value is a sample implementation of Serializable for testing purposes
func TestBlockyDeserializationSuccess(t *testing.T) {
	item1 := &value.Value{Key: []byte{1, 2, 3}, Value: []byte{4, 5, 6}, SeqNo: 42, ValueType: 0}
	item2 := &value.Value{Key: []byte{7, 8, 9}, Value: []byte{10, 11, 12}, SeqNo: 43, ValueType: 0}

	items := []value.Value{*item1, *item2}
	block := &disk.DiskBlock[value.Value]{Items: items}

	err := block.CreateCRC()
	if err != nil {
		t.Fatalf("Failed to create CRC: %v", err)
	}
	crc := block.CRC
	// Serialize to bytes
	var buf bytes.Buffer
	if err := block.Serialize(&buf); err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	// Deserialize from bytes
	deserializedBlock := &disk.DiskBlock[value.Value]{}
	if err := deserializedBlock.Deserialize(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	if len(deserializedBlock.Items) != 2 {
		t.Errorf("Expected 2 items, got %d", len(deserializedBlock.Items))
	}

	if deserializedBlock.CRC != crc {
		t.Errorf("CRC mismatch. Expected %d, got %d", crc, deserializedBlock.CRC)
	}

	// Compare deserialized items
	for i, item := range deserializedBlock.Items {
		deserializedItem := item

		var expectedItem *value.Value
		if i == 0 {
			expectedItem = item1
		} else {
			expectedItem = item2
		}

		if !bytes.Equal(deserializedItem.Key, expectedItem.Key) ||
			!bytes.Equal(deserializedItem.Value, expectedItem.Value) ||
			deserializedItem.SeqNo != expectedItem.SeqNo ||
			deserializedItem.ValueType != expectedItem.ValueType {
			t.Errorf("Item %d mismatch", i)
		}
	}
}

func TestBlockyDeserializationFailureCRC(t *testing.T) {
	item1 := &value.Value{Key: []byte{1, 2, 3}, Value: []byte{4, 5, 6}, SeqNo: 42, ValueType: 0}
	item2 := &value.Value{Key: []byte{7, 8, 9}, Value: []byte{10, 11, 12}, SeqNo: 43, ValueType: 0}

	block := &disk.DiskBlock[value.Value]{
		Items: []value.Value{*item1, *item2},
		CRC:   12345,
	}

	// Serialize to bytes
	var buf bytes.Buffer
	if err := block.Serialize(&buf); err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}

	// Deserialize from bytes
	deserializedBlock := &disk.DiskBlock[value.Value]{}
	if err := deserializedBlock.Deserialize(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}

	// Check CRC
	match, err := deserializedBlock.CheckCRC(54321)
	if err != nil {
		t.Fatalf("CRC check failed: %v", err)
	}
	if match {
		t.Errorf("Expected CRC mismatch, but got a match")
	}
}
