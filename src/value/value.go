package value

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// UserKey and UserValue are aliases for byte slices (to represent arbitrary byte arrays).
// @TODO: implement less, len swap etc. for userkey
type UserKey = []byte
type UserValue = []byte

type SerDeClone interface {
	Serialize(writer io.Writer) error
	Deserialize(reader io.Reader) error
	Clone() SerDeClone
}

// SeqNo is a monotonically increasing counter for sequence numbers.
type SeqNo uint64

// ValueType represents whether the value is a regular value or a tombstone (for deleted entries).
type ValueType uint8

const (
	Record    ValueType = iota // Regular value
	Tombstone                  // Deleted value
)

// Converts from a byte to ValueType
func ValueTypeFromByte(b byte) ValueType {
	switch b {
	case 0:
		return Record
	default:
		return Tombstone
	}
}

// Converts ValueType to byte
func (v ValueType) ToByte() byte {
	switch v {
	case Record:
		return 0
	case Tombstone:
		return 1
	}
	return 1
}

// Key for skiplist and memtable
type ParsedInternalKey struct {
	UserKey   UserKey   `json:"user_key"`
	SeqNo     SeqNo     `json:"seq_no"`
	ValueType ValueType `json:"value_type"`
}

func NewParsedInternalKey(userKey *UserKey, valueType ValueType, seqno SeqNo) *ParsedInternalKey {
	return &ParsedInternalKey{
		UserKey:   *userKey,
		SeqNo:     seqno,
		ValueType: valueType,
	}
}

func (pik ParsedInternalKey) IsTombstone() bool {
	return pik.ValueType == Tombstone
}

func (pik ParsedInternalKey) String() string {
	return fmt.Sprintf("%x:%d:%d", pik.UserKey, pik.ValueType)
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
type ParsedInternalKeys []ParsedInternalKey

func (piks ParsedInternalKeys) Len() int      { return len(piks) }
func (piks ParsedInternalKeys) Swap(i, j int) { piks[i], piks[j] = piks[j], piks[i] }

// Custom sorting for ParsedInternalKey based on user key and sequence number.
func (p ParsedInternalKey) Less(other ParsedInternalKey) bool {
	return bytes.Compare(p.UserKey, other.UserKey) < 0
}

// / Represents a value in the LSM-tree
// /
// / `key` and `value` are arbitrary user-defined byte arrays
type Value struct {
	/// User-defined key - an arbitrary byte array
	///
	/// Supports up to 2^16 bytes
	Key UserKey

	/// User-defined value - an arbitrary byte array
	///
	/// Supports up to 2^32 bytes
	Value UserValue
	// @P2 well I added seqno after some time so it is missing in manyplaces, pls filll @TODO:
	SeqNo SeqNo
	/// Tombstone marker
	ValueType ValueType
}

func (v Value) Serialize(writer io.Writer) error {
	// if err := binary.Write(writer, binary.BigEndian, uint8(v.ValueType)); err != nil {
	// 	return NewSerializeError(err)
	// }
	// if err := binary.Write(writer, binary.BigEndian, uint16(len(v.Key))); err != nil {
	// 	return NewSerializeError(err)
	// }
	// if _, err := writer.Write(v.Key); err != nil {
	// 	return NewSerializeError(err)
	// }
	// if err := binary.Write(writer, binary.BigEndian, uint32(len(v.Value))); err != nil {
	// 	return NewSerializeError(err)
	// }
	// if _, err := writer.Write(v.Value); err != nil {
	// 	return NewSerializeError(err)
	// }

	if err := binary.Write(writer, binary.BigEndian, int32(len(v.Key))); err != nil {
		return err
	}
	if _, err := writer.Write(v.Key); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, int32(len(v.Value))); err != nil {
		return err
	}
	if _, err := writer.Write(v.Value); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, v.SeqNo); err != nil {
		return err
	}
	return nil
}

func (v Value) Deserialize(reader io.Reader) error {
	var length int32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return err
	}
	v.Key = make([]byte, length)
	if _, err := io.ReadFull(reader, v.Key); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		return err
	}
	v.Value = make([]byte, length)
	if _, err := io.ReadFull(reader, v.Value); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.BigEndian, &v.SeqNo); err != nil {
		return err
	}

	var valueType uint8
	if err := binary.Read(reader, binary.BigEndian, &valueType); err != nil {
		return err
	}
	v.ValueType = ValueType(valueType)
	return nil
}

func (v Value) Clone() SerDeClone {
	return &Value{
		Key:       append([]byte(nil), v.Key...),
		Value:     append([]byte(nil), v.Value...),
		ValueType: v.ValueType,
		SeqNo:     v.SeqNo,
	}
}

// NewValue creates a new Value instance.
func NewValue(key UserKey, value UserValue, seqno SeqNo, valueType ValueType) *Value {
	if len(key) == 0 || len(key) > math.MaxUint16 {
		panic("invalid key length")
	}
	if len(value) > math.MaxUint32 {
		panic("invalid value length")
	}
	return &Value{
		Key:       key,
		Value:     value,
		SeqNo:     seqno,
		ValueType: valueType,
	}
}

// NewTombstone creates a new Value marked as a tombstone (for deletion).
func NewTombstone(key UserKey) *Value {
	if len(key) == 0 || len(key) > math.MaxUint16 {
		panic("invalid key length")
	}
	return &Value{
		Key:       key,
		Value:     nil,
		ValueType: Tombstone,
	}
}

func (v Value) IsTombstone() bool {
	return v.ValueType == Tombstone
}

func (v Value) ToTombstone() error {
	v.ValueType = Tombstone
	return nil
}

func (v Value) Size() int {
	return len(v.Key) + len(v.Value)
}

func (v Value) String() string {
	valueStr := fmt.Sprintf("%x", v.Value)
	if len(v.Value) >= 64 {
		valueStr = fmt.Sprintf("[ ... %d bytes ]", len(v.Value))
	}
	return fmt.Sprintf("%x:%d:%s => %s", v.Key, v.ValueType, valueStr)
}

// Sorting interface for Value. Sort by key and then by sequence number.
func (v Value) Less(other Value) bool {
	if cmp := bytes.Compare(v.Key, other.Key); cmp != 0 {
		return cmp < 0
	}
	return v.SeqNo > other.SeqNo
}

// ValueFromParsedInternalKeyAndUserValue creates a Value from a ParsedInternalKey and UserValue
func ValueFromParsedInternalKeyAndUserValue(key ParsedInternalKey, value UserValue) Value {
	return Value{
		Key:       key.UserKey,
		Value:     value,
		ValueType: key.ValueType,
	}
}

// ParsedInternalKeyFromValue creates a ParsedInternalKey from a Value
func ParsedInternalKeyFromValue(v Value) ParsedInternalKey {
	return ParsedInternalKey{
		UserKey:   v.Key,
		ValueType: v.ValueType,
	}
}

// ValueTypeFromUint8 converts a uint8 to a ValueType
func ValueTypeFromUint8(v uint8) ValueType {
	if v == 0 {
		return Record
	}
	return Tombstone
}

// Uint8FromValueType converts a ValueType to a uint8
func Uint8FromValueType(v ValueType) uint8 {
	return uint8(v)
}

// Test functions and value comparison
// func main() {
// 	// Create value
// 	val := NewValue([]byte{1, 2, 3}, []byte{3, 2, 1}, 42, Value)

// 	// Serialize value
// 	serialized, err := val.Serialize()
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Deserialize value
// 	deserialized, err := DeserializeValue(serialized)
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Check equality
// 	fmt.Printf("Original: %+v\n", val)
// 	fmt.Printf("Deserialized: %+v\n", deserialized)

// 	// Test sorting
// 	values := []Value{
// 		*NewValue([]byte{1, 2, 3}, []byte{3, 2, 1}, 0),
// 		*NewValue([]byte{1, 2, 3}, []byte{1, 2, 3}, 0),
// 		*NewTombstone([]byte{1, 2, 3}),
// 	}
// 	sort.Slice(values, func(i, j int) bool { return values[i].Less(values[j]) })
// 	fmt.Println("Sorted values:", values)
// }
