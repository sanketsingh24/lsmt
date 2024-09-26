package version

import (
	"encoding/binary"
	"fmt"
	"io"
	"slices"
)

type Version uint16

const (
	VersionV0 Version = iota
)

func (v Version) String() string {
	return "0"
}

func VersionFromU16(value uint16) (Version, error) {
	switch value {
	case 0:
		return VersionV0, nil
	default:
		return 0, fmt.Errorf("invalid version: %d", value)
	}
}

var MagicBytes = []byte{'L', 'S', 'M'}

func (Version) Len() uint8 {
	return 5
}

func ParseFileHeader(bytes []byte) (Version, bool) {
	if len(bytes) < 5 {
		return 0, false
	}

	if slices.Compare(bytes[:3], MagicBytes) != 0 {
		return 0, false
	}

	value := binary.BigEndian.Uint16(bytes[3:5])
	version, err := VersionFromU16(value)
	if err != nil {
		return 0, false
	}

	return version, true
}

func (v Version) WriteFileHeader(writer io.Writer) (int, error) {
	if _, err := writer.Write(MagicBytes); err != nil {
		return 0, err
	}

	if err := binary.Write(writer, binary.BigEndian, v); err != nil {
		return 0, err
	}

	return 5, nil
}
