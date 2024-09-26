package serde

import (
	"errors"
	"fmt"
	"io"
)

// Serializable is an interface for serializable objects
type Serializable interface {
	Serialize(writer io.Writer) error
}

// Deserializable is an interface for deserializable objects
type Deserializable interface {
	Deserialize(reader io.Reader) error
}

// SerializeError represents an error during serialization
type SerializeError struct {
	Err error
}

func (e SerializeError) Error() string {
	return fmt.Sprintf("SerializeError(%s)", e.Err)
}

// NewSerializeError creates a new SerializeError
func NewSerializeError(err error) SerializeError {
	return SerializeError{Err: err}
}

// DeserializeError represents an error during deserialization
type DeserializeError struct {
	Err interface{}
}

func (e DeserializeError) Error() string {
	switch err := e.Err.(type) {
	case error:
		return fmt.Sprintf("DeserializeError(%s)", err)
	default:
		return fmt.Sprintf("DeserializeError(%v)", err)
	}
}

// NewDeserializeError creates a new DeserializeError
func NewDeserializeError(err interface{}) DeserializeError {
	return DeserializeError{Err: err}
}

// InvalidTagError represents an invalid enum tag error
type InvalidTagError struct {
	Name string
	Tag  uint8
}

// InvalidHeaderError represents an invalid block header error
type InvalidHeaderError struct {
	Message string
}

// Helper function to create a new InvalidTagError
func NewInvalidTagError(name string, tag uint8) InvalidTagError {
	return InvalidTagError{Name: name, Tag: tag}
}

// Helper function to create a new InvalidHeaderError
func NewInvalidHeaderError(message string) InvalidHeaderError {
	return InvalidHeaderError{Message: message}
}

// Helper function to create a DeserializeError for I/O errors
func NewIODeserializeError(err error) DeserializeError {
	return NewDeserializeError(err)
}

// Helper function to create a DeserializeError for UTF-8 errors
func NewUTF8DeserializeError(err error) DeserializeError {
	return NewDeserializeError(err)
}

// Helper function to create a DeserializeError for invalid trailer
func NewInvalidTrailerError() DeserializeError {
	return NewDeserializeError(errors.New("invalid trailer"))
}
