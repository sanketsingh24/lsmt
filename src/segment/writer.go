package segment

import (
	"bagh/file"
	"bagh/id"
	"bagh/value"
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pierrec/lz4/v4"
)

type MultiWriter struct {
	TargetSize       uint64
	Opts             Options
	CreatedItems     []Metadata
	CurrentSegmentID string
	Writer           Writer
}

type Writer struct {
	Opts             Options
	BlockWriter      *bufio.Writer // writes os.file
	IndexWriter      IndexWriter
	Chunk            ValueBlock
	BlockFile        *os.File
	BlockCount       int
	ItemCount        int
	FilePos          uint64
	UncompressedSize uint64
	FirstKey         value.UserKey
	LastKey          value.UserKey
	TombstoneCount   int
	ChunkSize        int
	LowestSeqNo      value.SeqNo
	HighestSeqNo     value.SeqNo
	KeyCount         int
	CurrentKey       value.UserKey
}

type Options struct {
	Path            string
	EvictTombstones bool
	BlockSize       uint32
}

func NewMultiWriter(targetSize uint64, opts Options) (*MultiWriter, error) {
	segmentID := id.GenerateSegmentID()

	writer, err := NewWriter(Options{
		Path:            filepath.Join(opts.Path, segmentID),
		EvictTombstones: opts.EvictTombstones,
		BlockSize:       opts.BlockSize,
	})
	if err != nil {
		return nil, err
	}

	return &MultiWriter{
		TargetSize:       targetSize,
		Opts:             opts,
		CreatedItems:     make([]Metadata, 0, 10),
		CurrentSegmentID: segmentID,
		Writer:           *writer,
	}, nil
}

func (mw *MultiWriter) Rotate() error {
	// Flush segment, and start new one
	if err := mw.Writer.Finish(); err != nil {
		return err
	}

	newSegmentID := id.GenerateSegmentID()

	newWriter, err := NewWriter(Options{
		Path:            filepath.Join(mw.Opts.Path, newSegmentID),
		EvictTombstones: mw.Opts.EvictTombstones,
		BlockSize:       mw.Opts.BlockSize,
	})
	if err != nil {
		return err
	}

	oldWriter := mw.Writer
	mw.Writer = *newWriter
	oldSegmentID := mw.CurrentSegmentID
	mw.CurrentSegmentID = newSegmentID

	if oldWriter.ItemCount > 0 {
		metadata, err := MetadataFromWriter(oldSegmentID, &oldWriter)
		if err != nil {
			return err
		}
		mw.CreatedItems = append(mw.CreatedItems, *metadata)
	}

	return nil
}

func (mw *MultiWriter) Write(item value.Value) error {
	if err := mw.Writer.Write(item); err != nil {
		return err
	}

	if mw.Writer.FilePos >= mw.TargetSize {
		if err := mw.Rotate(); err != nil {
			return err
		}
	}

	return nil
}

func (mw *MultiWriter) Finish() ([]Metadata, error) {
	if err := mw.Writer.Finish(); err != nil {
		return nil, err
	}

	if mw.Writer.ItemCount > 0 {
		metadata, err := MetadataFromWriter(mw.CurrentSegmentID, &mw.Writer)
		if err != nil {
			return nil, err
		}
		mw.CreatedItems = append(mw.CreatedItems, *metadata)
	}

	return mw.CreatedItems, nil
}

func NewWriter(opts Options) (*Writer, error) {
	if err := os.MkdirAll(opts.Path, 0755); err != nil {
		return nil, err
	}

	blockFile, err := os.Create(filepath.Join(opts.Path, file.BlocksFile))
	if err != nil {
		return nil, err
	}

	blockWriter := bufio.NewWriterSize(blockFile, 512000)

	indexWriter, err := NewIndexWriter(opts.Path, opts.BlockSize)
	if err != nil {
		return nil, err
	}
	chunk := new(ValueBlock)
	chunk.Items = make([]value.Value, 0, 1000)
	chunk.CRC = 0

	return &Writer{
		Opts:         opts,
		BlockWriter:  blockWriter,
		IndexWriter:  *indexWriter,
		Chunk:        *chunk,
		BlockFile:    blockFile,
		LowestSeqNo:  value.SeqNo(^uint64(0)), // MAX value
		HighestSeqNo: 0,
	}, nil
}

func (w *Writer) WriteBlock() error {
	if len(w.Chunk.Items) == 0 {
		return fmt.Errorf("chunk is empty")
	}

	var uncompressedChunkSize uint64
	for _, item := range w.Chunk.Items {
		uncompressedChunkSize += uint64(item.Size())
	}

	w.UncompressedSize += uncompressedChunkSize

	// Serialize block
	// @TODO: do these even work?
	buf := new(bytes.Buffer)
	err := w.Chunk.Serialize(buf)
	if err != nil {
		return err
	}

	// Compress using LZ4
	compressor := new(lz4.Compressor)
	compressedBytes := make([]byte, 0)
	bytesWritten, err := compressor.CompressBlock(buf.Bytes(), compressedBytes)
	if err != nil {
		return err
	}

	// Write to file
	if _, err := w.BlockWriter.Write(compressedBytes); err != nil {
		return err
	}

	firstItem := w.Chunk.Items[0]
	if err := w.IndexWriter.RegisterBlock(firstItem.Key, w.FilePos, uint32(bytesWritten)); err != nil {
		return err
	}

	// Adjust metadata
	w.FilePos += uint64(bytesWritten)
	w.ItemCount += len(w.Chunk.Items)
	w.BlockCount++
	w.Chunk.Items = w.Chunk.Items[:0]

	return nil
}

func (w *Writer) Write(item value.Value) error {
	if item.IsTombstone() {
		if w.Opts.EvictTombstones {
			return nil
		}
		w.TombstoneCount++
	}

	if !bytes.Equal(item.Key, w.CurrentKey) {
		w.KeyCount++
		w.CurrentKey = item.Key
	}

	itemKey := make([]byte, len(item.Key))
	copy(itemKey, item.Key)
	seqno := item.SeqNo

	w.ChunkSize += item.Size()
	w.Chunk.Items = append(w.Chunk.Items, item)

	if w.ChunkSize >= int(w.Opts.BlockSize) {
		if err := w.WriteBlock(); err != nil {
			return err
		}
		w.ChunkSize = 0
	}

	if w.FirstKey == nil {
		w.FirstKey = itemKey
	}
	w.LastKey = itemKey

	if w.LowestSeqNo > seqno {
		w.LowestSeqNo = seqno
	}

	if w.HighestSeqNo < seqno {
		w.HighestSeqNo = seqno
	}

	return nil
}

func (w *Writer) Finish() error {
	if len(w.Chunk.Items) > 0 {
		if err := w.WriteBlock(); err != nil {
			return err
		}
	}

	if w.ItemCount == 0 {
		if err := os.RemoveAll(w.Opts.Path); err != nil {
			return err
		}
		return nil
	}

	// First, flush the data blocks
	if err := w.BlockWriter.Flush(); err != nil {
		return err
	}

	// Append index blocks to file
	if err := w.IndexWriter.Finish(w.FilePos); err != nil {
		return err
	}
	// Then fsync the blocks file
	if err := w.BlockFile.Sync(); err != nil {
		return err
	}

	return nil
}
