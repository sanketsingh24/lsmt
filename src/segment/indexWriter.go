package segment

import (
	"bagh/file"
	"bagh/value"
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"

	"github.com/pierrec/lz4/v4"
)

func concatFiles(srcPath, destPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer src.Close()

	// @TODO: check if this ok?
	dest, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer dest.Close()

	_, err = io.Copy(dest, src)
	return err
}

type IndexWriter struct {
	path             string
	filePos          uint64
	blockIndexWriter *bufio.Writer
	indexIndexWriter *bufio.Writer
	blockSize        uint32
	blockCounter     uint32
	blockChunk       BlockHandleBlock
	indexChunk       BlockHandleBlock
}

func NewIndexWriter(path string, blockSize uint32) (*IndexWriter, error) {
	blockFile, err := os.Create(filepath.Join(path, file.IndexBlocksFile))
	if err != nil {
		return nil, err
	}
	blockIndexWriter := bufio.NewWriterSize(blockFile, 65535)

	indexFile, err := os.Create(filepath.Join(path, file.TopLevelIndexFile))
	if err != nil {
		return nil, err
	}
	indexIndexWriter := bufio.NewWriter(indexFile)

	return &IndexWriter{
		path:             path,
		filePos:          0,
		blockIndexWriter: blockIndexWriter,
		indexIndexWriter: indexIndexWriter,
		blockSize:        blockSize,
		blockCounter:     0,
		blockChunk:       *new(BlockHandleBlock),
		indexChunk:       *new(BlockHandleBlock),
	}, nil
}

func (w *IndexWriter) writeBlock() error {
	// Serialize block
	err := w.blockChunk.CreateCRC()
	if err != nil {
		return err
	}
	// bhb := new(BlockHandleBlock)
	// bytes := make([]byte, 0, 65535) // u16::MAX @TODO:
	buf := new(bytes.Buffer)
	err = w.blockChunk.Serialize(buf)
	if err != nil {
		return err
	}

	if _, err := w.blockIndexWriter.Write(buf.Bytes()); err != nil {
		return err
	}

	first := w.blockChunk.Items[0]
	w.indexChunk.Items = append(w.indexChunk.Items, BlockHandle{
		StartKey: first.StartKey,
		Offset:   w.filePos,
		Size:     uint32(len(buf.Bytes())),
	})

	w.blockCounter = 0
	w.blockChunk.Items = w.blockChunk.Items[:0]
	w.filePos += uint64(len(buf.Bytes()))

	return nil
}

func (w *IndexWriter) RegisterBlock(startKey value.UserKey, offset uint64, size uint32) error {
	blockHandleSize := uint32(len(startKey)) + 12 // 12 is the size of offset and size fields

	reference := BlockHandle{
		StartKey: startKey,
		Offset:   offset,
		Size:     size,
	}
	w.blockChunk.Items = append(w.blockChunk.Items, reference)

	w.blockCounter += blockHandleSize

	if w.blockCounter >= w.blockSize {
		return w.writeBlock()
	}

	return nil
}

func (w *IndexWriter) writeTopLevelIndex(blockFileSize uint64) error {
	if err := w.blockIndexWriter.Flush(); err != nil {
		return err
	}

	if err := concatFiles(
		filepath.Join(w.path, file.IndexBlocksFile),
		filepath.Join(w.path, file.BlocksFile),
	); err != nil {
		return err
	}

	for i := range w.indexChunk.Items {
		w.indexChunk.Items[i].Offset += blockFileSize
	}

	if err := w.indexChunk.CreateCRC(); err != nil {
		return err
	}

	// @TODO: do these even work?
	buf := new(bytes.Buffer)
	err := w.indexChunk.Serialize(buf)
	if err != nil {
		return err
	}

	compressor := new(lz4.Compressor)
	compressedBytes := make([]byte, 0)
	if _, err := compressor.CompressBlock(buf.Bytes(), compressedBytes); err != nil {
		return err
	}

	if _, err := w.indexIndexWriter.Write(compressedBytes); err != nil {
		return err
	}

	return w.indexIndexWriter.Flush()
}

func (w *IndexWriter) Finish(blockFileSize uint64) error {
	if w.blockCounter > 0 {
		if err := w.writeBlock(); err != nil {
			return err
		}
	}

	if err := w.blockIndexWriter.Flush(); err != nil {
		return err
	}

	if err := w.writeTopLevelIndex(blockFileSize); err != nil {
		return err
	}

	if err := os.Remove(filepath.Join(w.path, file.IndexBlocksFile)); err != nil {
		return err
	}

	return nil
}

// func serializeAndCompress(block *DiskBlock) ([]byte, error) {
// 	var buf bytes.Buffer
// 	if err := block.Serialize(&buf); err != nil {
// 		return nil, err
// 	}

// 	var compressed bytes.Buffer
// 	lz4IndexWriter := lz4.NewIndexWriter(&compressed)
// 	if _, err := lz4IndexWriter.Write(buf.Bytes()); err != nil {
// 		return nil, err
// 	}
// 	if err := lz4IndexWriter.Close(); err != nil {
// 		return nil, err
// 	}

// 	return compressed.Bytes(), nil
// }
