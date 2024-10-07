package wal

import (
	"bagh/memtable"
	"bagh/value"
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type WalEntry struct {
	Key       string      `json:"k"`
	Value     string      `json:"v"`
	Seqno     value.SeqNo `json:"s"`
	ValueType uint8       `json:"t"`
}

type Wal struct {
	Writer *os.File
	mutex  *sync.Mutex
}

func OpenWal(path string) (*Wal, *memtable.MemTable, error) {
	walPath := filepath.Join(path, ".wal.jsonl")

	if _, err := os.Stat(walPath); err == nil {
		memtable, err := recoverWal(walPath)
		if err != nil {
			return nil, nil, err
		}

		writer, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return nil, nil, err
		}

		wal := &Wal{Writer: writer}
		return wal, memtable, nil
	} else if os.IsNotExist(err) {
		writer, err := os.OpenFile(walPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return nil, nil, err
		}

		wal := &Wal{Writer: writer}
		return wal, &memtable.MemTable{}, nil
	} else {
		return nil, nil, err
	}
}

func (w *Wal) Write(value value.Value) error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	walEntry := WalEntry{
		Key:       string(value.Key),
		Value:     string(value.Value),
		Seqno:     value.SeqNo,
		ValueType: uint8(value.ValueType),
	}

	data, err := json.Marshal(walEntry)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintln(w.Writer, string(data))
	return err
}

func (w *Wal) Sync() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.Writer.Sync()
}

func (w *Wal) Truncate() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if err := w.Writer.Truncate(0); err != nil {
		return err
	}
	_, err := w.Writer.Seek(0, 0)
	if err != nil {
		return err
	}
	return w.Writer.Sync()
}

func recoverWal(path string) (*memtable.MemTable, error) {
	fmt.Println("Recovering WAL")

	memtable := &memtable.MemTable{}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	cnt := 0

	for idx := 0; scanner.Scan(); idx++ {
		line := scanner.Text()
		if line == "" {
			break
		}

		var entry WalEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			fmt.Printf("Truncating WAL to line %d because of malformed content\n", idx)
			break
		}

		value := value.Value{
			Key:       []byte(entry.Key),
			Value:     []byte(entry.Value),
			SeqNo:     entry.Seqno,
			ValueType: value.ValueTypeFromByte(entry.ValueType),
		}
		memtable.Insert(value)
		cnt++
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	fmt.Printf("Recovered %d items from WAL\n", cnt)

	return memtable, nil
}
