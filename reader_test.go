package wal

import (
	"fmt"
	"io"
	"log"
	"testing"
	"time"
)

func TestReader(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		Directory:    dir,
		SegmentSize:  1 * GB,
		SyncInterval: 1 * time.Second,
	}

	wal, err := Open(opts)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer func() {
		if err := wal.Close(); err != nil {
			log.Fatalf("Failed to close WAL: %v", err)
		}
	}()

	// Write some data
	pos1, err := wal.Write([]byte("entry1"))
	if err != nil {
		log.Fatalf("Failed to write entry1: %v", err)
	}
	fmt.Println("pos1", pos1.EncodeString())

	pos2, err := wal.Write([]byte("entry2"))
	if err != nil {
		log.Fatalf("Failed to write entry2: %v", err)
	}
	fmt.Println("pos2", pos2.EncodeString())
	pos3, err := wal.Write([]byte("entry3"))
	if err != nil {
		log.Fatalf("Failed to write entry2: %v", err)
	}
	fmt.Println("pos3", pos3.EncodeString())

	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Create a reader starting from the first entry
	reader, err := wal.NewReader(pos1)
	if err != nil {
		log.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Read entries
	for {
		entry, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				log.Println("Reached the end of WAL")
				break
			}
			log.Fatalf("Failed to read entry: %v", err)
		}
		log.Printf("Read entry: %s", string(entry))
	}
}
