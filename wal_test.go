package wal

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWAL(t *testing.T) {
	dir := "test_wal"
	opts := Options{
		SegmentSize:  1024, // 1 KB
		SyncInterval: 1 * time.Second,
	}

	// Clean up any previous test data
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Test Open and Initialize
	wal, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	// Test Write
	data := []byte("test data")
	pos, err := wal.Write(data)
	if err != nil {
		t.Fatalf("Failed to write to WAL: %v", err)
	}

	// Test Read
	readData, err := wal.Read(pos)
	if err != nil {
		t.Fatalf("Failed to read from WAL: %v", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("Read data does not match written data: got %s, want %s", readData, data)
	}

	// Test Segment Rotation
	for i := 0; i < 10; i++ {
		_, err := wal.Write(make([]byte, 100)) // Write 100 bytes each time
		if err != nil {
			t.Fatalf("Failed to write to WAL: %v", err)
		}
	}

	// Ensure that the segment has rotated
	if len(wal.segments) < 2 {
		t.Fatalf("Expected at least 2 segments, got %d", len(wal.segments))
	}

	// Test Sync
	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Test Close
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}

	// Reopen WAL to ensure data persistence
	wal, err = Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal.Close()

	// Verify that the data is still there after reopening
	readData, err = wal.Read(pos)
	if err != nil {
		t.Fatalf("Failed to read from WAL after reopening: %v", err)
	}
	if string(readData) != string(data) {
		t.Fatalf("Read data does not match written data after reopening: got %s, want %s", readData, data)
	}
}

func TestWAL_ConcurrentWrite(t *testing.T) {
	dir := "test_wal_concurrent"
	opts := Options{
		SegmentSize:  1024, // 1 KB
		SyncInterval: 1 * time.Second,
	}

	// Clean up any previous test data
	os.RemoveAll(dir)
	//defer os.RemoveAll(dir)

	wal, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	var poses sync.Map
	var wg sync.WaitGroup
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte(fmt.Sprintf("test data %d", i))
			pos, err := wal.Write(data)
			if err != nil {
				t.Errorf("Failed to write to WAL: %v", err)
			}
			poses.Store(i, pos)
		}(i)
	}
	wg.Wait()

	// Test Sync
	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to sync WAL: %v", err)
	}

	// Ensure that all data was written correctly
	for i := 5000; i < 5010; i++ {
		data := []byte(fmt.Sprintf("test data %d", i))
		p, _ := poses.Load(i)
		pos := p.(*Position)
		readData, err := wal.Read(pos)
		if err != nil {
			t.Fatalf("Failed to read from WAL: %v", err)
		}
		if string(readData) != string(data) {
			t.Fatalf("Read data does not match written data: got %s, want %s", readData, data)
		}
	}
}

func TestWAL_BasicOperations(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		SegmentSize:  1024,
		SyncInterval: 10 * time.Millisecond,
	}
	wal, err := Open(dir, opts)
	assert.NoError(t, err)
	defer wal.Close()

	data := []byte("hello wal")
	pos, err := wal.Write(data)
	assert.NoError(t, err)
	assert.NotNil(t, pos)

	wal.Sync()

	readData, err := wal.Read(pos)
	assert.NoError(t, err)
	assert.Equal(t, data, readData)
}

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		SegmentSize:  20,
		SyncInterval: 10 * time.Millisecond,
	}
	wal, err := Open(dir, opts)
	assert.NoError(t, err)
	defer wal.Close()

	data1 := []byte("first entry")
	pos1, err := wal.Write(data1)
	assert.NoError(t, err)

	data2 := []byte("second entry that triggers rotation")
	pos2, err := wal.Write(data2)
	assert.NoError(t, err)

	assert.NotEqual(t, pos1.SegmentId, pos2.SegmentId)
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		SegmentSize:  1024,
		SyncInterval: 10 * time.Millisecond,
	}
	wal, err := Open(dir, opts)
	assert.NoError(t, err)
	defer wal.Close()

	var wg sync.WaitGroup
	for i := 0; i < 10000000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data := []byte("entry " + strconv.FormatInt(int64(i), 10))
			pos, err := wal.Write(data)
			assert.NoError(t, err)
			assert.NotNil(t, pos)
		}(i)
	}
	wg.Wait()
}

func TestWAL_Sync(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		SegmentSize:  1024,
		SyncInterval: 10 * time.Millisecond,
	}
	wal, err := Open(dir, opts)
	assert.NoError(t, err)
	defer wal.Close()

	assert.NoError(t, wal.Sync())
}

func TestWAL_Close(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		SegmentSize:  1024,
		SyncInterval: 10 * time.Millisecond,
	}
	wal, err := Open(dir, opts)
	assert.NoError(t, err)
	assert.NoError(t, wal.Close())
	assert.Error(t, wal.Sync())
}
