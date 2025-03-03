package wal

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSegment_New(t *testing.T) {
	tempDir := os.TempDir()
	path := filepath.Join(tempDir, "test_segment.wal")
	defer os.Remove(path)

	seg, err := NewSegment(1, path)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer seg.Close()

	if seg.id != 1 {
		t.Errorf("Expected segment ID to be 1, got %d", seg.id)
	}

	if seg.currentBlock == nil {
		t.Error("Expected currentBlock to be initialized")
	}
}

func TestSegment_WriteRead(t *testing.T) {
	tempDir := os.TempDir()
	path := filepath.Join(tempDir, "test_segment.wal")
	defer os.Remove(path)

	seg, err := NewSegment(1, path)
	if err != nil {
		t.Fatalf("Failed to create seg: %v", err)
	}
	defer seg.Close()

	data := []byte("Hello, WAL!")
	pos, err := seg.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Fatalf("Failed to sync seg: %v", err)
	}

	readData, err := seg.Read(pos)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	if !bytes.Equal(data, readData) {
		t.Errorf("Expected %q but got %q", data, readData)
	}
}

func TestSegment_Sync(t *testing.T) {
	tempDir := os.TempDir()
	path := filepath.Join(tempDir, "test_segment_sync.wal")
	defer os.Remove(path)

	seg, err := NewSegment(2, path)
	if err != nil {
		t.Fatalf("Failed to create seg: %v", err)
	}
	defer seg.Close()

	data := []byte("Sync test data")
	_, err = seg.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}
}

func TestSegment_Close(t *testing.T) {
	tempDir := os.TempDir()
	path := filepath.Join(tempDir, "test_segment_close.wal")
	defer os.Remove(path)

	seg, err := NewSegment(3, path)
	if err != nil {
		t.Fatalf("Failed to create seg: %v", err)
	}

	data := []byte("Hello, WAL!")
	_, err = seg.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	if err := seg.Close(); err != nil {
		t.Fatalf("Failed to close seg: %v", err)
	}

	if _, err := seg.Write([]byte("should fail")); err == nil {
		t.Errorf("Expected error when writing to a closed seg, got nil")
	}
}

func TestSegment_CRCValidation(t *testing.T) {
	path := "test_segment.log"
	defer os.Remove(path)

	seg, err := NewSegment(1, path)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer seg.Close()

	data := []byte("Hello, WAL!")
	pos, err := seg.Write(data)
	if err != nil {
		t.Fatalf("Failed to write data: %v", err)
	}

	fd, err := os.OpenFile(path, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("Failed to open file for tampering: %v", err)
	}
	defer fd.Close()

	tamperOffset := int64(pos.BlockId)*blockSize + pos.ChunkOffset + 4 // 跳过 CRC 字段
	if _, err := fd.WriteAt([]byte{0xFF}, tamperOffset); err != nil {
		t.Fatalf("Failed to tamper with file: %v", err)
	}

	_, err = seg.Read(pos)
	if err == nil {
		t.Error("Expected CRC validation error, got nil")
	}
}

func TestSegment_WriteLargeData(t *testing.T) {
	path := "test_segment.log"
	defer os.Remove(path)

	seg, err := NewSegment(1, path)
	if err != nil {
		t.Fatalf("Failed to create segment: %v", err)
	}
	defer seg.Close()

	data := make([]byte, blockSize*2)
	for i := range data {
		data[i] = byte(i % 256)
	}

	pos1, err := seg.Write(data)
	if err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	_, err = seg.Write(data)
	if err != nil {
		t.Fatalf("Failed to write large data: %v", err)
	}

	if err := seg.Sync(); err != nil {
		t.Fatalf("Sync failed: %v", err)
	}

	readData, err := seg.Read(pos1)
	if err != nil {
		t.Fatalf("Failed to read large data: %v", err)
	}

	if !bytes.Equal(data, readData) {
		t.Errorf("Expected data to be %v, got %v", data, readData)
	}

}
