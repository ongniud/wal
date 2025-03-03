package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// Block size, currently set to 128 bytes (originally 32KB)
const (
	blockSize       = 128
	chunkHeaderSize = 7
)

// ChunkType represents the type of a chunk, stored as a byte
type ChunkType byte

// Define different chunk types
const (
	kFullType ChunkType = iota
	kFirstType
	kMiddleType
	kLastType
)

// Error constants
var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

// Position records the position of a chunk
type Position struct {
	SegmentId   int   // Segment file ID
	BlockId     int   // Block ID
	ChunkOffset int64 // Chunk offset
}

// Segment represents the Write-Ahead Log segment
type Segment struct {
	id           int
	fd           *os.File
	closed       bool
	currentBlock *block
}

// block represents a block structure
type block struct {
	id      int
	data    []byte
	flushed int // Record the offset of the data that has been written to disk
}

// NewSegment creates a new Segment
func NewSegment(id int, path string) (*Segment, error) {
	fmt.Printf("Creating new segment with ID %d at path %s\n", id, path)
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("Failed to open file %s: %v\n", path, err)
		return nil, err
	}

	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		fmt.Printf("Failed to seek file %s: %v\n", path, err)
		_ = fd.Close()
		return nil, err
	}

	// Calculate the number of existing blocks
	blockCount := int(offset / int64(blockSize))
	blockOccupy := offset % blockSize
	blockData := make([]byte, 0, blockSize)
	if blockOccupy != 0 {
		fmt.Printf("WARNING !")
		if _, err := fd.Seek(offset-blockOccupy, io.SeekStart); err != nil {
			fmt.Printf("Failed to seek to offset %d: %v\n", offset-blockOccupy, err)
			return nil, err
		}
		_, err := fd.Read(blockData)
		if err != nil && err != io.ErrUnexpectedEOF {
			fmt.Printf("Failed to read : %v\n", err)
			return nil, err
		}
	}

	seg := &Segment{
		fd: fd,
		id: id,
		currentBlock: &block{
			id:   blockCount,
			data: blockData,
		},
	}

	fmt.Printf("Segment %d created successfully at offset %d\n", id, offset)
	return seg, nil
}

// Size returns the total disk space occupied by the current Segment
func (s *Segment) Size() int64 {
	return int64((s.currentBlock.id + 1) * blockSize)
}

// Id returns the ID of the Segment
func (s *Segment) Id() int {
	return s.id
}

// Write writes data and returns the Position
func (s *Segment) Write(data []byte) (*Position, error) {
	if s.closed {
		fmt.Println("Failed to write: segment is closed")
		return nil, ErrClosed
	}

	fmt.Printf("Writing data of length %d to segment %d\n", len(data), s.id)
	chunks := s.splitIntoChunks(data)

	var pos *Position
	for i, chunk := range chunks {
		if len(s.currentBlock.data)+chunkHeaderSize+len(chunk.data) > blockSize {
			fmt.Printf("Block %d is full, flushing...\n", s.currentBlock.id)
			if err := s.flushBlock(true); err != nil {
				fmt.Printf("Failed to flush block %d: %v\n", s.currentBlock.id, err)
				return nil, err
			}
		}
		position, err := s.writeChunk(chunk.data, chunk.chunkType)
		if err != nil {
			fmt.Printf("Failed to write chunk %d: %v\n", i, err)
			return nil, err
		}
		if i == 0 {
			pos = position
		}
	}

	fmt.Printf("Data written successfully. Position: SegmentId=%d, BlockId=%d, ChunkOffset=%d\n", pos.SegmentId, pos.BlockId, pos.ChunkOffset)
	return pos, nil
}

// writeChunk writes a chunk and returns the Position
func (s *Segment) writeChunk(data []byte, chunkType ChunkType) (*Position, error) {
	header := make([]byte, chunkHeaderSize)
	binary.LittleEndian.PutUint32(header[:4], crc32.ChecksumIEEE(data))
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(data)))
	header[6] = byte(chunkType)
	chunkOffset := int64(len(s.currentBlock.data))
	s.currentBlock.data = append(s.currentBlock.data, header...)
	s.currentBlock.data = append(s.currentBlock.data, data...)
	fmt.Printf("Chunk of type %d and length %d written at position: SegmentId=%d, BlockId=%d, ChunkOffset=%d\n", chunkType, len(data), s.id, s.currentBlock.id, chunkOffset)
	return &Position{
		SegmentId:   s.id,
		BlockId:     s.currentBlock.id,
		ChunkOffset: chunkOffset,
	}, nil
}

// flushBlock flushes the block to disk
func (s *Segment) flushBlock(padding bool) error {
	dataToWrite := s.currentBlock.data[s.currentBlock.flushed:]
	if len(dataToWrite) == 0 && !padding {
		return nil
	}

	if padding && len(s.currentBlock.data) < blockSize {
		paddingSize := blockSize - len(s.currentBlock.data)
		padding := make([]byte, paddingSize)
		s.currentBlock.data = append(s.currentBlock.data, padding...)
		dataToWrite = s.currentBlock.data[s.currentBlock.flushed:]
	}

	n, err := s.fd.Write(dataToWrite)
	if err != nil {
		return err
	}

	s.currentBlock.flushed += n

	if s.currentBlock.flushed == blockSize {
		s.currentBlock.id++
		s.currentBlock.flushed = 0
		s.currentBlock.data = s.currentBlock.data[:0]
	}
	return nil
}

// chunk represents a data chunk
type chunk struct {
	data      []byte
	chunkType ChunkType
}

// splitIntoChunks splits the data into chunks
func (s *Segment) splitIntoChunks(data []byte) []chunk {
	var chunks []chunk
	remaining := len(data)
	offset := 0

	remainingSpace := blockSize - len(s.currentBlock.data) - chunkHeaderSize
	if remainingSpace > 0 {
		fmt.Printf("write to current block, remainingSpace=%d\n", remainingSpace)
		chunkSize := remainingSpace
		if chunkSize > remaining {
			chunkSize = remaining
		}

		chunkType := kFirstType
		if remaining == len(data) && chunkSize == len(data) {
			chunkType = kFullType
		}

		chunks = append(chunks, chunk{
			data:      data[offset : offset+chunkSize],
			chunkType: chunkType,
		})

		offset += chunkSize
		remaining -= chunkSize
	}

	for remaining > 0 {
		chunkSize := blockSize - chunkHeaderSize
		if chunkSize > remaining {
			chunkSize = remaining
		}

		var chunkType ChunkType
		if remaining == len(data) && chunkSize == len(data) {
			chunkType = kFullType
		} else if remaining == len(data) {
			chunkType = kFirstType
		} else if remaining == chunkSize {
			chunkType = kLastType
		} else {
			chunkType = kMiddleType
		}

		chunks = append(chunks, chunk{
			data:      data[offset : offset+chunkSize],
			chunkType: chunkType,
		})

		offset += chunkSize
		remaining -= chunkSize
	}

	fmt.Printf("Data split into %d chunks, data size=%d, block size=%d, chunk size=%d\n", len(chunks), len(data), blockSize, blockSize-chunkHeaderSize)
	return chunks
}

// Read reads the WAL record
func (s *Segment) Read(pos *Position) ([]byte, error) {
	fmt.Printf("Reading entry from position: SegmentId=%d, BlockId=%d, ChunkOffset=%d\n", pos.SegmentId, pos.BlockId, pos.ChunkOffset)
	var entry []byte
	currentPos := &Position{
		SegmentId:   pos.SegmentId,
		BlockId:     pos.BlockId,
		ChunkOffset: pos.ChunkOffset,
	}

	for {
		blockData, err := s.readBlock(currentPos.BlockId)
		if err != nil {
			fmt.Printf("Failed to read block %d: %v\n", currentPos.BlockId, err)
			return nil, err
		}

		if currentPos.ChunkOffset >= int64(len(blockData)) {
			fmt.Printf("Chunk offset %d is out of range for block %d\n", currentPos.ChunkOffset, currentPos.BlockId)
			return nil, fmt.Errorf("chunk offset %d is out of range for block %d", currentPos.ChunkOffset, currentPos.BlockId)
		}

		chunk, err := s.readChunk(blockData[currentPos.ChunkOffset:])
		if err != nil {
			fmt.Printf("Failed to read chunk from block %d at offset %d: %v\n", currentPos.BlockId, currentPos.ChunkOffset, err)
			return nil, err
		}

		if len(entry) == 0 {
			if chunk.chunkType != kFullType && chunk.chunkType != kFirstType {
				return nil, fmt.Errorf("invalid first chunk type: %v", chunk.chunkType)
			}
		} else if chunk.chunkType != kMiddleType && chunk.chunkType != kLastType {
			return nil, fmt.Errorf("invalid chunk type: %v", chunk.chunkType)
		}

		entry = append(entry, chunk.data...)

		if chunk.chunkType == kLastType || chunk.chunkType == kFullType {
			fmt.Printf("Entry read successfully. Length: %d\n", len(entry))
			return entry, nil
		}
		currentPos.ChunkOffset += int64(chunkHeaderSize + len(chunk.data))
		if currentPos.ChunkOffset >= int64(len(blockData)) {
			currentPos.BlockId++
			currentPos.ChunkOffset = 0
		}
	}
}

// readBlock reads the specified block
func (s *Segment) readBlock(blockID int) ([]byte, error) {
	if s.closed {
		fmt.Printf("Failed to read block %d: segment is closed\n", blockID)
		return nil, ErrClosed
	}

	blockOffset := int64(blockID) * blockSize

	if _, err := s.fd.Seek(blockOffset, io.SeekStart); err != nil {
		fmt.Printf("Failed to seek to block %d at offset %d: %v\n", blockID, blockOffset, err)
		return nil, err
	}

	blockData := make([]byte, blockSize)
	_, err := io.ReadFull(s.fd, blockData)
	if err != nil && err != io.ErrUnexpectedEOF {
		fmt.Printf("Failed to read block %d: %v\n", blockID, err)
		return nil, err
	}

	fmt.Printf("Block %d read successfully. Length: %d\n", blockID, len(blockData))
	return blockData, nil
}

// Sync synchronizes the data to disk
func (s *Segment) Sync() error {
	if s.closed {
		fmt.Println("Failed to sync: segment is closed")
		return ErrClosed
	}

	fmt.Println("Syncing segment...")
	if err := s.flushBlock(false); err != nil {
		fmt.Printf("Failed to flush block during sync: %v\n", err)
		return err
	}

	if err := s.fd.Sync(); err != nil {
		fmt.Printf("Failed to sync file: %v\n", err)
		return err
	}

	fmt.Println("Segment synced successfully")
	return nil
}

// readChunk parses the chunk
func (s *Segment) readChunk(data []byte) (chunk, error) {
	if len(data) < chunkHeaderSize {
		fmt.Println("Insufficient data for chunk header")
		return chunk{}, io.ErrUnexpectedEOF
	}

	expectedCRC := binary.LittleEndian.Uint32(data[:4])
	length := binary.LittleEndian.Uint16(data[4:6])
	chunkType := ChunkType(data[6])

	if int(length)+chunkHeaderSize > len(data) {
		fmt.Println("Insufficient data for chunk")
		return chunk{}, io.ErrUnexpectedEOF
	}

	chunkData := data[chunkHeaderSize : chunkHeaderSize+int(length)]

	actualCRC := crc32.ChecksumIEEE(chunkData)
	if actualCRC != expectedCRC {
		fmt.Println("CRC mismatch for chunk")
		return chunk{}, ErrInvalidCRC
	}

	fmt.Printf("Chunk of type %d and length %d read successfully\n", chunkType, len(chunkData))
	return chunk{
		data:      data[chunkHeaderSize : chunkHeaderSize+int(length)],
		chunkType: chunkType,
	}, nil
}

// Close closes the segment
func (s *Segment) Close() error {
	if s.closed {
		fmt.Println("Segment is already closed")
		return nil
	}

	fmt.Println("Closing segment...")
	if err := s.flushBlock(true); err != nil {
		fmt.Printf("Failed to flush block during close: %v\n", err)
		return err
	}

	if err := s.fd.Sync(); err != nil {
		fmt.Printf("Failed to sync file during close: %v\n", err)
		return err
	}

	s.closed = true
	if err := s.fd.Close(); err != nil {
		fmt.Printf("Failed to close file: %v\n", err)
		return err
	}

	fmt.Println("Segment closed successfully")
	return nil
}
