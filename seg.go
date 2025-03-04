package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	sp "github.com/ongniud/slice-pool"
)

var (
	bp = sp.NewSlicePoolDefault[byte]()
)

// Block size, currently set to 128 bytes (originally 32KB)
const (
	blockSize       = 32 * KB
	chunkHeaderSize = 7
)

// ChunkType represents the type of chunk, stored as a byte
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
	cachedBlock  *block // 缓存最近读取的块
}

// block represents a block structure
type block struct {
	id      int
	data    []byte
	flushed int // Record the offset of the data that has been written to disk
}

// NewSegment creates a new Segment
func NewSegment(id int, path string) (*Segment, error) {
	// os.O_TRUNC
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		_ = fd.Close()
		return nil, err
	}

	// Calculate the number of existing blocks
	blockCount := int(offset / int64(blockSize))
	blockOccupy := offset % blockSize
	blockData := make([]byte, 0, blockSize)
	if blockOccupy != 0 {
		if _, err := fd.Seek(offset-blockOccupy, io.SeekStart); err != nil {
			return nil, err
		}
		occupy := make([]byte, blockOccupy)
		_, err := fd.Read(occupy)
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		blockData = append(blockData, occupy...)
	}

	seg := &Segment{
		fd: fd,
		id: id,
		currentBlock: &block{
			id:   blockCount,
			data: blockData,
		},
		cachedBlock: &block{
			id:   -1,
			data: make([]byte, blockSize),
		},
	}
	return seg, nil
}

// Size returns the total disk space occupied by the current Segment
func (s *Segment) Size() int64 {
	if s.currentBlock.flushed == 0 {
		return int64(s.currentBlock.id * blockSize)
	}
	if s.currentBlock.id <= 0 {
		return int64(s.currentBlock.flushed)
	}
	return int64((s.currentBlock.id-1)*blockSize + s.currentBlock.flushed)
}

// Id returns the ID of the Segment
func (s *Segment) Id() int {
	return s.id
}

// Write writes data and returns the Position
func (s *Segment) Write(data []byte) (*Position, error) {
	if s.closed {
		return nil, ErrClosed
	}

	chunks := s.splitIntoChunks(data)
	var pos *Position
	for i, chk := range chunks {
		if len(s.currentBlock.data)+chunkHeaderSize+len(chk.data) > blockSize {
			if err := s.flushBlock(true); err != nil {
				return nil, err
			}
		}
		position, err := s.writeChunk(chk.data, chk.chunkType)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			pos = position
		}
	}

	//log.Printf("Data written successfully. Position: SegmentId=%d, BlockId=%d, ChunkOffset=%d, dataSize=%d, ChunkCount=%d\n", pos.SegmentId, pos.BlockId, pos.ChunkOffset, len(data), len(chunks))
	return pos, nil
}

// writeChunk writes a chunk and returns the Position
func (s *Segment) writeChunk(data []byte, chunkType ChunkType) (*Position, error) {
	header := bp.Alloc(chunkHeaderSize)[0:chunkHeaderSize]
	binary.LittleEndian.PutUint32(header[:4], crc32.ChecksumIEEE(data))
	binary.LittleEndian.PutUint16(header[4:6], uint16(len(data)))
	header[6] = byte(chunkType)
	chunkOffset := int64(len(s.currentBlock.data))
	s.currentBlock.data = append(s.currentBlock.data, header...)
	s.currentBlock.data = append(s.currentBlock.data, data...)
	bp.Free(header)
	return &Position{
		SegmentId:   s.id,
		BlockId:     s.currentBlock.id,
		ChunkOffset: chunkOffset,
	}, nil
}

// flushBlock flushes the block to disk
func (s *Segment) flushBlock(padding bool) error {
	data := s.currentBlock.data[s.currentBlock.flushed:]
	if len(data) == 0 && !padding {
		return nil
	}

	if padding && len(s.currentBlock.data) < blockSize {
		paddingSize := blockSize - len(s.currentBlock.data)
		padding := bp.Alloc(paddingSize)[0:paddingSize]
		s.currentBlock.data = append(s.currentBlock.data, padding...)
		data = s.currentBlock.data[s.currentBlock.flushed:]
		bp.Free(padding)
	}

	n, err := s.fd.Write(data)
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
	return chunks
}

// Read reads the WAL record
func (s *Segment) Read(pos *Position) ([]byte, error) {
	var entry []byte
	currentPos := &Position{
		SegmentId:   pos.SegmentId,
		BlockId:     pos.BlockId,
		ChunkOffset: pos.ChunkOffset,
	}

	for {
		blockData, err := s.readBlock(currentPos.BlockId)
		if err != nil {
			return nil, err
		}

		if currentPos.ChunkOffset >= int64(len(blockData)) {
			return nil, fmt.Errorf("chk offset %d is out of range for block %d", currentPos.ChunkOffset, currentPos.BlockId)
		}

		chk, err := s.readChunk(blockData[currentPos.ChunkOffset:])
		if err != nil {
			return nil, err
		}

		if len(entry) == 0 {
			if chk.chunkType != kFullType && chk.chunkType != kFirstType {
				return nil, fmt.Errorf("invalid first chk type: %v", chk.chunkType)
			}
		} else if chk.chunkType != kMiddleType && chk.chunkType != kLastType {
			return nil, fmt.Errorf("invalid chk type: %v", chk.chunkType)
		}

		entry = append(entry, chk.data...)

		if chk.chunkType == kLastType || chk.chunkType == kFullType {
			return entry, nil
		}

		currentPos.ChunkOffset += int64(chunkHeaderSize + len(chk.data))
		if currentPos.ChunkOffset >= int64(len(blockData)) {
			currentPos.BlockId++
			currentPos.ChunkOffset = 0
		}
	}
}

// readBlock reads the specified block
func (s *Segment) readBlock(blockID int) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}

	if s.cachedBlock != nil && s.cachedBlock.id == blockID {
		return s.cachedBlock.data, nil
	}

	blockOffset := int64(blockID) * blockSize
	if _, err := s.fd.Seek(blockOffset, io.SeekStart); err != nil {
		return nil, err
	}

	s.cachedBlock.id = blockID
	s.cachedBlock.data = s.cachedBlock.data[0:blockSize]
	_, err := io.ReadFull(s.fd, s.cachedBlock.data)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	return s.cachedBlock.data, nil
}

// Sync synchronizes the data to disk
func (s *Segment) Sync() error {
	if s.closed {
		return ErrClosed
	}
	if err := s.flushBlock(false); err != nil {
		return err
	}
	if err := s.fd.Sync(); err != nil {
		return err
	}
	return nil
}

// readChunk parses the chunk
func (s *Segment) readChunk(data []byte) (chunk, error) {
	if len(data) < chunkHeaderSize {
		return chunk{}, io.ErrUnexpectedEOF
	}
	expectedCRC := binary.LittleEndian.Uint32(data[:4])
	length := binary.LittleEndian.Uint16(data[4:6])
	chunkType := ChunkType(data[6])
	if int(length)+chunkHeaderSize > len(data) {
		return chunk{}, io.ErrUnexpectedEOF
	}
	chunkData := data[chunkHeaderSize : chunkHeaderSize+int(length)]
	actualCRC := crc32.ChecksumIEEE(chunkData)
	if actualCRC != expectedCRC {
		return chunk{}, ErrInvalidCRC
	}
	return chunk{
		data:      data[chunkHeaderSize : chunkHeaderSize+int(length)],
		chunkType: chunkType,
	}, nil
}

// Close closes the segment
func (s *Segment) Close() error {
	if s.closed {
		return nil
	}
	if err := s.flushBlock(true); err != nil {
		return err
	}
	if err := s.fd.Sync(); err != nil {
		return err
	}
	s.closed = true
	if err := s.fd.Close(); err != nil {
		return err
	}
	return nil
}
