package wal

import (
	"io"
	"sync"
)

// Reader reads entries from the WAL starting at a given position
type Reader struct {
	wal     *WAL
	pos     *Position
	current *Segment
	closed  bool
	mu      sync.Mutex
}

// Next reads the next entry from the WAL
func (r *Reader) Next() ([]byte, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, io.EOF
	}

	for {
		entry, err := r.current.Read(r.pos)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// Current segment is exhausted, move to the next segment
				nextSegmentId := r.pos.SegmentId + 1
				nextSegment, ok := r.wal.segments[nextSegmentId]
				if !ok {
					// No more segments, return EOF
					r.closed = true
					return nil, io.EOF
				}
				r.current = nextSegment
				r.pos = &Position{
					SegmentId: nextSegmentId,
					BlockId:   0,
					Offset:    0,
				}
				continue // Continue to read from the next segment
			}
			return nil, err
		}

		// Update the position
		r.pos.Offset += chunkHeaderSize + len(entry)

		// If the current block is exhausted, move to the next block
		if r.pos.Offset >= blockSize {
			r.pos.BlockId++
			r.pos.Offset = 0
		}

		return entry, nil
	}
}

// Close closes the Reader
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true
	r.current = nil // Release the current segment
	r.pos = nil     // Release the current position
	return nil
}
