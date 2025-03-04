package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type WAL struct {
	opts       Options
	segment    *Segment
	segments   map[int]*Segment
	closeC     chan struct{}
	syncTicker *time.Ticker
	mu         sync.Mutex
}

type Options struct {
	Directory    string
	SegmentSize  int64
	SyncInterval time.Duration
}

func Open(opts Options) (*WAL, error) {
	w := &WAL{
		opts:       opts,
		segments:   make(map[int]*Segment),
		closeC:     make(chan struct{}),
		syncTicker: time.NewTicker(opts.SyncInterval),
	}
	if err := w.initialize(); err != nil {
		return nil, err
	}
	go w.periodicSync()
	return w, nil
}

func (w *WAL) initialize() error {
	if err := os.MkdirAll(w.opts.Directory, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	entries, err := os.ReadDir(w.opts.Directory)
	if err != nil {
		return err
	}

	var segmentIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		if _, err := fmt.Sscanf(entry.Name(), "seg_%d.log", &id); err == nil {
			segmentIDs = append(segmentIDs, id)
		}
	}

	sort.Ints(segmentIDs)

	if len(segmentIDs) == 0 {
		segId := 0
		file := filepath.Join(w.opts.Directory, fmt.Sprintf("seg_%d.log", segId))
		seg, err := NewSegment(segId, file)
		if err != nil {
			return err
		}
		w.segment = seg
		w.segments[segId] = seg
	} else {
		for _, segId := range segmentIDs {
			file := filepath.Join(w.opts.Directory, fmt.Sprintf("seg_%d.log", segId))
			seg, err := NewSegment(segId, file)
			if err != nil {
				return err
			}
			w.segments[segId] = seg
		}
		w.segment = w.segments[segmentIDs[len(segmentIDs)-1]]
	}

	return nil
}

func (w *WAL) Read(pos *Position) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	seg, ok := w.segments[pos.SegmentId]
	if !ok {
		return nil, errors.New("segment not found")
	}
	return seg.Read(pos)
}

func (w *WAL) Write(data []byte) (*Position, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.segment.Size() >= w.opts.SegmentSize {
		if err := w.rotate(); err != nil {
			return nil, fmt.Errorf("write succeeded but segment rotation failed: %w", err)
		}
	}
	pos, err := w.segment.Write(data)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (w *WAL) rotate() error {
	oldSeg := w.segment
	segId := oldSeg.Id() + 1
	file := filepath.Join(w.opts.Directory, fmt.Sprintf("seg_%d.log", segId))
	seg, err := NewSegment(segId, file)
	if err != nil {
		return err
	}
	w.segments[segId] = seg // Add the new segment to the map
	w.segment = seg         // Set the new segment as the active segment
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	select {
	case <-w.closeC:
		return nil // Already closed
	default:
		close(w.closeC)
	}

	w.syncTicker.Stop()

	var errs []error
	for _, segment := range w.segments {
		if err := segment.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing segments: %v", errs)
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.segment.Sync()
}

func (w *WAL) periodicSync() {
	for {
		select {
		case <-w.syncTicker.C:
			w.mu.Lock()
			if err := w.segment.Sync(); err != nil {
				fmt.Println("sync error:", err)
			}
			w.mu.Unlock()
		case <-w.closeC:
			return
		}
	}
}
