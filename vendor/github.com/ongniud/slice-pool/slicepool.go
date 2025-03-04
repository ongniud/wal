package pool

import (
	"sort"
	"sync"
)

const (
	minBufferSize = 16
	maxBufferSize = 1024
	growFactor    = 2
)

// SlicePool is a pool for managing slices of different sizes.
type SlicePool[T any] struct {
	sizes      []int
	pools      []sync.Pool
	min, max   int
	allocCount int
	freeCount  int
}

// NewSlicePoolDefault creates a SlicePool with default configuration.
func NewSlicePoolDefault[T any]() *SlicePool[T] {
	return NewSlicePool[T](minBufferSize, maxBufferSize, growFactor)
}

// NewSlicePool creates a SlicePool with custom configuration.
func NewSlicePool[T any](min, max, factor int) *SlicePool[T] {
	// Generate slice sizes based on the minimum, maximum, and growth factor
	sizes := make([]int, 0)
	for size := min; size <= max; size *= factor {
		sizes = append(sizes, size)
	}
	sp := &SlicePool[T]{
		sizes: sizes,
		pools: make([]sync.Pool, len(sizes)),
		min:   min,
		max:   max,
	}
	for idx, size := range sizes {
		sz := size
		sp.pools[idx].New = func() interface{} {
			sp.allocCount++
			return make([]T, 0, sz)
		}
	}
	return sp
}

// Alloc allocates a slice of the specified size from the pool.
func (p *SlicePool[T]) Alloc(size int) []T {
	i := sort.SearchInts(p.sizes, size)
	if i < len(p.sizes) {
		return p.pools[i].Get().([]T)
	}
	return make([]T, 0, size)
}

// Free returns a slice to the pool.
func (p *SlicePool[T]) Free(slc []T) {
	p.freeCount++
	cp := cap(slc)
	i := sort.SearchInts(p.sizes, cp)
	if i < len(p.sizes) && p.sizes[i] == cp {
		slc = slc[:0]
		p.pools[i].Put(slc)
	}
}

// GetAllocCount returns the number of memory allocations.
func (p *SlicePool[T]) GetAllocCount() int {
	return p.allocCount
}

// GetFreeCount returns the number of slice frees.
func (p *SlicePool[T]) GetFreeCount() int {
	return p.freeCount
}
