package wal

import (
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var wal *WAL

func init() {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := Options{
		Directory:    dir,
		SegmentSize:  1 * GB,
		SyncInterval: 1 * time.Hour,
	}
	var err error
	wal, err = Open(opts)
	if err != nil {
		panic(err)
	}
}

func BenchmarkWAL_Write(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := wal.Write([]byte("Hello World"))
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_WriteLargeSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	content := []byte(strings.Repeat("X", 256*KB+500))
	for i := 0; i < b.N; i++ {
		_, err := wal.Write(content)
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_Read(b *testing.B) {
	var positions []*Position
	for i := 0; i < 1000000; i++ {
		pos, err := wal.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}
	err := wal.Sync()
	assert.Nil(b, err)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := wal.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
}
