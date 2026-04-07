package memlimit

import (
	"math"
	"runtime/debug"
	"sync"
)

const (
	DefaultNodeCacheSize     = 256 * 1024 * 1024 // 256 MiB
	DefaultMemtableSize      = 256 * 1024 * 1024 // 256 MiB
	DefaultDecodedChunksSize = 1 << 25            // 32 MiB

	minNodeCacheSize     = 16 * 1024 * 1024 // 16 MiB
	minMemtableSize      = 4 * 1024 * 1024  // 4 MiB
	minDecodedChunksSize = 4 * 1024 * 1024  // 4 MiB
)

type Budget struct {
	NodeCache     uint64
	Memtable      uint64
	DecodedChunks uint64
}

var (
	once    sync.Once
	current Budget
)

// Init reads GOMEMLIMIT and partitions the memory budget across caches.
// When GOMEMLIMIT is not set, all sizes remain at their current defaults.
func Init() {
	once.Do(func() {
		current = compute()
	})
}

func compute() Budget {
	limit := debug.SetMemoryLimit(-1)
	if limit == math.MaxInt64 || limit <= 0 {
		return defaults()
	}

	// Partition 75% of the limit across caches, reserving the rest for
	// GC/stacks. Ratios mirror the original defaults (256:256:32 MiB).
	usable := float64(limit) * 0.75

	b := Budget{
		NodeCache:     uint64(usable * 0.47),
		Memtable:      uint64(usable * 0.47),
		DecodedChunks: uint64(usable * 0.06),
	}

	if b.NodeCache < minNodeCacheSize {
		b.NodeCache = minNodeCacheSize
	}
	if b.Memtable < minMemtableSize {
		b.Memtable = minMemtableSize
	}
	if b.DecodedChunks < minDecodedChunksSize {
		b.DecodedChunks = minDecodedChunksSize
	}

	return b
}

func defaults() Budget {
	return Budget{
		NodeCache:     DefaultNodeCacheSize,
		Memtable:      DefaultMemtableSize,
		DecodedChunks: DefaultDecodedChunksSize,
	}
}

// NodeCacheSize returns the byte size for the prolly tree node cache.
func NodeCacheSize() uint64 {
	Init()
	return current.NodeCache
}

// MemtableSize returns the byte size for the NBS memtable write buffer.
func MemtableSize() uint64 {
	Init()
	return current.Memtable
}

// DecodedChunksSize returns the byte size for the ValueStore decoded chunks cache.
func DecodedChunksSize() uint64 {
	Init()
	return current.DecodedChunks
}
