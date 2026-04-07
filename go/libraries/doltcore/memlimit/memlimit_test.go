package memlimit

import (
	"math"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	b := defaults()
	assert.Equal(t, uint64(DefaultNodeCacheSize), b.NodeCache)
	assert.Equal(t, uint64(DefaultMemtableSize), b.Memtable)
	assert.Equal(t, uint64(DefaultDecodedChunksSize), b.DecodedChunks)
}

func TestComputeNoLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	debug.SetMemoryLimit(math.MaxInt64)
	b := compute()
	assert.Equal(t, uint64(DefaultNodeCacheSize), b.NodeCache)
	assert.Equal(t, uint64(DefaultMemtableSize), b.Memtable)
	assert.Equal(t, uint64(DefaultDecodedChunksSize), b.DecodedChunks)
}

func TestComputeZeroLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	debug.SetMemoryLimit(0)
	b := compute()
	assert.Equal(t, uint64(DefaultNodeCacheSize), b.NodeCache)
}

func TestCompute512MiB(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	limit := int64(512 * 1024 * 1024)
	debug.SetMemoryLimit(limit)

	b := compute()

	usable := float64(limit) * 0.75
	assert.Equal(t, uint64(usable*0.47), b.NodeCache)
	assert.Equal(t, uint64(usable*0.47), b.Memtable)
	assert.Equal(t, uint64(usable*0.06), b.DecodedChunks)

	total := int64(b.NodeCache) + int64(b.Memtable) + int64(b.DecodedChunks)
	assert.Less(t, total, limit, "total cache budget should be under GOMEMLIMIT")
}

func TestCompute128MiB(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	debug.SetMemoryLimit(int64(128 * 1024 * 1024))

	b := compute()

	assert.Greater(t, b.NodeCache, uint64(minNodeCacheSize))
	assert.Greater(t, b.Memtable, uint64(minMemtableSize))
	assert.Greater(t, b.DecodedChunks, uint64(minDecodedChunksSize))

	assert.Less(t, b.NodeCache, uint64(DefaultNodeCacheSize))
	assert.Less(t, b.Memtable, uint64(DefaultMemtableSize))
	assert.Less(t, b.DecodedChunks, uint64(DefaultDecodedChunksSize))
}

func TestComputeVerySmall(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	debug.SetMemoryLimit(int64(32 * 1024 * 1024))

	b := compute()

	assert.Equal(t, uint64(minNodeCacheSize), b.NodeCache)
	assert.GreaterOrEqual(t, b.Memtable, uint64(minMemtableSize))
	assert.Equal(t, uint64(minDecodedChunksSize), b.DecodedChunks)
}

func TestComputeLarge(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	debug.SetMemoryLimit(int64(4 * 1024 * 1024 * 1024))

	b := compute()

	assert.Greater(t, b.NodeCache, uint64(DefaultNodeCacheSize))
	assert.Greater(t, b.Memtable, uint64(DefaultMemtableSize))
	assert.Greater(t, b.DecodedChunks, uint64(DefaultDecodedChunksSize))
}

func TestAccessorsCallInit(t *testing.T) {
	require.Greater(t, NodeCacheSize(), uint64(0))
	require.Greater(t, MemtableSize(), uint64(0))
	require.Greater(t, DecodedChunksSize(), uint64(0))
}
