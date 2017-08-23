package blockstore

import (
	"errors"

	context "context"
	"gx/ipfs/QmRg1gKTHzc3CZXSKzem8aR4E3TubFhbgXwfVuWnSK5CC5/go-metrics-interface"
)

// CacheOpts wraps options for CachedBlockStore().
// Next to each option is it aproximate memory usage per unit
type CacheOpts struct {
	HasBloomFilterSize   int // 1 byte
	HasBloomFilterHashes int // No size, 7 is usually best, consult bloom papers
	HasARCCacheSize      int // 32 bytes
}

// DefaultCacheOpts returns a CacheOpts initialized with default values.
func DefaultCacheOpts() CacheOpts {
	return CacheOpts{
		HasBloomFilterSize:   512 << 10,
		HasBloomFilterHashes: 7,
		HasARCCacheSize:      64 << 10,
	}
}

// CachedBlockstore returns a blockstore wrapped in an ARCCache and
// then in a bloom filter cache, if the options indicate it.
func CachedBlockstore(
	ctx context.Context,
	bs Blockstore,
	opts CacheOpts) (cbs Blockstore, err error) {
	cbs = bs

	if opts.HasBloomFilterSize < 0 || opts.HasBloomFilterHashes < 0 ||
		opts.HasARCCacheSize < 0 {
		return nil, errors.New("all options for cache need to be greater than zero")
	}

	if opts.HasBloomFilterSize != 0 && opts.HasBloomFilterHashes == 0 {
		return nil, errors.New("bloom filter hash count can't be 0 when there is size set")
	}

	ctx = metrics.CtxSubScope(ctx, "bs.cache")

	if opts.HasARCCacheSize > 0 {
		cbs, err = newARCCachedBS(ctx, cbs, opts.HasARCCacheSize)
	}
	if opts.HasBloomFilterSize != 0 {
		// *8 because of bytes to bits conversion
		cbs, err = bloomCached(ctx, cbs, opts.HasBloomFilterSize*8, opts.HasBloomFilterHashes)
	}

	return cbs, err
}
