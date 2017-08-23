package blockstore

import (
	"context"
	"testing"
)

func TestCachingOptsLessThanZero(t *testing.T) {
	opts := DefaultCacheOpts()
	opts.HasARCCacheSize = -1

	if _, err := CachedBlockstore(context.TODO(), nil, opts); err == nil {
		t.Error("wrong ARC setting was not detected")
	}

	opts = DefaultCacheOpts()
	opts.HasBloomFilterSize = -1

	if _, err := CachedBlockstore(context.TODO(), nil, opts); err == nil {
		t.Error("negative bloom size was not detected")
	}

	opts = DefaultCacheOpts()
	opts.HasBloomFilterHashes = -1

	if _, err := CachedBlockstore(context.TODO(), nil, opts); err == nil {
		t.Error("negative hashes setting was not detected")
	}
}

func TestBloomHashesAtZero(t *testing.T) {
	opts := DefaultCacheOpts()
	opts.HasBloomFilterHashes = 0

	if _, err := CachedBlockstore(context.TODO(), nil, opts); err == nil {
		t.Error("zero hashes setting with positive size was not detected")
	}
}
