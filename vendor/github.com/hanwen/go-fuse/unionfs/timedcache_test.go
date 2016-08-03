package unionfs

import (
	"testing"
	"time"
)

func TestTimedCacheUncacheable(t *testing.T) {
	fetchCount := 0
	fetch := func(n string) (interface{}, bool) {
		fetchCount++
		i := int(n[0])
		return &i, false
	}

	cache := NewTimedCache(fetch, 0)
	v := cache.Get("n").(*int)
	w := cache.Get("n").(*int)
	if *v != int('n') || *w != *v {
		t.Errorf("value mismatch: got %d, %d want %d", *v, *w, int('n'))
	}

	if fetchCount != 2 {
		t.Fatalf("Should have fetched twice: %d", fetchCount)
	}
}

func TestTimedCache(t *testing.T) {
	fetchCount := 0
	fetch := func(n string) (interface{}, bool) {
		fetchCount++
		i := int(n[0])
		return &i, true
	}

	// This fails with 1e6 on some Opteron CPUs.
	ttl := 100 * time.Millisecond

	cache := NewTimedCache(fetch, ttl)
	v := cache.Get("n").(*int)
	if *v != int('n') {
		t.Errorf("value mismatch: got %d, want %d", *v, int('n'))
	}
	if fetchCount != 1 {
		t.Errorf("fetch count mismatch: got %d want 1", fetchCount)
	}

	// The cache update is async.
	time.Sleep(time.Duration(ttl / 10))

	w := cache.Get("n")
	if v != w {
		t.Errorf("Huh, inconsistent: 1st = %v != 2nd = %v", v, w)
	}

	if fetchCount > 1 {
		t.Errorf("fetch count fail: %d > 1", fetchCount)
	}

	time.Sleep(time.Duration(ttl * 2))
	cache.Purge()

	w = cache.Get("n")
	if fetchCount == 1 {
		t.Error("Did not fetch again. Purge unsuccessful?")
	}
}
