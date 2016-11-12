// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"github.com/attic-labs/noms/go/hash"
)

// sequenceReadAhead implements read-ahead by mapping a hash to a channel returning
// the corresponding sequence.
//
// It reads ahead by firing off a set of short-lived go routines. Each go
// routine (1) reads a sequence, (2) inserts it into a channel, (3) adds the
// channel to the map keyed by sequence hash, and (4) exits.
//
// The caller retrieves the sequence by looking up the channel by hash and
// reading from it.
//
// It maintains parallelism |p| by initially firing off |p| go routines
// to read the next |p| sequences. When a sequence is retreived from the cache,
// It fires off a new go-routine to read the next sequence. This ensures that
// there are always |p| outstanding channels to read from the cache.
//
// This approach has one major advantage over a channel based approach:
// there are no go-routines to shutdown when finished with the cursor. This
// avoids requiring caller to call a Close() method.
type sequenceReadAhead struct {
	cursor      *sequenceCursor
	cache       map[raKey]chan sequence
	parallelism int
	getCount    float32
	hitCount    float32
}

// raKey is the future key. Rather than simply use the hash, we combines it
// with the local chunk offset. This increases the likelihood that repeat values
// in the sequence will get unique entries in the map.
type raKey struct {
	idx  int
	hash hash.Hash
}

func newSequenceReadAhead(cursor *sequenceCursor, parallelism int) *sequenceReadAhead {
	m := map[raKey]chan sequence{}
	return &sequenceReadAhead{cursor.clone(), m, parallelism, 0, 0}
}

func (ra *sequenceReadAhead) get(idx int, h hash.Hash) (sequence, bool) {
	ra.readAhead()
	key := raKey{idx, h}
	ra.getCount += 1
	if future, ok := ra.cache[key]; ok {
		result := <-future
		ra.hitCount += 1
		delete(ra.cache, key)
		return result, true
	}
	return nil, false
}

// readAhead (called when read-ahead is enabled) primes the next entries in the
// read-ahead cache. It ensures that go routines have been allocated for reading
// the next n entries in the current sequence. N is either readAheadParallelism
// or the number of entries left in the sequence if smaller.
func (ra *sequenceReadAhead) readAhead() {
	// the next position to be primed
	count := ra.parallelism - len(ra.cache)
	for i := 0; i < count; i += 1 {
		if !ra.cursor.advance() {
			break
		}
		future := make(chan sequence, 1)
		key := raKey{
			ra.cursor.idx,
			ra.cursor.current().(metaTuple).ref.target,
		}
		ra.cache[key] = future
		seq := ra.cursor.seq
		idx := ra.cursor.idx
		go func() {
			// close not required here but ensures fast fail if channel is misused
			defer close(future)
			val := seq.getChildSequence(idx)
			future <- val

		}()
	}
}

func (rc *sequenceReadAhead) hitRate() float32 {
	return rc.hitCount / rc.getCount
}
