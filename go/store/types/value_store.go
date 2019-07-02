// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"sync"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/constants"
	"github.com/liquidata-inc/ld/dolt/go/store/d"
	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/liquidata-inc/ld/dolt/go/store/util/sizecache"
)

// ValueReader is an interface that knows how to read Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value reading.
type ValueReader interface {
	Format() *Format
	ReadValue(ctx context.Context, h hash.Hash) Value
	ReadManyValues(ctx context.Context, hashes hash.HashSlice) ValueSlice
}

// ValueWriter is an interface that knows how to write Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value writing.
type ValueWriter interface {
	WriteValue(ctx context.Context, v Value) Ref
}

// ValueReadWriter is an interface that knows how to read and write Noms
// Values, e.g. datas/Database. Required to avoid import cycle between this
// package and the package that implements Value read/writing.
type ValueReadWriter interface {
	ValueReader
	ValueWriter
}

// ValueStore provides methods to read and write Noms Values to a ChunkStore.
// It minimally validates Values as they're written, but does not guarantee
// that these Values are persisted through the ChunkStore until a subsequent
// Flush.
// Currently, WriteValue validates the following properties of a Value v:
// - v can be correctly serialized and its Ref taken
type ValueStore struct {
	cs                   chunks.ChunkStore
	bufferMu             sync.RWMutex
	bufferedChunks       map[hash.Hash]chunks.Chunk
	bufferedChunksMax    uint64
	bufferedChunkSize    uint64
	withBufferedChildren map[hash.Hash]uint64 // chunk Hash -> ref height
	unresolvedRefs       hash.HashSet
	enforceCompleteness  bool
	decodedChunks        *sizecache.SizeCache
	format               *Format

	versOnce sync.Once
}

func PanicIfDangling(ctx context.Context, unresolved hash.HashSet, cs chunks.ChunkStore) {
	absent, err := cs.HasMany(ctx, unresolved)

	// TODO: fix panics
	d.PanicIfError(err)

	if len(absent) != 0 {
		d.Panic("Found dangling references to %v", absent)
	}
}

const (
	defaultDecodedChunksSize = 1 << 25 // 32MB
	defaultPendingPutMax     = 1 << 28 // 256MB
)

// newTestValueStore creates a simple struct that satisfies ValueReadWriter
// and is backed by a chunks.TestStore.
func newTestValueStore() *ValueStore {
	ts := &chunks.TestStorage{}
	return NewValueStore(ts.NewView())
}

// NewValueStore returns a ValueStore instance that owns the provided
// ChunkStore and manages its lifetime. Calling Close on the returned
// ValueStore will Close() cs.
func NewValueStore(cs chunks.ChunkStore) *ValueStore {
	return newValueStoreWithCacheAndPending(cs, defaultDecodedChunksSize, defaultPendingPutMax)
}

func newValueStoreWithCacheAndPending(cs chunks.ChunkStore, cacheSize, pendingMax uint64) *ValueStore {
	return &ValueStore{
		cs: cs,

		bufferMu:             sync.RWMutex{},
		bufferedChunks:       map[hash.Hash]chunks.Chunk{},
		bufferedChunksMax:    pendingMax,
		withBufferedChildren: map[hash.Hash]uint64{},
		decodedChunks:        sizecache.New(cacheSize),
		unresolvedRefs:       hash.HashSet{},
		enforceCompleteness:  true,
		versOnce:             sync.Once{},
	}
}

func (lvs *ValueStore) expectVersion() {
	dataVersion := lvs.cs.Version()
	if constants.NomsVersion != dataVersion {
		d.Panic("SDK version %s incompatible with data of version %s", constants.NomsVersion, dataVersion)
	}
	lvs.format = getFormatForVersionString(dataVersion)
}

func (lvs *ValueStore) SetEnforceCompleteness(enforce bool) {
	lvs.enforceCompleteness = enforce
}

func (lvs *ValueStore) ChunkStore() chunks.ChunkStore {
	return lvs.cs
}

func (lvs *ValueStore) Format() *Format {
	lvs.versOnce.Do(lvs.expectVersion)
	return lvs.format
}

// ReadValue reads and decodes a value from lvs. It is not considered an error
// for the requested chunk to be empty; in this case, the function simply
// returns nil.
func (lvs *ValueStore) ReadValue(ctx context.Context, h hash.Hash) Value {
	lvs.versOnce.Do(lvs.expectVersion)
	if v, ok := lvs.decodedChunks.Get(h); ok {
		d.PanicIfTrue(v == nil)
		return v.(Value)
	}

	chunk := func() chunks.Chunk {
		lvs.bufferMu.RLock()
		defer lvs.bufferMu.RUnlock()
		if pending, ok := lvs.bufferedChunks[h]; ok {
			return pending
		}
		return chunks.EmptyChunk
	}()

	if chunk.IsEmpty() {
		var err error
		chunk, err = lvs.cs.Get(ctx, h)

		// TODO: fix panics
		d.PanicIfError(err)
	}
	if chunk.IsEmpty() {
		return nil
	}

	v := DecodeValue(chunk, lvs)
	d.PanicIfTrue(v == nil)
	lvs.decodedChunks.Add(h, uint64(len(chunk.Data())), v)
	return v
}

// ReadManyValues reads and decodes Values indicated by |hashes| from lvs and
// returns the found Values in the same order. Any non-present Values will be
// represented by nil.
func (lvs *ValueStore) ReadManyValues(ctx context.Context, hashes hash.HashSlice) ValueSlice {
	lvs.versOnce.Do(lvs.expectVersion)
	decode := func(h hash.Hash, chunk *chunks.Chunk) Value {
		v := DecodeValue(*chunk, lvs)
		d.PanicIfTrue(v == nil)
		lvs.decodedChunks.Add(h, uint64(len(chunk.Data())), v)
		return v
	}

	foundValues := make(map[hash.Hash]Value, len(hashes))

	// First, see which hashes can be found in either the Value cache or bufferedChunks.
	// Put the rest into a new HashSet to be requested en masse from the ChunkStore.
	remaining := hash.HashSet{}
	for _, h := range hashes {
		if v, ok := lvs.decodedChunks.Get(h); ok {
			d.PanicIfTrue(v == nil)
			foundValues[h] = v.(Value)
			continue
		}

		chunk := func() chunks.Chunk {
			lvs.bufferMu.RLock()
			defer lvs.bufferMu.RUnlock()
			if pending, ok := lvs.bufferedChunks[h]; ok {
				return pending
			}
			return chunks.EmptyChunk
		}()
		if !chunk.IsEmpty() {
			foundValues[h] = decode(h, &chunk)
			continue
		}

		remaining.Insert(h)
	}

	if len(remaining) != 0 {
		// Request remaining hashes from ChunkStore, processing the found chunks as they come in.
		foundChunks := make(chan *chunks.Chunk, 16)

		go func() {
			err := lvs.cs.GetMany(ctx, remaining, foundChunks)

			// TODO: fix panics
			d.PanicIfError(err)

			close(foundChunks)
		}()
		for c := range foundChunks {
			h := c.Hash()
			foundValues[h] = decode(h, c)
		}
	}

	rv := make(ValueSlice, len(hashes))
	for i, h := range hashes {
		rv[i] = foundValues[h]
	}
	return rv
}

// WriteValue takes a Value, schedules it to be written it to lvs, and returns
// an appropriately-typed types.Ref. v is not guaranteed to be actually
// written until after Flush().
func (lvs *ValueStore) WriteValue(ctx context.Context, v Value) Ref {
	lvs.versOnce.Do(lvs.expectVersion)
	d.PanicIfFalse(v != nil)

	c := EncodeValue(v, lvs.format)
	d.PanicIfTrue(c.IsEmpty())
	h := c.Hash()
	height := maxChunkHeight(lvs.format, v) + 1
	r := constructRef(lvs.format, h, TypeOf(v), height)
	lvs.bufferChunk(ctx, v, c, height)
	return r
}

// bufferChunk enqueues c (which is the serialization of v) within this
// ValueStore. Buffered chunks are flushed progressively to the underlying
// ChunkStore in a way which attempts to locate children and grandchildren
// sequentially together. The following invariants are retained:
//
// 1. For any given chunk currently in the buffer, only direct children of the
//    chunk may also be presently buffered (any grandchildren will have been
//    flushed).
// 2. The total data occupied by buffered chunks does not exceed
//    lvs.bufferedChunksMax
func (lvs *ValueStore) bufferChunk(ctx context.Context, v Value, c chunks.Chunk, height uint64) {
	lvs.bufferMu.Lock()
	defer lvs.bufferMu.Unlock()

	d.PanicIfTrue(height == 0)
	h := c.Hash()
	if _, present := lvs.bufferedChunks[h]; !present {
		lvs.bufferedChunks[h] = c
		lvs.bufferedChunkSize += uint64(len(c.Data()))
	}

	put := func(h hash.Hash, c chunks.Chunk) error {
		err := lvs.cs.Put(ctx, c)

		if err != nil {
			return err
		}

		lvs.bufferedChunkSize -= uint64(len(c.Data()))
		delete(lvs.bufferedChunks, h)

		return nil
	}

	putChildren := func(parent hash.Hash) error {
		pending, isBuffered := lvs.bufferedChunks[parent]
		if !isBuffered {
			return nil
		}

		var err error
		WalkRefs(pending, lvs.format, func(grandchildRef Ref) {
			if err != nil {
				// as soon as an error occurs ignore the rest of the refs
				return
			}

			gch := grandchildRef.TargetHash()
			if pending, present := lvs.bufferedChunks[gch]; present {
				err = put(gch, pending)
			}
		})

		if err != nil {
			return err
		}

		delete(lvs.withBufferedChildren, parent)

		return nil
	}

	// Enforce invariant (1)
	if height > 1 {
		var err error
		v.WalkRefs(lvs.format, func(childRef Ref) {
			if err != nil {
				// as soon as an error occurs ignore the rest of the refs
				return
			}

			childHash := childRef.TargetHash()
			if _, isBuffered := lvs.bufferedChunks[childHash]; isBuffered {
				lvs.withBufferedChildren[h] = height
			} else if lvs.enforceCompleteness {
				// If the childRef isn't presently buffered, we must consider it an
				// unresolved ref.
				lvs.unresolvedRefs.Insert(childHash)
			}

			if _, hasBufferedChildren := lvs.withBufferedChildren[childHash]; hasBufferedChildren {
				err = putChildren(childHash)
			}
		})

		// TODO: fix panics
		d.PanicIfError(err)
	}

	// Enforce invariant (2)
	for lvs.bufferedChunkSize > lvs.bufferedChunksMax {
		var tallest hash.Hash
		var height uint64 = 0
		for parent, ht := range lvs.withBufferedChildren {
			if ht > height {
				tallest = parent
				height = ht
			}
		}
		if height == 0 { // This can happen if there are no pending parents
			var chunk chunks.Chunk
			for tallest, chunk = range lvs.bufferedChunks {
				// Any pendingPut is as good as another in this case, so take the first one
				break
			}

			err := put(tallest, chunk)

			// TODO: fix panics
			d.PanicIfError(err)

			continue
		}

		err := putChildren(tallest)

		// TODO: fix panics
		d.PanicIfError(err)
	}
}

func (lvs *ValueStore) Root(ctx context.Context) hash.Hash {
	root, err := lvs.cs.Root(ctx)

	// TODO: fix panics
	d.PanicIfError(err)

	return root
}

func (lvs *ValueStore) Rebase(ctx context.Context) {
	err := lvs.cs.Rebase(ctx)

	//TODO: fix panics
	d.PanicIfError(err)
}

// Commit() flushes all bufferedChunks into the ChunkStore, with best-effort
// locality, and attempts to Commit, updating the root to |current| (or keeping
// it the same as Root()). If the root has moved since this ValueStore was
// opened, or last Rebased(), it will return false and will have internally
// rebased. Until Commit() succeeds, no work of the ValueStore will be visible
// to other readers of the underlying ChunkStore.
func (lvs *ValueStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	return func() (bool, error) {
		lvs.bufferMu.Lock()
		defer lvs.bufferMu.Unlock()

		put := func(h hash.Hash, chunk chunks.Chunk) error {
			err := lvs.cs.Put(ctx, chunk)

			if err != nil {
				return err
			}

			delete(lvs.bufferedChunks, h)
			lvs.bufferedChunkSize -= uint64(len(chunk.Data()))
			return nil
		}

		for parent := range lvs.withBufferedChildren {
			if pending, present := lvs.bufferedChunks[parent]; present {
				var err error
				WalkRefs(pending, lvs.format, func(reachable Ref) {
					if err != nil {
						// as soon as an error occurs ignore the rest of the refs
						return
					}

					if pending, present := lvs.bufferedChunks[reachable.TargetHash()]; present {
						err = put(reachable.TargetHash(), pending)
					}
				})

				if err != nil {
					return false, err
				}

				err = put(parent, pending)

				if err != nil {
					return false, err
				}
			}
		}
		for _, c := range lvs.bufferedChunks {
			// Can't use put() because it's wrong to delete from a lvs.bufferedChunks while iterating it.
			err := lvs.cs.Put(ctx, c)

			if err != nil {
				return false, err
			}

			lvs.bufferedChunkSize -= uint64(len(c.Data()))
		}

		d.PanicIfFalse(lvs.bufferedChunkSize == 0)
		lvs.withBufferedChildren = map[hash.Hash]uint64{}
		lvs.bufferedChunks = map[hash.Hash]chunks.Chunk{}

		if lvs.enforceCompleteness {
			if (current != hash.Hash{} && current != lvs.Root(ctx)) {
				if _, ok := lvs.bufferedChunks[current]; !ok {
					// If the client is attempting to move the root and the referenced
					// value isn't still buffered, we need to ensure that it is contained
					// in the ChunkStore.
					lvs.unresolvedRefs.Insert(current)
				}
			}

			PanicIfDangling(ctx, lvs.unresolvedRefs, lvs.cs)
		}

		success, err := lvs.cs.Commit(ctx, current, last)

		if err != nil {
			return false, err
		}

		if !success {
			return false, nil
		}

		if lvs.enforceCompleteness {
			lvs.unresolvedRefs = hash.HashSet{}
		}

		return true, nil
	}()
}

// Close closes the underlying ChunkStore
func (lvs *ValueStore) Close() error {
	return lvs.cs.Close()
}
