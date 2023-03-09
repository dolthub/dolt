// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/sizecache"
)

type HashFilterFunc func(context.Context, hash.HashSet) (hash.HashSet, error)

func unfilteredHashFunc(_ context.Context, hs hash.HashSet) (hash.HashSet, error) {
	return hs, nil
}

// ValueReader is an interface that knows how to read Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value reading.
type ValueReader interface {
	Format() *NomsBinFormat
	ReadValue(ctx context.Context, h hash.Hash) (Value, error)
	ReadManyValues(ctx context.Context, hashes hash.HashSlice) (ValueSlice, error)
}

// ValueWriter is an interface that knows how to write Noms Values, e.g.
// datas/Database. Required to avoid import cycle between this package and the
// package that implements Value writing.
type ValueWriter interface {
	WriteValue(ctx context.Context, v Value) (Ref, error)
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
	cs                  chunks.ChunkStore
	validateContentAddr bool
	decodedChunks       *sizecache.SizeCache
	nbf                 *NomsBinFormat

	gcMu    sync.RWMutex
	gcCond  *sync.Cond
	doingGC bool

	versOnce sync.Once
}

func AddrsFromNomsValue(ctx context.Context, c chunks.Chunk, nbf *NomsBinFormat) (addrs hash.HashSet, err error) {
	addrs = hash.NewHashSet()
	if NomsKind(c.Data()[0]) == SerialMessageKind {
		err = SerialMessage(c.Data()).WalkAddrs(nbf, func(a hash.Hash) error {
			addrs.Insert(a)
			return nil
		})
		return
	}

	err = walkRefs(c.Data(), nbf, func(r Ref) error {
		addrs.Insert(r.TargetHash())
		return nil
	})
	return
}

func (lvs *ValueStore) getAddrs(ctx context.Context, c chunks.Chunk) (hash.HashSet, error) {
	return AddrsFromNomsValue(ctx, c, lvs.nbf)
}

const (
	defaultDecodedChunksSize = 1 << 25 // 32MB
	defaultPendingPutMax     = 1 << 28 // 256MB

	gcBuffSize = 16
)

// newTestValueStore creates a simple struct that satisfies ValueReadWriter
// and is backed by a chunks.TestStore. Used for testing noms.
func newTestValueStore() *ValueStore {
	ts := &chunks.TestStorage{}
	return NewValueStore(ts.NewViewWithDefaultFormat())
}

func newTestValueStore_LD_1() *ValueStore {
	ts := &chunks.TestStorage{}
	return NewValueStore(ts.NewView())
}

// NewMemoryValueStore creates a simple struct that satisfies ValueReadWriter
// and is backed by a chunks.TestStore. Used for dolt operations outside of noms.
func NewMemoryValueStore() *ValueStore {
	ts := &chunks.TestStorage{}
	return NewValueStore(ts.NewViewWithDefaultFormat())
}

// NewValueStore returns a ValueStore instance that owns the provided
// ChunkStore and manages its lifetime. Calling Close on the returned
// ValueStore will Close() cs.
func NewValueStore(cs chunks.ChunkStore) *ValueStore {
	return newValueStoreWithCacheAndPending(cs, defaultDecodedChunksSize, defaultPendingPutMax)
}

func newValueStoreWithCacheAndPending(cs chunks.ChunkStore, cacheSize, pendingMax uint64) *ValueStore {
	vs := &ValueStore{
		cs:            cs,
		decodedChunks: sizecache.New(cacheSize),
		versOnce:      sync.Once{},
	}
	vs.gcCond = sync.NewCond(&vs.gcMu)
	return vs
}

func (lvs *ValueStore) expectVersion() {
	dataVersion := lvs.cs.Version()
	nbf, err := GetFormatForVersionString(dataVersion)
	if err != nil {
		panic(err)
	}
	lvs.nbf = nbf
}

func (lvs *ValueStore) SetValidateContentAddresses(validate bool) {
	lvs.validateContentAddr = validate
}

func (lvs *ValueStore) ChunkStore() chunks.ChunkStore {
	return lvs.cs
}

func (lvs *ValueStore) Format() *NomsBinFormat {
	lvs.versOnce.Do(lvs.expectVersion)
	return lvs.nbf
}

// ReadValue reads and decodes a value from lvs. It is not considered an error
// for the requested chunk to be empty; in this case, the function simply
// returns nil.
func (lvs *ValueStore) ReadValue(ctx context.Context, h hash.Hash) (Value, error) {
	lvs.versOnce.Do(lvs.expectVersion)
	if v, ok := lvs.decodedChunks.Get(h); ok {
		d.PanicIfTrue(v == nil)
		nv := v.(Value)
		return nv, nil
	}

	chunk, err := lvs.cs.Get(ctx, h)
	if err != nil {
		return nil, err
	}
	if chunk.IsEmpty() {
		return nil, nil
	}

	v, err := DecodeValue(chunk, lvs)
	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, errors.New("decoded value is empty")
	}

	if lvs.validateContentAddr {
		if err = validateContentAddress(lvs.nbf, h, v); err != nil {
			return nil, err
		}
	}

	lvs.decodedChunks.Add(h, uint64(len(chunk.Data())), v)
	return v, nil
}

// ReadManyValues reads and decodes Values indicated by |hashes| from lvs and
// returns the found Values in the same order. Any non-present Values will be
// represented by nil.
func (lvs *ValueStore) ReadManyValues(ctx context.Context, hashes hash.HashSlice) (ValueSlice, error) {
	lvs.versOnce.Do(lvs.expectVersion)
	decode := func(h hash.Hash, chunk *chunks.Chunk) (Value, error) {
		v, ferr := DecodeValue(*chunk, lvs)

		if ferr != nil {
			return nil, ferr
		}

		if v == nil {
			return nil, errors.New("decoded value is empty")
		}
		if lvs.validateContentAddr {
			if err := validateContentAddress(lvs.nbf, h, v); err != nil {
				return nil, err
			}
		}

		lvs.decodedChunks.Add(h, uint64(len(chunk.Data())), v)
		return v, nil
	}

	foundValues := make(map[hash.Hash]Value, len(hashes))

	// First, see which hashes can be found in either the Value cache or bufferedChunks.
	// Put the rest into a new HashSet to be requested en masse from the ChunkStore.
	remaining := hash.HashSet{}
	for _, h := range hashes {
		if v, ok := lvs.decodedChunks.Get(h); ok {
			d.PanicIfTrue(v == nil)
			nv := v.(Value)
			foundValues[h] = nv
			continue
		}
		remaining.Insert(h)
	}

	if len(remaining) != 0 {
		mu := new(sync.Mutex)
		var decodeErr error
		err := lvs.cs.GetMany(ctx, remaining, func(ctx context.Context, c *chunks.Chunk) {
			mu.Lock()
			defer mu.Unlock()
			if decodeErr != nil {
				return
			}
			h := c.Hash()
			foundValues[h], decodeErr = decode(h, c)
		})
		if err != nil {
			return nil, err
		}
		if decodeErr != nil {
			return nil, decodeErr
		}
	}

	rv := make(ValueSlice, len(hashes))
	for i, h := range hashes {
		rv[i] = foundValues[h]
	}
	return rv, nil
}

// WriteValue takes a Value, schedules it to be written it to lvs, and returns
// an appropriately-typed types.Ref. v is not guaranteed to be actually
// written until after Flush().
func (lvs *ValueStore) WriteValue(ctx context.Context, v Value) (Ref, error) {
	lvs.versOnce.Do(lvs.expectVersion)
	d.PanicIfFalse(v != nil)

	c, err := EncodeValue(v, lvs.nbf)

	if err != nil {
		return Ref{}, err
	}

	if c.IsEmpty() {
		return Ref{}, errors.New("value encoded to empty chunk")
	}

	h := c.Hash()
	height, err := maxChunkHeight(lvs.nbf, v)

	if err != nil {
		return Ref{}, err
	}

	height++
	t, err := TypeOf(v)

	if err != nil {
		return Ref{}, err
	}

	r, err := constructRef(lvs.nbf, h, t, height)
	if err != nil {
		return Ref{}, err
	}

	err = lvs.cs.Put(ctx, c, lvs.getAddrs)
	if err != nil {
		return Ref{}, err
	}

	return r, nil
}

func (lvs *ValueStore) Root(ctx context.Context) (hash.Hash, error) {
	root, err := lvs.cs.Root(ctx)

	if err != nil {
		return hash.Hash{}, err
	}

	return root, nil
}

func (lvs *ValueStore) Rebase(ctx context.Context) error {
	return lvs.cs.Rebase(ctx)
}

// Call with lvs.bufferMu locked. Blocks until doingGC == false, releasing the
// lock while we are blocked. Returns with the lock held, doingGC == false.
func (lvs *ValueStore) waitForGC() {
	for lvs.doingGC {
		lvs.gcCond.Wait()
	}
}

// Call without lvs.bufferMu held. If val == false, then doingGC must equal
// true when this is called. We will set it to false and return without
// lvs.bufferMu held. If val == true, we will set doingGC to true and return
// with lvs.bufferMu not held.
//
// When val == true, this routine will block until it has a unique opportunity
// to toggle doingGC from false to true while holding the lock.
func (lvs *ValueStore) toggleGC(val bool) {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	if !val {
		if !lvs.doingGC {
			panic("tried to toggleGC to false while it was not true...")
		}
		lvs.doingGC = false
		lvs.gcCond.Broadcast()
	} else {
		lvs.waitForGC()
		lvs.doingGC = true
	}
	return
}

// Commit flushes all bufferedChunks into the ChunkStore, with best-effort
// locality, and attempts to Commit, updating the root to |current| (or keeping
// it the same as Root()). If the root has moved since this ValueStore was
// opened, or last Rebased(), it will return false and will have internally
// rebased. Until Commit() succeeds, no work of the ValueStore will be visible
// to other readers of the underlying ChunkStore.
func (lvs *ValueStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	lvs.waitForGC()

	success, err := lvs.cs.Commit(ctx, current, last)
	if err != nil {
		return false, err
	}
	if !success {
		return false, nil
	}

	return true, nil
}

func makeBatches(hss []hash.HashSet, count int) [][]hash.Hash {
	const maxBatchSize = 16384

	buffer := make([]hash.Hash, count)
	i := 0
	for _, hs := range hss {
		for h := range hs {
			buffer[i] = h
			i++
		}
	}

	numBatches := (count + (maxBatchSize - 1)) / maxBatchSize
	batchSize := count / numBatches

	res := make([][]hash.Hash, numBatches)
	for i := 0; i < numBatches; i++ {
		if i != numBatches-1 {
			res[i] = buffer[i*batchSize : (i+1)*batchSize]
		} else {
			res[i] = buffer[i*batchSize:]
		}
	}

	return res
}

// GC traverses the ValueStore from the root and removes unreferenced chunks from the ChunkStore
func (lvs *ValueStore) GC(ctx context.Context, oldGenRefs, newGenRefs hash.HashSet) error {
	lvs.toggleGC(true)
	defer lvs.toggleGC(false)

	lvs.versOnce.Do(lvs.expectVersion)

	root, err := lvs.Root(ctx)

	if err != nil {
		return err
	}

	rootVal, err := lvs.ReadValue(ctx, root)
	if err != nil {
		return err
	}

	if rootVal == nil {
		// empty root
		return nil
	}

	newGenRefs.Insert(root)
	if gcs, ok := lvs.cs.(chunks.GenerationalCS); ok {
		oldGen := gcs.OldGen()
		newGen := gcs.NewGen()
		err = lvs.gc(ctx, root, oldGenRefs, oldGen.HasMany, newGen, oldGen)
		if err != nil {
			return err
		}

		err = lvs.gc(ctx, root, newGenRefs, oldGen.HasMany, newGen, newGen)
		if err != nil {
			return err
		}
	} else if collector, ok := lvs.cs.(chunks.ChunkStoreGarbageCollector); ok {
		if len(oldGenRefs) > 0 {
			newGenRefs.InsertAll(oldGenRefs)
		}

		err = lvs.gc(ctx, root, newGenRefs, unfilteredHashFunc, collector, collector)
		if err != nil {
			return err
		}
	} else {
		return chunks.ErrUnsupportedOperation
	}

	if tfs, ok := lvs.cs.(chunks.TableFileStore); ok {
		return tfs.PruneTableFiles(ctx)
	}
	return nil
}

func (lvs *ValueStore) gc(ctx context.Context, root hash.Hash, toVisit hash.HashSet, hashFilter HashFilterFunc, src, dest chunks.ChunkStoreGarbageCollector) error {
	keepChunks := make(chan []hash.Hash, gcBuffSize)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return src.MarkAndSweepChunks(ctx, root, keepChunks, dest)
	})

	keepHashes := func(hs []hash.Hash) error {
		select {
		case keepChunks <- hs:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	concurrency := runtime.GOMAXPROCS(0) - 1
	if concurrency < 1 {
		concurrency = 1
	}
	walker := newParallelRefWalker(ctx, lvs.nbf, concurrency)

	eg.Go(func() error {
		defer walker.Close()

		visited := toVisit.Copy()
		err := lvs.gcProcessRefs(ctx, visited, []hash.HashSet{toVisit}, keepHashes, walker, hashFilter)
		if err != nil {
			return err
		}

		// NOTE: We do not defer this close here. When keepChunks
		// closes, it signals to NBSStore.MarkAndSweepChunks that we
		// are done walking the references. If gcProcessRefs returns an
		// error, we did not successfully walk all references and we do
		// not want MarkAndSweepChunks finishing its work, swapping
		// table files, etc. It would be racing with returning an error
		// here. Instead, we have returned the error above and that
		// will force it to fail when the errgroup ctx fails.
		close(keepChunks)
		return nil
	})

	return eg.Wait()
}

func (lvs *ValueStore) gcProcessRefs(ctx context.Context, visited hash.HashSet, toVisit []hash.HashSet, keepHashes func(hs []hash.Hash) error, walker *parallelRefWalker, hashFilter HashFilterFunc) error {
	if len(toVisit) != 1 {
		panic("Must be one initial hashset to visit")
	}

	toVisitCount := len(toVisit[0])
	for toVisitCount > 0 {
		batches := makeBatches(toVisit, toVisitCount)
		toVisit = make([]hash.HashSet, len(batches))
		toVisitCount = 0
		for i, batch := range batches {
			if err := keepHashes(batch); err != nil {
				return err
			}

			vals, err := lvs.ReadManyValues(ctx, batch)
			if err != nil {
				return err
			}
			if len(vals) != len(batch) {
				return errors.New("dangling reference found in chunk store")
			}

			hashes, err := walker.GetRefSet(visited, vals)
			if err != nil {
				return err
			}

			// continue processing
			hashes, err = hashFilter(ctx, hashes)
			if err != nil {
				return err
			}

			toVisit[i] = hashes
			toVisitCount += len(hashes)
		}
	}

	lvs.decodedChunks.Purge()

	return nil
}

// Close closes the underlying ChunkStore
func (lvs *ValueStore) Close() error {
	return lvs.cs.Close()
}

func validateContentAddress(nbf *NomsBinFormat, h hash.Hash, v Value) (err error) {
	var actual hash.Hash
	actual, err = v.Hash(nbf)
	if err != nil {
		return
	} else if actual != h {
		err = fmt.Errorf("incorrect hash for value %s (%s != %s)",
			v.HumanReadableString(), actual.String(), h.String())
	}
	return
}
