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
	"sync"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/sizecache"
)

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
	PurgeCaches()
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
	versOnce            sync.Once
	skipWriteCaching    bool

	gcMu       sync.Mutex
	gcCond     *sync.Cond
	gcState    gcState
	gcOut      int
	gcNewAddrs hash.HashSet
}

type gcState int

const (
	gcState_NoGC   gcState = 0
	gcState_NewGen         = iota
	gcState_OldGen
	gcState_Finalizing
)

func AddrsFromNomsValue(c chunks.Chunk, nbf *NomsBinFormat, addrs hash.HashSet) (err error) {
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

func (lvs *ValueStore) getAddrs(c chunks.Chunk) chunks.GetAddrsCb {
	return func(ctx context.Context, addrs hash.HashSet, _ chunks.PendingRefExists) error {
		return AddrsFromNomsValue(c, lvs.nbf, addrs)
	}
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
		gcNewAddrs:    make(hash.HashSet),
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
	if chunk.IsGhost() {
		return GhostValue{hash: chunk.Hash()}, nil
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
		if chunk.IsGhost() {
			return GhostValue{hash: chunk.Hash()}, nil
		}

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
		} else {
			remaining.Insert(h)
		}
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

	finalize, err := lvs.waitForNotFinalizingGC(ctx)
	if err != nil {
		return Ref{}, err
	}
	defer finalize()

	err = lvs.cs.Put(ctx, c, lvs.getAddrs)
	if err != nil {
		return Ref{}, err
	}

	if !lvs.skipWriteCaching {
		lvs.decodedChunks.Add(c.Hash(), uint64(c.Size()), v)
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

// Call with lvs.gcMu locked. Blocks until gcState == gcState_NoGC,
// releasing the lock while blocked.
//
// Returns with the lock held and gcState == gcState_NoGC.
func (lvs *ValueStore) waitForNoGC() {
	for lvs.gcState != gcState_NoGC {
		lvs.gcCond.Wait()
	}
}

// Call without lvs.gcMu locked.
//
// Will block the caller until gcState != gcState_Finalizing.
//
// When this function returns, ValueStore is guaranteed to be in a state such
// that lvs.gcAddChunk will not return ErrAddChunkMustBlock. This function
// returns a finalizer which must be called. gcAddChunk will not return
// MustBlock until after the finalizer which this function returns is called.
//
// The critical sections delimited by `waitForNotFinalizingGC` and its return
// value should fully enclose any critical section which takes `bufferMu`.
//
// These sections are coping with the fact that no call into NomsBlockStore
// while we hold `lvs.bufferMu` is allowed to see `ErrAddChunkMustBlock` or we
// will block with the lock held. While the lock is held, reads cannot
// progress, and the GC process will not complete.

func (lvs *ValueStore) waitForNotFinalizingGC(ctx context.Context) (func(), error) {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			lvs.gcCond.Broadcast()
		case <-stop:
		}
	}()
	for lvs.gcState == gcState_Finalizing && ctx.Err() == nil {
		lvs.gcCond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lvs.gcOut += 1
	lvs.gcCond.Broadcast()
	return func() {
		lvs.gcMu.Lock()
		defer lvs.gcMu.Unlock()
		lvs.gcOut -= 1
		lvs.gcCond.Broadcast()
	}, nil
}

// Call without lvs.gcMu held. Puts the ValueStore into its initial GC state,
// where it is collecting OldGen. gcAddChunk begins accumulating new gen addrs.
func (lvs *ValueStore) transitionToOldGenGC() {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	lvs.waitForNoGC()
	lvs.gcState = gcState_OldGen
	lvs.gcCond.Broadcast()
}

// Call without lvs.gcMu held. Puts the ValueStore into the second in-progress
// GC state, where we are copying newgen to newgen. Returns all the novel
// addresses which were collected by lvs.gcAddChunk while we were collecting
// into the oldgen.
func (lvs *ValueStore) transitionToNewGenGC() hash.HashSet {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	if lvs.gcState != gcState_OldGen {
		panic("attempt to transition to NewGenGC from state != OldGenGC.")
	}
	lvs.gcState = gcState_NewGen
	ret := lvs.gcNewAddrs
	lvs.gcNewAddrs = make(hash.HashSet)
	lvs.gcCond.Broadcast()
	return ret
}

// Call without lvs.gcMu held. Puts the ValueStore into the third and final
// in-progress GC state, where we are finalizing the newgen GC. Returns all the
// novel addresses which were collected by lvs.gcAddChunk. This function will
// block until all inprogress `waitForNotFinalizingGC()` critical sections are
// complete, and will take the accumulated addresses then.
//
// The attempt to start new critical sections will block because the gcState is
// already Finalizing, but existing critical sections run to completion and
// count down gcOut.
func (lvs *ValueStore) transitionToFinalizingGC() hash.HashSet {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	if lvs.gcState != gcState_NewGen {
		panic("attempt to transition to FinalizingGC from state != NewGenGC.")
	}
	lvs.gcState = gcState_Finalizing
	for lvs.gcOut != 0 {
		lvs.gcCond.Wait()
	}
	ret := lvs.gcNewAddrs
	lvs.gcNewAddrs = make(hash.HashSet)
	lvs.gcCond.Broadcast()
	return ret
}

// Call without lvs.gcMu held. Transitions the ValueStore to the quiescent
// state where we are not running a GC. This is a valid transition from any
// gcState in the case of an error.
//
// gcOut is not reset here, because it is maintained by paired up increments
// and decrements which do not have to do with the gcState per se.
func (lvs *ValueStore) transitionToNoGC() {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	if len(lvs.gcNewAddrs) > 0 {
		// In the case of an error during a GC, we transition to NoGC
		// and it's expected to drop these addresses.
		lvs.gcNewAddrs = make(hash.HashSet)
	}
	lvs.gcState = gcState_NoGC
	lvs.gcCond.Broadcast()
}

func (lvs *ValueStore) gcAddChunk(h hash.Hash) bool {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	if lvs.gcState == gcState_NoGC {
		panic("ValueStore gcAddChunk called while no GC is ongoing")
	}
	if lvs.gcState == gcState_Finalizing && lvs.gcOut == 0 {
		return true
	}
	lvs.gcNewAddrs.Insert(h)
	return false
}

func (lvs *ValueStore) readAndResetNewGenToVisit() hash.HashSet {
	lvs.gcMu.Lock()
	defer lvs.gcMu.Unlock()
	if lvs.gcState == gcState_NewGen {
		ret := lvs.gcNewAddrs
		lvs.gcNewAddrs = make(hash.HashSet)
		return ret
	}
	return make(hash.HashSet)
}

// Commit attempts to Commit, updating the root to |current| (or
// keeping it the same as Root()). If the root has moved since this
// ValueStore was opened, or last Rebased(), it will return false and
// will have internally rebased. Until Commit() succeeds, no work of
// the ValueStore will be visible to other readers of the underlying
// ChunkStore.
func (lvs *ValueStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
	c, err := lvs.waitForNotFinalizingGC(ctx)
	if err != nil {
		return false, err
	}
	defer c()

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

type GCMode int

const (
	GCModeDefault GCMode = iota
	GCModeFull
)

type GCSafepointController interface {
	BeginGC(ctx context.Context, keeper func(h hash.Hash) bool) error
	EstablishPreFinalizeSafepoint(context.Context) error
	EstablishPostFinalizeSafepoint(context.Context) error
	CancelSafepoint()
}

// GC traverses the ValueStore from the root and removes unreferenced chunks from the ChunkStore
func (lvs *ValueStore) GC(ctx context.Context, mode GCMode, oldGenRefs, newGenRefs hash.HashSet, safepoint GCSafepointController) error {
	lvs.versOnce.Do(lvs.expectVersion)

	lvs.transitionToOldGenGC()
	defer lvs.transitionToNoGC()

	gcs, gcsOK := lvs.cs.(chunks.GenerationalCS)
	collector, collectorOK := lvs.cs.(chunks.ChunkStoreGarbageCollector)

	var chksMode chunks.GCMode

	if gcsOK && collectorOK {
		oldGen := gcs.OldGen()
		newGen := gcs.NewGen()

		var oldGenHasMany chunks.HasManyFunc
		switch mode {
		case GCModeDefault:
			oldGenHasMany = gcs.OldGenGCFilter()
			chksMode = chunks.GCMode_Default
		case GCModeFull:
			oldGenHasMany = unfilteredHashFunc
			chksMode = chunks.GCMode_Full
		default:
			return fmt.Errorf("unsupported GCMode %v", mode)
		}

		err := func() error {
			err := collector.BeginGC(lvs.gcAddChunk, chksMode)
			if err != nil {
				return err
			}
			defer collector.EndGC(chksMode)

			var callCancelSafepoint bool
			if safepoint != nil {
				err = safepoint.BeginGC(ctx, lvs.gcAddChunk)
				if err != nil {
					return err
				}
				callCancelSafepoint = true
				defer func() {
					if callCancelSafepoint {
						safepoint.CancelSafepoint()
					}
				}()
			}

			root, err := lvs.Root(ctx)
			if err != nil {
				return err
			}

			if root.IsEmpty() {
				// empty root
				return nil
			}

			newGenRefs.Insert(root)

			var oldGenFinalizer, newGenFinalizer chunks.GCFinalizer
			oldGenFinalizer, err = lvs.gc(ctx, oldGenRefs, oldGenHasMany, chksMode, collector, oldGen, nil, func() hash.HashSet {
				n := lvs.transitionToNewGenGC()
				newGenRefs.InsertAll(n)
				return make(hash.HashSet)
			})
			if err != nil {
				return err
			}

			var newFileHasMany chunks.HasManyFunc
			newFileHasMany, err = oldGenFinalizer.AddChunksToStore(ctx)
			if err != nil {
				return err
			}

			if mode == GCModeDefault {
				oldGenHasMany = gcs.OldGenGCFilter()
			} else {
				oldGenHasMany = newFileHasMany
			}

			newGenFinalizer, err = lvs.gc(ctx, newGenRefs, oldGenHasMany, chksMode, collector, newGen, safepoint, lvs.transitionToFinalizingGC)
			if err != nil {
				return err
			}
			callCancelSafepoint = false

			err = newGenFinalizer.SwapChunksInStore(ctx)
			if err != nil {
				return err
			}

			if mode == GCModeFull {
				err = oldGenFinalizer.SwapChunksInStore(ctx)
				if err != nil {
					return err
				}
			}

			return nil
		}()

		if err != nil {
			return err
		}
	} else if collectorOK {
		extraNewGenRefs := lvs.transitionToNewGenGC()
		newGenRefs.InsertAll(extraNewGenRefs)
		newGenRefs.InsertAll(oldGenRefs)

		err := func() error {
			err := collector.BeginGC(lvs.gcAddChunk, chunks.GCMode_Full)
			if err != nil {
				return err
			}
			defer collector.EndGC(chunks.GCMode_Full)

			var callCancelSafepoint bool
			if safepoint != nil {
				err = safepoint.BeginGC(ctx, lvs.gcAddChunk)
				if err != nil {
					return err
				}
				callCancelSafepoint = true
				defer func() {
					if callCancelSafepoint {
						safepoint.CancelSafepoint()
					}
				}()
			}

			root, err := lvs.Root(ctx)
			if err != nil {
				return err
			}

			if root == (hash.Hash{}) {
				// empty root
				return nil
			}

			newGenRefs.Insert(root)

			var finalizer chunks.GCFinalizer
			finalizer, err = lvs.gc(ctx, newGenRefs, unfilteredHashFunc, chunks.GCMode_Full, collector, collector, safepoint, lvs.transitionToFinalizingGC)
			if err != nil {
				return err
			}
			callCancelSafepoint = false

			err = finalizer.SwapChunksInStore(ctx)
			if err != nil {
				return err
			}

			return nil
		}()

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

func (lvs *ValueStore) gc(ctx context.Context,
	toVisit hash.HashSet,
	hashFilter chunks.HasManyFunc,
	chksMode chunks.GCMode,
	src, dest chunks.ChunkStoreGarbageCollector,
	safepointController GCSafepointController,
	finalize func() hash.HashSet) (chunks.GCFinalizer, error) {
	sweeper, err := src.MarkAndSweepChunks(ctx, lvs.getAddrs, hashFilter, dest, chksMode)
	if err != nil {
		return nil, err
	}

	err = sweeper.SaveHashes(ctx, toVisit.ToSlice())
	if err != nil {
		cErr := sweeper.Close(ctx)
		return nil, errors.Join(err, cErr)
	}
	toVisit = nil

	if safepointController != nil {
		err = safepointController.EstablishPreFinalizeSafepoint(ctx)
		if err != nil {
			cErr := sweeper.Close(ctx)
			return nil, errors.Join(err, cErr)
		}
	}

	// Before we call finalize(), we can process the current set of
	// NewGenToVisit. NewGen -> Finalize is going to block writes until
	// we are done, so its best to keep it as small as possible.
	next := lvs.readAndResetNewGenToVisit()
	err = sweeper.SaveHashes(ctx, next.ToSlice())
	if err != nil {
		cErr := sweeper.Close(ctx)
		return nil, errors.Join(err, cErr)
	}
	next = nil

	final := finalize()
	err = sweeper.SaveHashes(ctx, final.ToSlice())
	if err != nil {
		cErr := sweeper.Close(ctx)
		return nil, errors.Join(err, cErr)
	}

	if safepointController != nil {
		err = safepointController.EstablishPostFinalizeSafepoint(ctx)
		if err != nil {
			cErr := sweeper.Close(ctx)
			return nil, errors.Join(err, cErr)
		}
	}
	finalizer, err := sweeper.Finalize(ctx)
	if err != nil {
		return nil, err
	}
	return finalizer, sweeper.Close(ctx)
}

func (lvs *ValueStore) PurgeCaches() {
	lvs.decodedChunks.Purge()
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
