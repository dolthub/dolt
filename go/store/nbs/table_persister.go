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

package nbs

import (
	"bytes"
	"context"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/dolthub/dolt/go/store/util/sizecache"
)

// tablePersister allows interaction with persistent storage. It provides
// primitives for pushing the contents of a memTable to persistent storage,
// opening persistent tables for reading, and conjoining a number of existing
// chunkSources into one. A tablePersister implementation must be goroutine-
// safe.
type tablePersister interface {
	// Persist makes the contents of mt durable. Chunks already present in
	// |haver| may be dropped in the process.
	Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error)

	// ConjoinAll conjoins all chunks in |sources| into a single, new
	// chunkSource.
	ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error)

	// Open a table named |name|, containing |chunkCount| chunks.
	Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error)

	// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
	PruneTableFiles(ctx context.Context, contents manifestContents) error
}

// indexCache provides sized storage for table indices. While getting and/or
// setting the cache entry for a given table name, the caller MUST hold the
// lock that for that entry.
type indexCache struct {
	cache  *sizecache.SizeCache
	cond   *sync.Cond
	locked map[addr]struct{}
}

// Returns an indexCache which will burn roughly |size| bytes of memory.
func newIndexCache(size uint64) *indexCache {
	return &indexCache{sizecache.New(size), sync.NewCond(&sync.Mutex{}), map[addr]struct{}{}}
}

// Take an exclusive lock on the cache entry for |name|. Callers must do this
// before calling get(addr) or put(addr, index)
func (sic *indexCache) lockEntry(name addr) {
	sic.cond.L.Lock()
	defer sic.cond.L.Unlock()

	for {
		if _, present := sic.locked[name]; !present {
			sic.locked[name] = struct{}{}
			break
		}
		sic.cond.Wait()
	}
}

func (sic *indexCache) unlockEntry(name addr) error {
	sic.cond.L.Lock()
	defer sic.cond.L.Unlock()

	_, ok := sic.locked[name]

	if !ok {
		return fmt.Errorf("failed to unlock %s", name.String())
	}

	delete(sic.locked, name)

	sic.cond.Broadcast()

	return nil
}

func (sic *indexCache) get(name addr) (onHeapTableIndex, bool) {
	if idx, found := sic.cache.Get(name); found {
		return idx.(onHeapTableIndex), true
	}
	return onHeapTableIndex{}, false
}

func (sic *indexCache) put(name addr, idx onHeapTableIndex) {
	indexSize := uint64(idx.chunkCount) * (addrSize + ordinalSize + lengthSize + uint64Size)
	sic.cache.Add(name, indexSize, idx)
}

type chunkSourcesByAscendingCount struct {
	sources chunkSources
	err     error
}

func (csbc chunkSourcesByAscendingCount) Len() int { return len(csbc.sources) }
func (csbc chunkSourcesByAscendingCount) Less(i, j int) bool {
	srcI, srcJ := csbc.sources[i], csbc.sources[j]
	cntI, err := srcI.count()

	if err != nil {
		csbc.err = err
		return false
	}

	cntJ, err := srcJ.count()

	if err != nil {
		csbc.err = err
		return false
	}

	if cntI == cntJ {
		hi, err := srcI.hash()

		if err != nil {
			csbc.err = err
			return false
		}

		hj, err := srcJ.hash()

		if err != nil {
			csbc.err = err
			return false
		}

		return bytes.Compare(hi[:], hj[:]) < 0
	}

	return cntI < cntJ
}

func (csbc chunkSourcesByAscendingCount) Swap(i, j int) {
	csbc.sources[i], csbc.sources[j] = csbc.sources[j], csbc.sources[i]
}

type chunkSourcesByDescendingDataSize struct {
	sws []sourceWithSize
	err error
}

func newChunkSourcesByDescendingDataSize(sws []sourceWithSize) chunkSourcesByDescendingDataSize {
	return chunkSourcesByDescendingDataSize{sws, nil}
}

func (csbds chunkSourcesByDescendingDataSize) Len() int { return len(csbds.sws) }
func (csbds chunkSourcesByDescendingDataSize) Less(i, j int) bool {
	swsI, swsJ := csbds.sws[i], csbds.sws[j]
	if swsI.dataLen == swsJ.dataLen {
		hi, err := swsI.source.hash()

		if err != nil {
			csbds.err = err
			return false
		}

		hj, err := swsJ.source.hash()

		if err != nil {
			csbds.err = err
			return false
		}

		return bytes.Compare(hi[:], hj[:]) < 0
	}
	return swsI.dataLen > swsJ.dataLen
}
func (csbds chunkSourcesByDescendingDataSize) Swap(i, j int) {
	csbds.sws[i], csbds.sws[j] = csbds.sws[j], csbds.sws[i]
}

type sourceWithSize struct {
	source  chunkSource
	dataLen uint64
}

type compactionPlan struct {
	sources             chunkSourcesByDescendingDataSize
	mergedIndex         []byte
	chunkCount          uint32
	totalCompressedData uint64
}

func (cp compactionPlan) suffixes() []byte {
	suffixesStart := uint64(cp.chunkCount) * (prefixTupleSize + lengthSize)
	return cp.mergedIndex[suffixesStart : suffixesStart+uint64(cp.chunkCount)*addrSuffixSize]
}

func planConjoin(sources chunkSources, stats *Stats) (plan compactionPlan, err error) {
	var totalUncompressedData uint64
	for _, src := range sources {
		var uncmp uint64
		uncmp, err = src.uncompressedLen()

		if err != nil {
			return compactionPlan{}, err
		}

		totalUncompressedData += uncmp
		index, err := src.index()

		if err != nil {
			return compactionPlan{}, err
		}

		plan.chunkCount += index.ChunkCount()

		// Calculate the amount of chunk data in |src|
		chunkDataLen := calcChunkDataLen(index)
		plan.sources.sws = append(plan.sources.sws, sourceWithSize{src, chunkDataLen})
		plan.totalCompressedData += chunkDataLen
	}
	sort.Sort(plan.sources)

	if plan.sources.err != nil {
		return compactionPlan{}, plan.sources.err
	}

	lengthsPos := lengthsOffset(plan.chunkCount)
	suffixesPos := suffixesOffset(plan.chunkCount)
	plan.mergedIndex = make([]byte, indexSize(plan.chunkCount)+footerSize)

	prefixIndexRecs := make(prefixIndexSlice, 0, plan.chunkCount)
	var ordinalOffset uint32
	for _, sws := range plan.sources.sws {
		var index tableIndex
		index, err = sws.source.index()

		if err != nil {
			return compactionPlan{}, err
		}

		ordinals, err := index.Ordinals()
		if err != nil {
			return compactionPlan{}, err
		}
		prefixes, err := index.Prefixes()
		if err != nil {
			return compactionPlan{}, err
		}

		// Add all the prefix tuples from this index to the list of all prefixIndexRecs, modifying the ordinals such that all entries from the 1st item in sources come after those in the 0th and so on.
		for j, prefix := range prefixes {
			rec := prefixIndexRec{prefix: prefix, order: ordinalOffset + ordinals[j]}
			prefixIndexRecs = append(prefixIndexRecs, rec)
		}

		var cnt uint32
		cnt, err = sws.source.count()

		if err != nil {
			return compactionPlan{}, err
		}

		ordinalOffset += cnt

		if onHeap, ok := index.(onHeapTableIndex); ok {
			// TODO: copy the lengths and suffixes as a byte-copy from src BUG #3438
			// Bring over the lengths block, in order
			for ord := uint32(0); ord < onHeap.chunkCount; ord++ {
				e := onHeap.getIndexEntry(ord)
				binary.BigEndian.PutUint32(plan.mergedIndex[lengthsPos:], e.Length())
				lengthsPos += lengthSize
			}

			// Bring over the suffixes block, in order
			n := copy(plan.mergedIndex[suffixesPos:], onHeap.suffixB)

			if n != len(onHeap.suffixB) {
				return compactionPlan{}, errors.New("failed to copy all data")
			}

			suffixesPos += uint64(n)
		} else {
			// Build up the index one entry at a time.
			var a addr
			for i := 0; i < len(ordinals); i++ {
				e, err := index.IndexEntry(uint32(i), &a)
				if err != nil {
					return compactionPlan{}, err
				}
				li := lengthsPos + lengthSize*uint64(ordinals[i])
				si := suffixesPos + addrSuffixSize*uint64(ordinals[i])
				binary.BigEndian.PutUint32(plan.mergedIndex[li:], e.Length())
				copy(plan.mergedIndex[si:], a[addrPrefixSize:])
			}
			lengthsPos += lengthSize * uint64(len(ordinals))
			suffixesPos += addrSuffixSize * uint64(len(ordinals))
		}
	}

	// Sort all prefixTuples by hash and then insert them starting at the beginning of plan.mergedIndex
	sort.Sort(prefixIndexRecs)
	var pfxPos uint64
	for _, pi := range prefixIndexRecs {
		binary.BigEndian.PutUint64(plan.mergedIndex[pfxPos:], pi.prefix)
		pfxPos += addrPrefixSize
		binary.BigEndian.PutUint32(plan.mergedIndex[pfxPos:], pi.order)
		pfxPos += ordinalSize
	}

	writeFooter(plan.mergedIndex[uint64(len(plan.mergedIndex))-footerSize:], plan.chunkCount, totalUncompressedData)

	stats.BytesPerConjoin.Sample(uint64(plan.totalCompressedData) + uint64(len(plan.mergedIndex)))
	return plan, nil
}

func nameFromSuffixes(suffixes []byte) (name addr) {
	sha := sha512.New()
	sha.Write(suffixes)

	var h []byte
	h = sha.Sum(h) // Appends hash to h
	copy(name[:], h)
	return
}

func calcChunkDataLen(index tableIndex) uint64 {
	return index.TableFileSize() - indexSize(index.ChunkCount()) - footerSize
}
