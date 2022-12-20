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
	"io"
	"sort"
	"time"
)

var errCacheMiss = errors.New("index cache miss")

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

	// Exists checks if a table named |name| exists.
	Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error)

	// PruneTableFiles deletes old table files that are no longer referenced in the manifest.
	PruneTableFiles(ctx context.Context, contents manifestContents, mtime time.Time) error

	io.Closer
}

type tableFilePersister interface {
	tablePersister

	// Path returns the file system path.
	Path() string
}

type chunkSourcesByDescendingDataSize struct {
	sws []sourceWithSize
}

func (csbds chunkSourcesByDescendingDataSize) Len() int { return len(csbds.sws) }
func (csbds chunkSourcesByDescendingDataSize) Less(i, j int) bool {
	swsI, swsJ := csbds.sws[i], csbds.sws[j]
	if swsI.dataLen == swsJ.dataLen {
		hi := swsI.source.hash()
		hj := swsJ.source.hash()
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

// planRangeCopyConjoin computes a conjoin plan for tablePersisters that can conjoin
// chunkSources using range copies (copy only chunk records, not chunk indexes).
func planRangeCopyConjoin(sources chunkSources, stats *Stats) (compactionPlan, error) {
	var sized []sourceWithSize
	for _, src := range sources {
		index, err := src.index()
		if err != nil {
			return compactionPlan{}, err
		}
		// Calculate the amount of chunk data in |src|
		sized = append(sized, sourceWithSize{src, calcChunkRangeSize(index)})
	}
	return planConjoin(sized, stats)
}

// calcChunkRangeSize computes the size of the chunk records for a table file.
func calcChunkRangeSize(index tableIndex) uint64 {
	return index.tableFileSize() - indexSize(index.chunkCount()) - footerSize
}

func planConjoin(sources []sourceWithSize, stats *Stats) (plan compactionPlan, err error) {
	// place largest chunk sources at the beginning of the conjoin
	plan.sources = chunkSourcesByDescendingDataSize{sws: sources}
	sort.Sort(plan.sources)

	var totalUncompressedData uint64
	for _, s := range sources {
		var uncmp uint64
		if uncmp, err = s.source.uncompressedLen(); err != nil {
			return compactionPlan{}, err
		}
		totalUncompressedData += uncmp

		index, err := s.source.index()
		if err != nil {
			return compactionPlan{}, err
		}
		// Calculate the amount of chunk data in |src|
		plan.totalCompressedData += s.dataLen
		plan.chunkCount += index.chunkCount()
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

		ordinals, err := index.ordinals()
		if err != nil {
			return compactionPlan{}, err
		}
		prefixes, err := index.prefixes()
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
			for ord := uint32(0); ord < onHeap.chunkCount(); ord++ {
				e := onHeap.getIndexEntry(ord)
				binary.BigEndian.PutUint32(plan.mergedIndex[lengthsPos:], e.Length())
				lengthsPos += lengthSize
			}

			// Bring over the suffixes block, in order
			n := copy(plan.mergedIndex[suffixesPos:], onHeap.suffixes)

			if n != len(onHeap.suffixes) {
				return compactionPlan{}, errors.New("failed to copy all data")
			}

			suffixesPos += uint64(n)
		} else {
			// Build up the index one entry at a time.
			var a addr
			for i := 0; i < len(ordinals); i++ {
				e, err := index.indexEntry(uint32(i), &a)
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
