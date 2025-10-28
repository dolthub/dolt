// Copyright 2025 Dolthub, Inc.
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

package nbs

import (
	"fmt"
	"io"
	"math/bits"
	"os"
	"sync/atomic"

	"github.com/dolthub/dolt/go/libraries/utils/file"
	"github.com/dolthub/dolt/go/store/hash"
)

type archiveIndexReader interface {
	getNumChunks() uint32
	getPrefix(idx uint32) uint64
	searchPrefix(uint64) int32
	getSpanIndex(idx uint32) uint64
	getChunkRef(idx uint32) (dict, data uint32)
	getSuffix(idx uint32) suffix
	clone() (archiveIndexReader, error)
	io.Closer
}

// mmapIndexReader lazily loads archive index data from a memory-mapped file.
type mmapIndexReader struct {
	data          *file.MmapData
	indexSize     uint64
	byteSpanCount uint32
	chunkCount    uint32

	// Byte offsets within the mapped region for each section
	spanIndexOffset uint64
	prefixesOffset  uint64
	chunkRefsOffset uint64
	suffixesOffset  uint64

	refCnt atomic.Int32
}

// newMmapIndexReader creates a new memory-mapped index reader.
func newMmapIndexReader(fileHandle *os.File, footer archiveFooter) (*mmapIndexReader, error) {
	// Calculate the total index span
	indexSpan := footer.totalIndexSpan()

	// Calculate section offsets within the mapped region
	spanIndexOffset := uint64(0)
	prefixesOffset := spanIndexOffset + uint64(footer.byteSpanCount)*uint64(uint64Size)
	chunkRefsOffset := prefixesOffset + uint64(footer.chunkCount)*uint64(uint64Size)
	suffixesOffset := chunkRefsOffset + uint64(footer.chunkCount)*2*uint64(uint32Size)

	// Memory map the entire index section
	mappedData, err := file.Mmap(fileHandle, int64(indexSpan.offset), int(indexSpan.length))
	if err != nil {
		return nil, fmt.Errorf("failed to mmap index: %w", err)
	}

	ret := &mmapIndexReader{
		data:            mappedData,
		indexSize:       footer.indexSize,
		byteSpanCount:   footer.byteSpanCount,
		chunkCount:      footer.chunkCount,
		spanIndexOffset: spanIndexOffset,
		prefixesOffset:  prefixesOffset,
		chunkRefsOffset: chunkRefsOffset,
		suffixesOffset:  suffixesOffset,
	}
	ret.refCnt.Add(1)

	return ret, nil
}

func (m *mmapIndexReader) getNumChunks() uint32 {
	return m.chunkCount
}

// getSpanIndex returns the span index value at the given position
func (m *mmapIndexReader) getSpanIndex(idx uint32) uint64 {
	if idx == 0 {
		return 0 // Null span to simplify logic, matching original implementation
	}
	if idx > m.byteSpanCount {
		return 0
	}

	offset := m.spanIndexOffset + uint64(idx-1)*uint64(uint64Size)
	return m.data.GetUint64(offset)
}

// getPrefix returns the prefix value at the given index
func (m *mmapIndexReader) getPrefix(idx uint32) uint64 {
	if idx >= m.chunkCount {
		return 0
	}
	offset := m.prefixesOffset + uint64(idx)*uint64(uint64Size)
	return m.data.GetUint64(offset)
}

func (m *mmapIndexReader) searchPrefix(target uint64) int32 {
	items := int32(m.chunkCount)
	if items == 0 {
		return 0
	}
	lft, rht := int32(0), items
	lo, hi := m.getPrefix(0), m.getPrefix(uint32(rht-1))

	if target > hi {
		return rht
	}
	if lo >= target {
		return lft
	}
	for lft < rht {
		valRangeSz := hi - lo
		idxRangeSz := uint64(rht - lft - 1)
		shiftedTgt := target - lo
		mhi, mlo := bits.Mul64(shiftedTgt, idxRangeSz)
		dU64, _ := bits.Div64(mhi, mlo, valRangeSz)
		idx := int32(dU64) + lft
		if m.getPrefix(uint32(idx)) < target {
			lft = idx + 1
			if lft < items {
				lo = m.getPrefix(uint32(lft))
				if lo >= target {
					return lft
				}
			}
		} else {
			rht = idx
			hi = m.getPrefix(uint32(rht))
		}
	}
	return lft
}

// getChunkRef returns the dictionary and data references for the chunk at the given index
func (m *mmapIndexReader) getChunkRef(idx uint32) (dict, data uint32) {
	if idx < 0 || idx >= m.chunkCount {
		return 0, 0
	}

	// Chunk refs are stored as pairs of uint32s
	offset := m.chunkRefsOffset + uint64(idx)*2*uint64(uint32Size)
	dict = m.data.GetUint32(offset)
	data = m.data.GetUint32(offset + uint32Size)
	return
}

// getSuffix returns the suffix for the chunk at the given index
func (m *mmapIndexReader) getSuffix(idx uint32) (suf suffix) {
	if idx >= m.chunkCount {
		return suffix{}
	}

	start := m.suffixesOffset + uint64(idx)*hash.SuffixLen
	_, _ = m.data.ReadAt(suf[:], int64(start))
	return
}

func (m *mmapIndexReader) clone() (archiveIndexReader, error) {
	m.refCnt.Add(1)
	return m, nil
}

// close unmaps the memory region
func (m *mmapIndexReader) Close() error {
	if m.refCnt.Add(-1) == 0 {
		return m.data.Close()
	}
	return nil
}
