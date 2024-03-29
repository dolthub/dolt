// Copyright 2024 Dolthub, Inc.
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
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func TestArchiveSingleChunk(t *testing.T) {
	writer := NewFixedBufferTableSink(make([]byte, 1024))

	aw := newArchiveWriter(writer)

	bsId, err := aw.writeByteSpan([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	assert.NoError(t, err)
	assert.Equal(t, uint32(1), bsId)
	assert.Equal(t, uint64(14), aw.bytesWritten) // 10 bytes + CRC

	oneHash := hashWithPrefix(t, 23)

	err = aw.stageChunk(oneHash, 0, 1)
	assert.NoError(t, err)

	n, err := aw.writeIndex()
	assert.NoError(t, err)
	assert.Equal(t, uint32(24), n) // NM4 - verify manually before shipping.

	err = aw.writeFooter(24)
	assert.NoError(t, err)

	assert.Equal(t, uint64(58), aw.bytesWritten) // 14 + 24 + 20

	theBytes := writer.buff[:writer.pos]
	fileSize := uint64(len(theBytes))
	readerAt := bytes.NewReader(theBytes)
	aIdx, err := newArchiveIndex(readerAt, fileSize)
	assert.NoError(t, err)

	assert.Equal(t, []uint64{23}, aIdx.prefixes)

	assert.True(t, aIdx.has(oneHash))

}

func TestPrefixSearch(t *testing.T) {
	pf := []uint64{2, 3, 4, 4, 4, 5, 6, 7, 10, 10, 11, 12, 13}

	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 1))
	assert.Equal(t, []int{0}, findMatchingPrefixes(pf, 2))
	assert.Equal(t, []int{2, 3, 4}, findMatchingPrefixes(pf, 4))
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 8))
	assert.Equal(t, []int{8, 9}, findMatchingPrefixes(pf, 10))
	assert.Equal(t, []int{12}, findMatchingPrefixes(pf, 13))
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 14))

	pf = []uint64{}
	assert.Equal(t, []int{}, findMatchingPrefixes(pf, 42))
}

func hashWithPrefix(t *testing.T, prefix uint64) hash.Hash {
	randomBytes := make([]byte, 20)
	n, err := rand.Read(randomBytes)
	assert.Equal(t, 20, n)
	assert.NoError(t, err)

	binary.BigEndian.PutUint64(randomBytes, prefix)
	return hash.Hash(randomBytes)
}
