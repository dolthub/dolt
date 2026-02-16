// Copyright 2023 Dolthub, Inc.
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
	"bufio"
	"bytes"
	"hash/crc32"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func TestRoundTripIndexLookups(t *testing.T) {
	// write lookups to a writer
	buf := new(bytes.Buffer)
	w := bufio.NewWriter(buf)
	batches := 10
	chunksPerBatch := 1000
	start := uint64(math.MaxInt32)
	var off int
	for i := 0; i <= batches; i++ {
		lookups, meta := newLookups(t, chunksPerBatch, start)
		for _, l := range lookups {
			err := writeIndexLookup(w, l)
			require.NoError(t, err)
		}
		err := writeJournalIndexMeta(w, meta.latestHash, meta.batchStart, meta.batchEnd, meta.checkSum)
		require.NoError(t, err)
		start = uint64(meta.batchEnd)
		off += (1+lookupSz)*chunksPerBatch + (1 + lookupMetaSz)
	}

	// read lookups from the buffer
	lookupCnt := 0
	metaCnt := 0

	_, err := processIndexRecords(bufio.NewReader(buf), int64(off), func(meta lookupMeta, lookups []lookup, checksum uint32) error {
		require.Equal(t, meta.checkSum, checksum)
		lookupCnt += len(lookups)
		metaCnt += 1
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, batches*chunksPerBatch, lookupCnt)
	require.Equal(t, batches, metaCnt)
}

func newLookups(t *testing.T, n int, start uint64) ([]lookup, lookupMeta) {
	var lookups []lookup
	var off uint64
	var end uint64
	var checksum uint32
	hashes := genBytes(20, n)
	for _, h := range hashes {
		addr := toAddr16(hash.New(h))
		length := (rand.Uint64() % 1024)
		checksum = crc32.Update(checksum, crcTable, addr[:])
		start = end
		lookups = append(lookups, lookup{
			a: toAddr16(hash.New(h)),
			r: Range{Offset: off, Length: uint32(length)},
		})
		off += length
		end = start + (rand.Uint64() % 1024)
	}
	return lookups, lookupMeta{
		batchStart: int64(start),
		batchEnd:   int64(end),
		checkSum:   checksum,
		latestHash: hash.New(hashes[len(hashes)-1]),
	}
}

func genBytes(size, count int) (keys [][]byte) {
	src := rand.New(rand.NewSource(int64(size * count)))
	letters := []byte("123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	r := make([]byte, size*count)
	for i := range r {
		r[i] = letters[src.Intn(len(letters))]
	}
	keys = make([][]byte, count)
	for i := range keys {
		keys[i] = r[:size]
		r = r[size:]
	}
	return
}

func TestRoundTripIndexLookupMeta(t *testing.T) {
	// write metadata to buffer
	// read from buffer
}

func TestRoundTripIndexLookupsMeta(t *testing.T) {
	// create writer
	// add lookups through ranges.put
	// flush with flushIndexRecord
	// do a bunch of iters
	// use processIndexRecords2 to read back, make sure roots/checksums are consistent, counts, etc
}
