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

package nbs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompressedChunkIsEmpty(t *testing.T) {
	if !EmptyCompressedChunk.IsEmpty() {
		t.Fatal("EmptyCompressedChunkIsEmpty() should equal true.")
	}
	if !(CompressedChunk{}).IsEmpty() {
		t.Fatal("CompressedChunk{}.IsEmpty() should equal true.")
	}
}

func TestCanReadAhead(t *testing.T) {
	type expected struct {
		end uint64
		can bool
	}
	type testCase struct {
		rec       offsetRec
		start     uint64
		end       uint64
		blockSize uint64
		ex        expected
	}
	for _, c := range []testCase{
		testCase{offsetRec{offset: 8191, length: 2048}, 0, 4096, 4096, expected{end: 10239, can: true}},
		testCase{offsetRec{offset: 8191, length: 2048}, 0, 4096, 2048, expected{end: 4096, can: false}},
		testCase{offsetRec{offset: 2048, length: 2048}, 0, 4096, 2048, expected{end: 4096, can: true}},
		testCase{offsetRec{offset: (1 << 27), length: 2048}, 0, 128 * 1024 * 1024, 4096, expected{end: 134217728, can: false}},
	} {
		end, can := canReadAhead(c.rec, c.start, c.end, c.blockSize)
		assert.Equal(t, c.ex.end, end)
		assert.Equal(t, c.ex.can, can)
	}
}
