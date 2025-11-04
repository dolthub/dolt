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
	"testing"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func TestSizes(t *testing.T) {
	// These sizes should not change, and if they do change they
	// should change carefully. In particular, allocation for
	// rebuilding indexes in Conjoin needs to be able to allocate
	// all O(n) memory blocks reliably, so these structs should
	// never contain heap allocated memory or interfaces, etc.
	assert.Equal(t, hash.ByteLen + uint32Size + uint32Size, prefixIndexRecSize)
	assert.Equal(t, uint64Size + uint64Size, byteSpanSize)
	assert.Equal(t, hash.ByteLen + uint32Size + uint32Size, stagedChunkRefSize)
	assert.Equal(t, hash.ByteLen + uint32Size + uint64Size, tableChunkRecordSize)
}
