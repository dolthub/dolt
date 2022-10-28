// Copyright 2022 Dolthub, Inc.
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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkJournalAddr(t *testing.T) {
	expAddr := addr{
		255, 255, 255, 255, 255,
		255, 255, 255, 255, 255,
		255, 255, 255, 255, 255,
		255, 255, 255, 255, 255,
	}
	expString := "vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"
	assert.Equal(t, expAddr, chunkJournalAddr)
	assert.Equal(t, expString, chunkJournalAddr.String())
	assert.Equal(t, uint64(math.MaxUint64), chunkJournalAddr.Prefix())
	assert.Equal(t, uint32(math.MaxUint32), chunkJournalAddr.Checksum())
}
