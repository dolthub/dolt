// Copyright 2021 Dolthub, Inc.
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

package edits

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/types"
)

func TestDiskBackedEdits(t *testing.T) {
	const (
		maxKVPs = 64 * 1024
	)

	size := maxKVPs
	rng := rand.New(rand.NewSource(0))
	kvps := createKVPs(t, types.Format_Default, rng, maxKVPs)
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			testDBE(t, kvps[:size])
		})
		size = rng.Intn(maxKVPs)
	}

	// test something smaller than the flush interval
	size = 4
	t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
		testDBE(t, kvps[:size])
	})
}

func testDBE(t *testing.T, kvps []types.KVP) {
	ctx := context.Background()
	nbf := types.Format_Default
	vrw := types.NewMemoryValueStore()
	tmpDir, err := os.MkdirTemp("", "TestDiskBackedEdits")
	require.NoError(t, err)

	newEA := func() types.EditAccumulator {
		return NewAsyncSortedEdits(nbf, 64, 2, 2)
	}

	dbe := NewDiskBackedEditAcc(ctx, nbf, vrw, 2*1024, tmpDir, newEA)
	for _, kvp := range kvps {
		dbe.AddEdit(kvp.Key, kvp.Val)
	}

	itr, err := dbe.FinishedEditing()
	assert.NoError(t, err)

	inOrder, count, err := IsInOrder(nbf, itr)

	assert.NoError(t, err)
	require.Equal(t, len(kvps), count, "Invalid count %d != %d", count, len(kvps))
	require.True(t, inOrder)
}
