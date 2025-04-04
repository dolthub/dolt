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

package tree

import (
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/val"
)

// TestDifferFromRoots tests the DifferFromRoots function, very minimally. We don't have any direct tests of this
// method, and when developing the layerDifferFromRoots method I wanted to verify some assumptions.
// TODO - test DifferFromRoots more thoroughly.
func TestDifferFromRoots(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()

	fromTups, desc := AscendingUintTuples(1234)
	fromRoot := makeTree(t, fromTups)

	toTups := make([][2]val.Tuple, len(fromTups))
	// Copy elements from the original slice to the new slice
	copy(toTups, fromTups)
	bld := val.NewTupleBuilder(desc, ns)
	bld.PutUint32(0, uint32(42))
	var err error
	toTups[23][1], err = bld.Build(sharedPool) // modify value at index 23.
	assert.NoError(t, err)
	toRoot := makeTree(t, toTups)

	dfr, err := DifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
	assert.NoError(t, err)

	dif, err := dfr.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, dif)
	assert.Equal(t, fromTups[23][0], val.Tuple(dif.Key))
	assert.Equal(t, fromTups[23][1], val.Tuple(dif.From))
	assert.Equal(t, toTups[23][1], val.Tuple(dif.To))
	assert.Equal(t, ModifiedDiff, dif.Type)

	dif, err = dfr.Next(ctx)
	assert.Equal(t, io.EOF, err)
}
