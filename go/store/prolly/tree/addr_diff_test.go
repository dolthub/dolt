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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/val"
)

// Single layer trees are entirely root nodes - which are embedded in the table flatbuffer, so we don't
// currently use them for purposes of grouping chunks.
func TestAddressDifferFromRootsOneLayer(t *testing.T) {
	fromTups, desc := AscendingUintTuples(42)
	fromRoot := makeTree(t, fromTups)
	assert.Equal(t, 42, fromRoot.Count())
	assert.Equal(t, 0, fromRoot.Level())

	toTups := make([][2]val.Tuple, len(fromTups))
	// Copy elements from the original slice to the new slice
	copy(toTups, fromTups)
	bld := val.NewTupleBuilder(desc)
	// modify value in the first half of the tree
	bld.PutUint32(0, uint32(42))
	toTups[23][1] = bld.Build(sharedPool)
	toRoot := makeTree(t, toTups)

	ctx := context.Background()
	ns := NewTestNodeStore()

	dfr, err := layerDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
	assert.NoError(t, err)

	_, err = dfr.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestAddressDifferFromRootsTwoLayer(t *testing.T) {
	fromTups, desc := AscendingUintTuples(416)
	fromRoot := makeTree(t, fromTups)
	assert.Equal(t, 2, fromRoot.Count())
	assert.Equal(t, 1, fromRoot.Level())

	before := fromRoot.getAddress(0)
	unchanged := fromRoot.getAddress(1)

	toTups := make([][2]val.Tuple, len(fromTups))
	// Copy elements from the original slice to the new slice
	copy(toTups, fromTups)
	bld := val.NewTupleBuilder(desc)
	// modify value early in the tree, to ensure the modification happens on the first child of the root.
	bld.PutUint32(0, uint32(42))
	toTups[23][1] = bld.Build(sharedPool)
	toRoot := makeTree(t, toTups)

	after := toRoot.getAddress(0)
	assert.NotEqual(t, before, after)
	assert.Equal(t, unchanged, toRoot.getAddress(1))

	ctx := context.Background()
	ns := NewTestNodeStore()

	dfr, err := layerDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
	assert.NoError(t, err)

	dif, err := dfr.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, before, dif.From)
	assert.Equal(t, after, dif.To)

	_, err = dfr.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestAddressDifferFromRootsThreeLayer(t *testing.T) {
	// 23800 - results in a 3 level tree, where the root has two children. The second child will have one child.
	// of its own, which is the content. If we alter the content in the last ~250 elements of the tuple, it should
	// result in a modification of the second child of the root.
	fromTups, desc := AscendingUintTuples(23800)
	fromRoot := makeTree(t, fromTups)
	assert.Equal(t, 2, fromRoot.Count())
	assert.Equal(t, 2, fromRoot.Level())

	toTups := make([][2]val.Tuple, len(fromTups))
	// Copy elements from the original slice to the new slice
	copy(toTups, fromTups)
	bld := val.NewTupleBuilder(desc)
	// modify value in the second half of the tree
	bld.PutUint32(0, uint32(42))
	toTups[23700][1] = bld.Build(sharedPool)
	toRoot := makeTree(t, toTups)

	ctx := context.Background()
	ns := NewTestNodeStore()
	dfr, err := layerDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
	assert.NoError(t, err)

	// Manually grab the items from the tree we expect to see from out of the differ.
	fromMidLayerAddr := fromRoot.getAddress(1)
	toMidLayerAddr := toRoot.getAddress(1)

	fromContentNode, _ := ns.Read(ctx, fromMidLayerAddr)
	fromContentAddr := fromContentNode.getAddress(0)
	toContentNode, _ := ns.Read(ctx, toMidLayerAddr)
	toContentAddr := toContentNode.getAddress(0)

	// Items returned from the diff stream will start at the content addresses, then mid layer, and end at the root.
	dif, err := dfr.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, fromContentAddr, dif.From)
	assert.Equal(t, toContentAddr, dif.To)

	dif, err = dfr.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, fromMidLayerAddr, dif.From)
	assert.Equal(t, toMidLayerAddr, dif.To)

	dif, err = dfr.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestAddressDifferFromRootsLayerMismatch(t *testing.T) {
	fromTups, desc := AscendingUintTuples(416)
	fromRoot := makeTree(t, fromTups)
	assert.Equal(t, 2, fromRoot.Count())
	assert.Equal(t, 1, fromRoot.Level())

	toTups := fromTups[:415]
	toRoot := makeTree(t, toTups)
	assert.Equal(t, 415, toRoot.Count())
	assert.Equal(t, 0, toRoot.Level())

	ctx := context.Background()
	ns := NewTestNodeStore()
	_, err := layerDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
	assert.Equal(t, ErrRootDepthMismatch, err)
}
