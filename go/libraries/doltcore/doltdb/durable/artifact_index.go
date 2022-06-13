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

package durable

import (
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type ArtifactIndex interface {
	HashOf() (hash.Hash, error)
	Count() uint64
	Format() *types.NomsBinFormat

	HasConflicts(ctx context.Context) (bool, error)
	ConflictCount(ctx context.Context) (uint64, error)
	ClearConflicts(ctx context.Context) (ArtifactIndex, error)
}

// RefFromArtifactIndex persists |idx| and returns the types.Ref targeting it.
func RefFromArtifactIndex(ctx context.Context, vrw types.ValueReadWriter, idx ArtifactIndex) (types.Ref, error) {
	switch idx.Format() {
	case types.Format_LD_1, types.Format_7_18, types.Format_DOLT_DEV:
		panic("TODO")

	case types.Format_DOLT_1:
		b := shim.ValueFromArtifactMap(idx.(prollyArtifactIndex).index)
		return refFromNomsValue(ctx, vrw, b)

	default:
		return types.Ref{}, errNbfUnkown
	}
}

// NewEmptyArtifactIndex returns an ArtifactIndex with no artifacts.
func NewEmptyArtifactIndex(ctx context.Context, vrw types.ValueReadWriter, tableSch schema.Schema) (ArtifactIndex, error) {
	switch vrw.Format() {
	case types.Format_LD_1, types.Format_7_18, types.Format_DOLT_DEV:
		panic("TODO")

	case types.Format_DOLT_1:
		kd := shim.KeyDescriptorFromSchema(tableSch)
		ns := tree.NewNodeStore(shim.ChunkStoreFromVRW(vrw))
		m, err := prolly.NewArtifactMapFromTuples(ctx, ns, kd)
		if err != nil {
			return nil, err
		}
		return ArtifactIndexFromProllyMap(m), nil

	default:
		return nil, errNbfUnkown
	}
}

func ArtifactIndexFromProllyMap(m prolly.ArtifactMap) ArtifactIndex {
	return prollyArtifactIndex{
		index: m,
	}
}

func ProllyMapFromArtifactIndex(i ArtifactIndex) prolly.ArtifactMap {
	return i.(prollyArtifactIndex).index
}

func artifactIndexFromRef(ctx context.Context, vrw types.ValueReadWriter, tableSch schema.Schema, r types.Ref) (ArtifactIndex, error) {
	return artifactIndexFromAddr(ctx, vrw, tableSch, r.TargetHash())
}

func artifactIndexFromAddr(ctx context.Context, vrw types.ValueReadWriter, tableSch schema.Schema, addr hash.Hash) (ArtifactIndex, error) {
	v, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	switch vrw.Format() {
	case types.Format_LD_1, types.Format_7_18, types.Format_DOLT_DEV:
		panic("TODO")

	case types.Format_DOLT_1:
		root := shim.NodeFromValue(v)
		kd := shim.KeyDescriptorFromSchema(tableSch)
		ns := tree.NewNodeStore(shim.ChunkStoreFromVRW(vrw))
		m := prolly.NewArtifactMap(root, ns, kd)
		return ArtifactIndexFromProllyMap(m), nil

	default:
		return nil, errNbfUnkown
	}
}

type prollyArtifactIndex struct {
	index prolly.ArtifactMap
}

func (i prollyArtifactIndex) HashOf() (hash.Hash, error) {
	return i.index.HashOf(), nil
}

func (i prollyArtifactIndex) Count() uint64 {
	return uint64(i.index.Count())
}

func (i prollyArtifactIndex) Format() *types.NomsBinFormat {
	return i.index.Format()
}

func (i prollyArtifactIndex) HasConflicts(ctx context.Context) (bool, error) {
	return i.index.HasArtifactOfType(ctx, prolly.ArtifactTypeConflict)
}

func (i prollyArtifactIndex) ConflictCount(ctx context.Context) (uint64, error) {
	return i.index.CountOfType(ctx, prolly.ArtifactTypeConflict)
}

func (i prollyArtifactIndex) ClearConflicts(ctx context.Context) (ArtifactIndex, error) {
	updated, err := i.index.ClearArtifactsOfType(ctx, prolly.ArtifactTypeConflict)
	if err != nil {
		return nil, err
	}
	return prollyArtifactIndex{updated}, nil
}
