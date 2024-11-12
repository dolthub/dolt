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
	Count() (uint64, error)
	Format() *types.NomsBinFormat
	HasConflicts(ctx context.Context) (bool, error)
	// ConflictCount returns the number of conflicts
	ConflictCount(ctx context.Context) (uint64, error)
	// ConstraintViolationCount returns the sum total of foreign key violations,
	// unique key violations, and check constraint violations.
	ConstraintViolationCount(ctx context.Context) (uint64, error)
	// ClearConflicts clears all conflicts
	ClearConflicts(ctx context.Context) (ArtifactIndex, error)
}

// RefFromArtifactIndex persists |idx| and returns the types.Ref targeting it.
func RefFromArtifactIndex(ctx context.Context, vrw types.ValueReadWriter, idx ArtifactIndex) (types.Ref, error) {
	switch idx.Format() {
	case types.Format_LD_1:
		panic("TODO")

	case types.Format_DOLT:
		b := shim.ValueFromMap(idx.(prollyArtifactIndex).index)
		return refFromNomsValue(ctx, vrw, b)

	default:
		return types.Ref{}, errNbfUnknown
	}
}

// NewEmptyArtifactIndex returns an ArtifactIndex with no artifacts.
func NewEmptyArtifactIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, tableSch schema.Schema) (ArtifactIndex, error) {
	switch vrw.Format() {
	case types.Format_LD_1:
		panic("TODO")

	case types.Format_DOLT:
		kd := tableSch.GetKeyDescriptor()
		m, err := prolly.NewArtifactMapFromTuples(ctx, ns, kd)
		if err != nil {
			return nil, err
		}
		return ArtifactIndexFromProllyMap(m), nil

	default:
		return nil, errNbfUnknown
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

func artifactIndexFromRef(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, tableSch schema.Schema, r types.Ref) (ArtifactIndex, error) {
	return artifactIndexFromAddr(ctx, vrw, ns, tableSch, r.TargetHash())
}

func artifactIndexFromAddr(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, tableSch schema.Schema, addr hash.Hash) (ArtifactIndex, error) {
	v, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	switch vrw.Format() {
	case types.Format_LD_1:
		panic("TODO")

	case types.Format_DOLT:
		root, err := shim.NodeFromValue(v)
		if err != nil {
			return nil, err
		}
		kd := tableSch.GetKeyDescriptor()
		m := prolly.NewArtifactMap(root, ns, kd)
		return ArtifactIndexFromProllyMap(m), nil

	default:
		return nil, errNbfUnknown
	}
}

type prollyArtifactIndex struct {
	index prolly.ArtifactMap
}

func (i prollyArtifactIndex) HashOf() (hash.Hash, error) {
	return i.index.HashOf(), nil
}

func (i prollyArtifactIndex) Count() (uint64, error) {
	c, err := i.index.Count()
	return uint64(c), err
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

func (i prollyArtifactIndex) ConstraintViolationCount(ctx context.Context) (uint64, error) {
	return i.index.CountOfTypes(ctx,
		prolly.ArtifactTypeForeignKeyViol,
		prolly.ArtifactTypeUniqueKeyViol,
		prolly.ArtifactTypeChkConsViol,
		prolly.ArtifactTypeNullViol)
}

func (i prollyArtifactIndex) ClearConflicts(ctx context.Context) (ArtifactIndex, error) {
	updated, err := i.index.ClearArtifactsOfType(ctx, prolly.ArtifactTypeConflict)
	if err != nil {
		return nil, err
	}
	return prollyArtifactIndex{updated}, nil
}
