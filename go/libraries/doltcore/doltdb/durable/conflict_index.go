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
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type ConflictIndex interface {
	HashOf() (hash.Hash, error)
	Count() uint64
	Format() *types.NomsBinFormat
}

// RefFromConflictIndex persists |idx| and returns the types.Ref targeting it.
func RefFromConflictIndex(ctx context.Context, vrw types.ValueReadWriter, idx ConflictIndex) (types.Ref, error) {
	switch idx.Format() {
	case types.Format_LD_1, types.Format_7_18:
		return refFromNomsValue(ctx, vrw, idx.(nomsConflictIndex).index)

	case types.Format_DOLT:
		return types.Ref{}, fmt.Errorf("__DOLT__ conflicts should be stored in ArtifactIndex")

	default:
		return types.Ref{}, errNbfUnkown
	}
}

// NewEmptyConflictIndex returns an ConflictIndex with no rows.
func NewEmptyConflictIndex(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, oursSch, theirsSch, baseSch schema.Schema) (ConflictIndex, error) {
	switch vrw.Format() {
	case types.Format_LD_1, types.Format_7_18:
		m, err := types.NewMap(ctx, vrw)
		if err != nil {
			return nil, err
		}
		return ConflictIndexFromNomsMap(m, vrw), nil

	case types.Format_DOLT:
		return nil, fmt.Errorf("__DOLT__ conflicts should be stored in ArtifactIndex")

	default:
		return nil, errNbfUnkown
	}
}

func ConflictIndexFromNomsMap(m types.Map, vrw types.ValueReadWriter) ConflictIndex {
	return nomsConflictIndex{
		index: m,
		vrw:   vrw,
	}
}

func NomsMapFromConflictIndex(i ConflictIndex) types.Map {
	return i.(nomsConflictIndex).index
}

func conflictIndexFromRef(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, ourSch, theirSch, baseSch schema.Schema, r types.Ref) (ConflictIndex, error) {
	return conflictIndexFromAddr(ctx, vrw, ns, ourSch, theirSch, baseSch, r.TargetHash())
}

func conflictIndexFromAddr(ctx context.Context, vrw types.ValueReadWriter, ns tree.NodeStore, ourSch, theirSch, baseSch schema.Schema, addr hash.Hash) (ConflictIndex, error) {
	v, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	switch vrw.Format() {
	case types.Format_LD_1, types.Format_7_18:
		return ConflictIndexFromNomsMap(v.(types.Map), vrw), nil

	case types.Format_DOLT:
		return nil, fmt.Errorf("__DOLT__ conflicts should be stored in ArtifactIndex")

	default:
		return nil, errNbfUnkown
	}
}

type nomsConflictIndex struct {
	index types.Map
	vrw   types.ValueReadWriter
}

func (i nomsConflictIndex) HashOf() (hash.Hash, error) {
	return i.index.Hash(i.vrw.Format())
}

func (i nomsConflictIndex) Count() uint64 {
	return i.index.Len()
}

func (i nomsConflictIndex) Format() *types.NomsBinFormat {
	return i.vrw.Format()
}
