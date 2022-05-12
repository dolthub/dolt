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
	case types.Format_LD_1, types.Format_7_18, types.Format_DOLT_DEV:
		return refFromNomsValue(ctx, vrw, idx.(nomsConflictIndex).index)

	case types.Format_DOLT_1:
		b := prolly.ValueFromConflictMap(idx.(prollyConflictIndex).index)
		return refFromNomsValue(ctx, vrw, b)

	default:
		return types.Ref{}, errNbfUnkown
	}
}

// NewEmptyConflictIndex returns an ConflictIndex with no rows.
func NewEmptyConflictIndex(ctx context.Context, vrw types.ValueReadWriter, oursSch, theirsSch, baseSch schema.Schema) (ConflictIndex, error) {
	switch vrw.Format() {
	case types.Format_LD_1, types.Format_7_18, types.Format_DOLT_DEV:
		m, err := types.NewMap(ctx, vrw)
		if err != nil {
			return nil, err
		}
		return ConflictIndexFromNomsMap(m, vrw), nil

	case types.Format_DOLT_1:
		kd, oursVD := prolly.MapDescriptorsFromScheam(oursSch)
		theirsVD := prolly.ValueDescriptorFromSchema(theirsSch)
		baseVD := prolly.ValueDescriptorFromSchema(baseSch)
		ns := tree.NewNodeStore(prolly.ChunkStoreFromVRW(vrw))

		m := prolly.NewEmptyConflictMap(ns, kd, oursVD, theirsVD, baseVD)

		return ConflictIndexFromProllyMap(m), nil

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

func ConflictIndexFromProllyMap(m prolly.ConflictMap) ConflictIndex {
	return prollyConflictIndex{
		index: m,
	}
}

func ProllyMapFromConflictIndex(i ConflictIndex) prolly.ConflictMap {
	return i.(prollyConflictIndex).index
}

func conflictIndexFromRef(ctx context.Context, vrw types.ValueReadWriter, ourSch, theirSch, baseSch schema.Schema, r types.Ref) (ConflictIndex, error) {
	return conflictIndexFromAddr(ctx, vrw, ourSch, theirSch, baseSch, r.TargetHash())
}

func conflictIndexFromAddr(ctx context.Context, vrw types.ValueReadWriter, ourSch, theirSch, baseSch schema.Schema, addr hash.Hash) (ConflictIndex, error) {
	v, err := vrw.ReadValue(ctx, addr)
	if err != nil {
		return nil, err
	}

	switch vrw.Format() {
	case types.Format_LD_1, types.Format_7_18, types.Format_DOLT_DEV:
		return ConflictIndexFromNomsMap(v.(types.Map), vrw), nil

	case types.Format_DOLT_1:
		m := prolly.ConflictMapFromValue(v, ourSch, theirSch, baseSch, vrw)
		return ConflictIndexFromProllyMap(m), nil

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

type prollyConflictIndex struct {
	index prolly.ConflictMap
}

func (i prollyConflictIndex) HashOf() (hash.Hash, error) {
	return i.index.HashOf(), nil
}

func (i prollyConflictIndex) Count() uint64 {
	return uint64(i.index.Count())
}

func (i prollyConflictIndex) Format() *types.NomsBinFormat {
	return i.index.Format()
}
