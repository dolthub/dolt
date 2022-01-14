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

package durable

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"

	"github.com/dolthub/dolt/go/store/types"
)

type Index interface {
	// HashOf returns the hash.Hash of this table.
	HashOf() (hash.Hash, error)

	// Count returns the cardinality of the index.
	Count() uint64
}

type IndexSet interface {
	// HashOf returns the hash.Hash of this table.
	HashOf() (hash.Hash, error)

	// GetIndex gets an index from the set.
	GetIndex(ctx context.Context, name string) (Index, error)

	// PutIndex puts an index into the set.
	PutIndex(ctx context.Context, name string, idx Index) (IndexSet, error)

	// PutNomsIndex puts a noms index into the set.
	// todo(andy): this is a temporary stop-gap while abstracting types.Map
	PutNomsIndex(ctx context.Context, name string, idx types.Map) (IndexSet, error)

	// DropIndex removes an index from the set.
	DropIndex(ctx context.Context, name string) (IndexSet, error)
}

type nomsIndex struct {
	index types.Map
	vrw   types.ValueReadWriter
}

func NomsMapFromIndex(i Index) types.Map {
	return i.(nomsIndex).index
}

func IndexFromNomsMap(m types.Map, vrw types.ValueReadWriter) Index {
	return nomsIndex{
		index: m,
		vrw:   vrw,
	}
}

var _ Index = nomsIndex{}

func (i nomsIndex) HashOf() (hash.Hash, error) {
	return i.index.Hash(i.vrw.Format())
}

func (i nomsIndex) Count() uint64 {
	return i.index.Len()
}

func NewIndexSet(ctx context.Context, vrw types.ValueReadWriter) IndexSet {
	empty, _ := types.NewMap(ctx, vrw)
	return nomsIndexSet{
		indexes: empty,
		vrw:     vrw,
	}
}

type nomsIndexSet struct {
	indexes types.Map
	vrw     types.ValueReadWriter
}

var _ IndexSet = nomsIndexSet{}

// HashOf implementes IndexSet.
func (s nomsIndexSet) HashOf() (hash.Hash, error) {
	return s.indexes.Hash(s.vrw.Format())
}

// GetIndex implementes IndexSet.
func (s nomsIndexSet) GetIndex(ctx context.Context, name string) (Index, error) {
	v, ok, err := s.indexes.MaybeGet(ctx, types.String(name))
	if !ok {
		err = fmt.Errorf("index %s not found in IndexSet", name)
	}
	if err != nil {
		return nil, err
	}

	v, err = v.(types.Ref).TargetValue(ctx, s.vrw)
	if err != nil {
		return nil, err
	}

	return nomsIndex{
		index: v.(types.Map),
		vrw:   s.vrw,
	}, nil
}

// PutIndex implementes IndexSet.
func (s nomsIndexSet) PutNomsIndex(ctx context.Context, name string, idx types.Map) (IndexSet, error) {
	return s.PutIndex(ctx, name, IndexFromNomsMap(idx, s.vrw))
}

// PutIndex implementes IndexSet.
func (s nomsIndexSet) PutIndex(ctx context.Context, name string, idx Index) (IndexSet, error) {
	ref, err := refFromNomsValue(ctx, s.vrw, idx.(nomsIndex).index)
	if err != nil {
		return nil, err
	}

	im, err := s.indexes.Edit().Set(types.String(name), ref).Map(ctx)
	if err != nil {
		return nil, err
	}

	return nomsIndexSet{indexes: im, vrw: s.vrw}, nil
}

// DropIndex implementes IndexSet.
func (s nomsIndexSet) DropIndex(ctx context.Context, name string) (IndexSet, error) {
	im, err := s.indexes.Edit().Remove(types.String(name)).Map(ctx)
	if err != nil {
		return nil, err
	}

	return nomsIndexSet{indexes: im, vrw: s.vrw}, nil
}

func mapFromIndexSet(ic IndexSet) types.Map {
	return ic.(nomsIndexSet).indexes
}
