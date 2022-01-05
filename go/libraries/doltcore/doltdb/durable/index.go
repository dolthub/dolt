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

	"github.com/dolthub/dolt/go/store/types"
)

type IndexSet interface {
	// GetIndex gets an index from the set.
	GetIndex(ctx context.Context, name string) (types.Map, error)

	// PutIndex puts an index into the set.
	// todo(andy): should this be immutable
	PutIndex(ctx context.Context, name string, idx types.Map) error

	// DropIndex removes an index from the set.
	DropIndex(ctx context.Context, name string) error
}

func NewIndexSet(ctx context.Context, vrw types.ValueReadWriter) IndexSet {
	empty, _ := types.NewMap(ctx, vrw)
	return &nomsIndexSet{
		indexes: empty,
		vrw:     vrw,
	}
}

func MapFromIndexSet(ic IndexSet) types.Map {
	return ic.(*nomsIndexSet).indexes
}

type nomsIndexSet struct {
	indexes types.Map
	vrw     types.ValueReadWriter
}

var _ IndexSet = &nomsIndexSet{}

func (c *nomsIndexSet) GetIndex(ctx context.Context, name string) (types.Map, error) {
	v, ok, err := c.indexes.MaybeGet(ctx, types.String(name))
	if !ok {
		err = fmt.Errorf("index %s not found in IndexSet", name)
	}
	if err != nil {
		return types.Map{}, err
	}

	v, err = v.(types.Ref).TargetValue(ctx, c.vrw)
	if err != nil {
		return types.Map{}, err
	}

	return v.(types.Map), nil
}

func (c *nomsIndexSet) PutIndex(ctx context.Context, name string, idx types.Map) (err error) {
	ref, err := refFromNomsValue(ctx, c.vrw, idx)
	if err != nil {
		return err
	}

	c.indexes, err = c.indexes.Edit().Set(types.String(name), ref).Map(ctx)
	return err
}

func (c *nomsIndexSet) DropIndex(ctx context.Context, name string) (err error) {
	c.indexes, err = c.indexes.Edit().Remove(types.String(name)).Map(ctx)
	return err
}
