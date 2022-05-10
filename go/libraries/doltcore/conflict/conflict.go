// Copyright 2019 Dolthub, Inc.
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

package conflict

import (
	"context"
	"errors"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/store/types"
)

type ConflictSchema struct {
	Base        schema.Schema
	Schema      schema.Schema
	MergeSchema schema.Schema
}

func NewConflictSchema(base, sch, mergeSch schema.Schema) ConflictSchema {
	return ConflictSchema{
		Base:        base,
		Schema:      sch,
		MergeSchema: mergeSch,
	}
}

func ValueFromConflictSchema(ctx context.Context, vrw types.ValueReadWriter, cs ConflictSchema) (types.Value, error) {
	b, err := serializeSchema(ctx, vrw, cs.Base)
	if err != nil {
		return nil, err
	}

	s, err := serializeSchema(ctx, vrw, cs.Schema)
	if err != nil {
		return nil, err
	}

	m, err := serializeSchema(ctx, vrw, cs.MergeSchema)
	if err != nil {
		return nil, err
	}

	return types.NewTuple(vrw.Format(), b, s, m)
}

func ConflictSchemaFromValue(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (cs ConflictSchema, err error) {
	tup, ok := v.(types.Tuple)
	if !ok {
		err = errors.New("conflict schema value must be types.Struct")
		return ConflictSchema{}, err
	}

	b, err := tup.Get(0)
	if err != nil {
		return ConflictSchema{}, err
	}
	cs.Base, err = deserializeSchema(ctx, vrw, b)
	if err != nil {
		return ConflictSchema{}, err
	}

	s, err := tup.Get(1)
	if err != nil {
		return ConflictSchema{}, err
	}
	cs.Schema, err = deserializeSchema(ctx, vrw, s)
	if err != nil {
		return ConflictSchema{}, err
	}

	m, err := tup.Get(2)
	if err != nil {
		return ConflictSchema{}, err
	}
	cs.MergeSchema, err = deserializeSchema(ctx, vrw, m)
	if err != nil {
		return ConflictSchema{}, err
	}

	return cs, nil
}

func serializeSchema(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Ref, error) {
	st, err := encoding.MarshalSchemaAsNomsValue(ctx, vrw, sch)
	if err != nil {
		return types.Ref{}, err
	}

	return vrw.WriteValue(ctx, st)
}

func deserializeSchema(ctx context.Context, vrw types.ValueReadWriter, v types.Value) (schema.Schema, error) {
	r, ok := v.(types.Ref)
	if !ok {
		return nil, errors.New("conflict schemas field value is unexpected type")
	}

	tv, err := r.TargetValue(ctx, vrw)
	if err != nil {
		return nil, err
	}

	return encoding.UnmarshalSchemaNomsValue(ctx, vrw.Format(), tv)
}

type Conflict struct {
	Base       types.Value
	Value      types.Value
	MergeValue types.Value
}

func NewConflict(base, value, mergeValue types.Value) Conflict {
	if base == nil {
		base = types.NullValue
	}
	if value == nil {
		value = types.NullValue
	}
	if mergeValue == nil {
		mergeValue = types.NullValue
	}
	return Conflict{base, value, mergeValue}
}

func ConflictFromTuple(tpl types.Tuple) (Conflict, error) {
	base, err := tpl.Get(0)

	if err != nil {
		return Conflict{}, err
	}

	val, err := tpl.Get(1)

	if err != nil {
		return Conflict{}, err
	}

	mv, err := tpl.Get(2)

	if err != nil {
		return Conflict{}, err
	}
	return Conflict{base, val, mv}, nil
}

func (c Conflict) ToNomsList(vrw types.ValueReadWriter) (types.Tuple, error) {
	return types.NewTuple(vrw.Format(), c.Base, c.Value, c.MergeValue)
}
