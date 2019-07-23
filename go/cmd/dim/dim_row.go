// Copyright 2019 Liquidata, Inc.
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

package main

import (
	"context"
	"log"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/valutil"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

type DimRow struct {
	key         types.Value
	dbVals      row.TaggedValues
	currentVals row.TaggedValues
	toTyped     *rowconv.RowConverter
}

func NewDimRow(r row.Row, toUntyped, toTyped *rowconv.RowConverter) (*DimRow, error) {
	key := r.NomsMapKey(toUntyped.SrcSch).Value(context.Background())
	untyped, err := toUntyped.Convert(r)

	if err != nil {
		return nil, err
	}

	return &DimRow{key, row.GetTaggedVals(untyped), row.GetTaggedVals(untyped), toTyped}, nil
}

func (dr *DimRow) StoreValue(me *types.MapEditor) *types.MapEditor {
	r := row.New(me.Format(), dr.toTyped.SrcSch, dr.currentVals)
	typed, err := dr.toTyped.Convert(r)

	if err != nil {
		panic(err)
	}

	typedSch := dr.toTyped.DestSch
	key := typed.NomsMapKey(typedSch).Value(context.Background())

	if !dr.key.Equals(key) {
		me = me.Remove(dr.key)
	}

	dr.key = key
	dr.dbVals = dr.currentVals
	log.Println("stored vals")

	return me.Set(key, typed.NomsMapValue(typedSch))
}

func (dr *DimRow) UpdateVal(tag uint64, str string) error {
	strVal := types.String(str)
	convFunc := dr.toTyped.ConvFuncs[tag]

	_, err := convFunc(strVal)

	if err != nil {
		return err
	}

	dr.currentVals[tag] = strVal
	return nil
}

func (dr *DimRow) KeyChanged() bool {
	equal := true
	dr.toTyped.DestSch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		currVal, _ := dr.currentVals[tag]
		dbVal, _ := dr.dbVals[tag]

		equal = valutil.NilSafeEqCheck(currVal, dbVal)

		return !equal
	})

	return equal
}

func (dr *DimRow) HasChanged() bool {
	if len(dr.currentVals) != len(dr.dbVals) {
		return true
	}

	equal := true
	dr.currentVals.Iter(func(tag uint64, val types.Value) bool {
		val2, ok := dr.dbVals.Get(tag)
		equal = ok && val.Equals(val2)

		return !equal
	})

	return !equal
}
