package main

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/valutil"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"log"
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
	r := row.New(dr.toTyped.SrcSch, dr.currentVals)
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
