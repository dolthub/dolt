// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"
	"github.com/attic-labs/noms/go/d"
)

// CreateEditAcc defines a factory method for EditAccumulator creation
type CreateEditAcc func() EditAccumulator

// CreateEditAccForMapEdits allows users to define the EditAccumulator that should be used when creating a MapEditor via
// the Map.Edit method.  In most cases you should call:
//
// func init() {
// 		types.CreateEditAccForMapEdits = func() EditAccumulator {
//			return edits.NewAsyncSortedEdits(10000, 4, 2) // configure your own constants
// 		}
// }
var CreateEditAccForMapEdits CreateEditAcc = NewDumbEditAccumulator

// EditAccumulator is an interface for a datastructure that can have edits added to it.  Once all edits are
// added FinishedEditing can be called to get an EditProvider which provides the edits in sorted order
type EditAccumulator interface {
	// AddEdit adds an edit to the list of edits
	AddEdit(k LesserValuable, v Valuable)

	// FinishEditing should be called when all edits have been added to get an EditProvider which provides the
	// edits in sorted order.  Adding more edits after calling FinishedEditing is an error
	FinishedEditing() EditProvider
}

// MapEditor allows for efficient editing of Map-typed prolly trees. Edits
// are buffered to memory and can be applied via Build(), which returns a new
// Map. Prior to Build(), Get() & Has() will return the value that the resulting
// Map would return if it were built immediately prior to the respective call.
// Note: The implementation biases performance towards a usage which applies
// edits in key-order.
type MapEditor struct {
	m        Map
	numEdits int64
	acc      EditAccumulator
}

func NewMapEditor(m Map) *MapEditor {
	return &MapEditor{m, 0, CreateEditAccForMapEdits()}
}

// Map applies all edits and returns a newly updated Map
func (me *MapEditor) Map(ctx context.Context) Map {
	edits := me.acc.FinishedEditing()
	return ApplyEdits(ctx, edits, me.m)
}

// Set adds an edit
func (me *MapEditor) Set(k LesserValuable, v Valuable) *MapEditor {
	d.PanicIfTrue(v == nil)
	me.set(k, v)
	return me
}

// SetM adds M edits where even values are keys followed by their respective value
func (me *MapEditor) SetM(kv ...Valuable) *MapEditor {
	d.PanicIfFalse(len(kv)%2 == 0)

	for i := 0; i < len(kv); i += 2 {
		me.Set(kv[i].(LesserValuable), kv[i+1])
	}
	return me
}

// Remove adds an edit that will remove a value by key
func (me *MapEditor) Remove(k LesserValuable) *MapEditor {
	me.set(k, nil)
	return me
}

func (me *MapEditor) set(k LesserValuable, v Valuable) {
	me.numEdits++
	me.acc.AddEdit(k, v)
}

func (me *MapEditor) NumEdits() int64 {
	return me.numEdits
}
