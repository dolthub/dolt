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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"context"

	"github.com/liquidata-inc/dolt/go/store/d"
)

// CreateEditAcc defines a factory method for EditAccumulator creation
type CreateEditAcc func(nbf *NomsBinFormat) EditAccumulator

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

// MapEditor allows for efficient editing of Map-typed prolly trees.
type MapEditor struct {
	m        Map
	numEdits int64
	acc      EditAccumulator
}

func NewMapEditor(m Map) *MapEditor {
	return &MapEditor{m, 0, CreateEditAccForMapEdits(m.format())}
}

// Map applies all edits and returns a newly updated Map
func (me *MapEditor) Map(ctx context.Context) (Map, error) {
	edits := me.acc.FinishedEditing()
	m, _, err := ApplyEdits(ctx, edits, me.m)
	return m, err
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

// NumEdits returns the number of edits that have been added.
func (me *MapEditor) NumEdits() int64 {
	return me.numEdits
}

func (me *MapEditor) Format() *NomsBinFormat {
	return me.m.format()
}
