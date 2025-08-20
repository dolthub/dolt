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

	"github.com/dolthub/dolt/go/store/d"
)

// CreateEditAcc defines a factory method for EditAccumulator creation
type CreateEditAcc func(ValueReader) EditAccumulator

// CreateEditAccForMapEdits allows users to define the EditAccumulator that should be used when creating a MapEditor via
// the Map.Edit method.  In most cases you should call:
//
//	func init() {
//			types.CreateEditAccForMapEdits = func() EditAccumulator {
//				return edits.NewAsyncSortedEdits(10000, 4, 2) // configure your own constants
//			}
//	}
var CreateEditAccForMapEdits CreateEditAcc = NewDumbEditAccumulator

// EditAccumulator is an interface for a datastructure that can have edits added to it. Once all edits are
// added FinishedEditing can be called to get an EditProvider which provides the edits in sorted order
type EditAccumulator interface {
	// EditsAdded returns the number of edits that have been added to this EditAccumulator
	EditsAdded() int

	// AddEdit adds an edit to the list of edits.  Not thread safe.
	AddEdit(k LesserValuable, v Valuable)

	// FinishedEditing should be called when all edits have been added to get an EditProvider which provides the
	// edits in sorted order. Adding more edits after calling FinishedEditing is an error.
	FinishedEditing(context.Context) (EditProvider, error)

	// Close ensures that the accumulator is closed. Repeat calls are allowed. Not guaranteed to be thread-safe, thus
	// requires external synchronization.
	Close(context.Context) error
}

// MapEditor allows for efficient editing of Map-typed prolly trees.
type MapEditor struct {
	m        Map
	acc      EditAccumulator
	numEdits int64
}

func NewMapEditor(m Map) *MapEditor {
	return &MapEditor{m, CreateEditAccForMapEdits(m.valueReadWriter()), 0}
}

// Map applies all edits and returns a newly updated Map
func (med *MapEditor) Map(ctx context.Context) (Map, error) {
	edits, err := med.acc.FinishedEditing(ctx)
	if err != nil {
		return EmptyMap, err
	}

	m, _, err := ApplyEdits(ctx, edits, med.m)
	return m, err
}

// Set adds an edit
func (med *MapEditor) Set(k LesserValuable, v Valuable) *MapEditor {
	d.PanicIfTrue(v == nil)
	med.set(k, v)
	return med
}

// SetM adds M edits where even values are keys followed by their respective value
func (med *MapEditor) SetM(kv ...Valuable) *MapEditor {
	d.PanicIfFalse(len(kv)%2 == 0)

	for i := 0; i < len(kv); i += 2 {
		med.Set(kv[i].(LesserValuable), kv[i+1])
	}
	return med
}

// Remove adds an edit that will remove a value by key
func (med *MapEditor) Remove(k LesserValuable) *MapEditor {
	med.set(k, nil)
	return med
}

func (med *MapEditor) set(k LesserValuable, v Valuable) {
	med.numEdits++
	med.acc.AddEdit(k, v)
}

// NumEdits returns the number of edits that have been added.
func (med *MapEditor) NumEdits() int64 {
	return med.numEdits
}

func (med *MapEditor) Format() *NomsBinFormat {
	return med.m.format()
}

func (med *MapEditor) Close(ctx context.Context) error {
	return med.acc.Close(ctx)
}
