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

package doltdb

import (
	"context"

	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	noteStructName = "note"

	noteRefKey       = "note_ref"
	noteTextKey      = "note_text_ref"
	conflictNotesKey = "conflict_notes"
)

// Note is a struct which holds note text.
type Note struct {
	vrw        types.ValueReadWriter
	noteStruct types.Struct
}

func NewNote(ctx context.Context, vrw types.ValueReadWriter, name types.Value, text types.Value) (*Note, error) {
	noteRef, err := writeValAndGetRef(ctx, vrw, name)

	if err != nil {
		return nil, err
	}

	noteTextRef, err := writeValAndGetRef(ctx, vrw, text)

	sd := types.StructData{
		noteRefKey:  noteRef,
		noteTextKey: noteTextRef,
	}

	noteStruct, err := types.NewStruct(vrw.Format(), noteStructName, sd)

	if err != nil {
		return nil, err
	}

	return &Note{vrw, noteStruct}, nil
}

func (n *Note) Format() *types.NomsBinFormat {
	return n.vrw.Format()
}

func (n *Note) SetConflicts(ctx context.Context, notes Conflict, conflictData types.Map) (*Note, error) {
	conflictsRef, err := writeValAndGetRef(ctx, n.vrw, conflictData)

	if err != nil {
		return nil, err
	}

	ntl, err := notes.ToNomsList(n.vrw)

	if err != nil {
		return nil, err
	}

	updatedSt, err := n.noteStruct.Set(conflictNotesKey, ntl)

	if err != nil {
		return nil, err
	}

	updatedSt, err = updatedSt.Set(conflictsKey, conflictsRef)

	if err != nil {
		return nil, err
	}

	return &Note{n.vrw, updatedSt}, nil
}

func (n *Note) HasConflicts() (bool, error) {
	if n == nil {
		return false, nil
	}

	_, ok, err := n.noteStruct.MaybeGet(conflictNotesKey)

	return ok, err
}
