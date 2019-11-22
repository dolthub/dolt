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
	docStructName = "doc"

	docRefKey       = "doc_ref"
	docTextKey      = "doc_text_ref"
	conflictDocsKey = "conflict_docs"
)

// Doc is a struct which holds doc text.
type Doc struct {
	vrw        types.ValueReadWriter
	docStruct types.Struct
}

func NewDoc(ctx context.Context, vrw types.ValueReadWriter, name types.Value, text types.Value) (*Doc, error) {
	docRef, err := writeValAndGetRef(ctx, vrw, name)

	if err != nil {
		return nil, err
	}

	docTextRef, err := writeValAndGetRef(ctx, vrw, text)

	sd := types.StructData{
		docRefKey:  docRef,
		docTextKey: docTextRef,
	}

	docStruct, err := types.NewStruct(vrw.Format(), docStructName, sd)

	if err != nil {
		return nil, err
	}

	return &Doc{vrw, docStruct}, nil
}

func (n *Doc) Format() *types.NomsBinFormat {
	return n.vrw.Format()
}

func (n *Doc) SetConflicts(ctx context.Context, docs Conflict, conflictData types.Map) (*Doc, error) {
	conflictsRef, err := writeValAndGetRef(ctx, n.vrw, conflictData)

	if err != nil {
		return nil, err
	}

	dcl, err := docs.ToNomsList(n.vrw)

	if err != nil {
		return nil, err
	}

	updatedSt, err := n.docStruct.Set(conflictDocsKey, dcl)

	if err != nil {
		return nil, err
	}

	updatedSt, err = updatedSt.Set(conflictsKey, conflictsRef)

	if err != nil {
		return nil, err
	}

	return &Doc{n.vrw, updatedSt}, nil
}

func (n *Doc) HasConflicts() (bool, error) {
	if n == nil {
		return false, nil
	}

	_, ok, err := n.docStruct.MaybeGet(conflictDocsKey)

	return ok, err
}
