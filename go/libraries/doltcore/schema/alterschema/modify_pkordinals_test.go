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

package alterschema

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func TestNewPkOrdinals(t *testing.T) {
	oldSch := schema.MustSchemaFromCols(
		schema.NewColCollection(
			schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
			schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
			schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
			schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, true, schema.NotNullConstraint{}),
			schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
		),
	)
	err := oldSch.SetPkOrdinals([]int{3, 1})
	require.NoError(t, err)

	tests := []struct {
		name          string
		newSch        schema.Schema
		expPkOrdinals []int
		err           error
	}{
		{
			name: "remove column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{2, 1},
		},
		{
			name: "add column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("new", dtestutils.NextTag, types.StringKind, false),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{4, 1},
		},
		{
			name: "transpose column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, true, schema.NotNullConstraint{}),
				),
			),
			expPkOrdinals: []int{4, 1},
		},
		{
			name: "transpose PK column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{1, 2},
		},
		{
			name: "drop PK column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			err: ErrPrimaryKeySetsIncompatible,
		},
		{
			name: "add PK column",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.IsMarriedTag, types.BoolKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
					schema.NewColumn("new", dtestutils.NextTag, types.StringKind, true),
				),
			),
			err: ErrPrimaryKeySetsIncompatible,
		},
		{
			name: "change PK tag",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("is_married", dtestutils.NextTag, types.BoolKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{3, 1},
		},
		{
			name: "change PK name",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("new", dtestutils.IsMarriedTag, types.BoolKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			expPkOrdinals: []int{3, 1},
		},
		{
			name: "changing PK tag and name is the same as dropping a PK",
			newSch: schema.MustSchemaFromCols(
				schema.NewColCollection(
					schema.NewColumn("newId", dtestutils.IdTag, types.StringKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("name", dtestutils.NameTag, types.StringKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("age", dtestutils.AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
					schema.NewColumn("new", dtestutils.NextTag, types.BoolKind, true, schema.NotNullConstraint{}),
					schema.NewColumn("title", dtestutils.TitleTag, types.StringKind, false),
				),
			),
			err: ErrPrimaryKeySetsIncompatible,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := modifyPkOrdinals(oldSch, tt.newSch)
			if tt.err != nil {
				require.True(t, errors.Is(err, tt.err))
			} else {
				require.Equal(t, res, tt.expPkOrdinals)
			}
		})
	}
}
