// Copyright 2022 Dolthub, Inc.
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

package creation

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

func TestGetIndexKeyMapping(t *testing.T) {
	tests := []struct {
		Name    string
		AllCols []schema.Column
		IdxCols []string
		KeyLen  int
		Mapping val.OrdinalMapping
	}{
		{
			Name: "basic",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, true),
				schema.NewColumn("col2", 1, types.IntKind, false),
			},
			IdxCols: []string{"col2"},
			KeyLen:  1,
			Mapping: []int{1, 0},
		},
		{
			Name: "basic, pk not first",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, true),
			},
			IdxCols: []string{"col1"},
			KeyLen:  1,
			Mapping: []int{1, 0},
		},
		{
			Name: "compound index",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, true),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col2", "col3"},
			KeyLen:  1,
			Mapping: []int{1, 2, 0},
		},
		{
			Name: "compound index reverse",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, true),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col3", "col2"},
			KeyLen:  1,
			Mapping: []int{2, 1, 0},
		},
		{
			Name: "compound index, pk not first",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, true),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col1", "col3"},
			KeyLen:  1,
			Mapping: []int{1, 2, 0},
		},
		{
			Name: "compound index, pk not first, reverse",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, true),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col3", "col1"},
			KeyLen:  1,
			Mapping: []int{2, 1, 0},
		},
		{
			Name: "keyless",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
			},
			IdxCols: []string{"col1"},
			KeyLen:  1,
			Mapping: []int{1, 0},
		},
		{
			Name: "keyless other",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
			},
			IdxCols: []string{"col2"},
			KeyLen:  1,
			Mapping: []int{2, 0},
		},
		{
			Name: "compound keyless",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col2", "col3"},
			KeyLen:  1,
			Mapping: []int{2, 3, 0},
		},
		{
			Name: "compound keyless reverse",
			AllCols: []schema.Column{
				schema.NewColumn("col1", 0, types.IntKind, false),
				schema.NewColumn("col2", 1, types.IntKind, false),
				schema.NewColumn("col3", 2, types.IntKind, false),
			},
			IdxCols: []string{"col3", "col2"},
			KeyLen:  1,
			Mapping: []int{3, 2, 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			sch := schema.MustSchemaFromCols(schema.NewColCollection(tt.AllCols...))

			idxTags := make([]uint64, len(tt.IdxCols))
			for i, name := range tt.IdxCols {
				col := sch.GetAllCols().NameToCol[name]
				idxTags[i] = col.Tag
			}
			allTags := append(idxTags, sch.GetPKCols().Tags...)
			idx := schema.NewIndex("test_idx", idxTags, allTags, nil, schema.IndexProperties{})

			keyLen, mapping := GetIndexKeyMapping(sch, idx)
			require.Equal(t, tt.KeyLen, keyLen)
			require.Equal(t, tt.Mapping, mapping)
		})
	}
}
