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

package dtestutils

import (
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	IdTag uint64 = iota
	NameTag
	AgeTag
	IsMarriedTag
	TitleTag
	NextTag // leave last
)

const (
	IndexName = "idx_name"
)

// Schema returns the schema for the `people` test table.
func Schema() (schema.Schema, error) {
	var typedColColl = schema.NewColCollection(
		schema.NewColumn("id", IdTag, types.StringKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("name", NameTag, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("age", AgeTag, types.UintKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("is_married", IsMarriedTag, types.IntKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("title", TitleTag, types.StringKind, false),
	)
	sch := schema.MustSchemaFromCols(typedColColl)

	_, err := sch.Indexes().AddIndexByColTags(IndexName, []uint64{NameTag}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
	if err != nil {
		return nil, err
	}

	_, err = sch.Checks().AddCheck("test-check", "age < 123", true)
	if err != nil {
		return nil, err
	}

	return sch, err
}
