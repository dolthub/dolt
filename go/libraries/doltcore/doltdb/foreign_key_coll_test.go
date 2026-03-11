// Copyright 2026 Dolthub, Inc.
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

import "testing"

func BenchmarkForeignKeyHashOf(b *testing.B) {
	fk := ForeignKey{
		Name: "name",
		TableName: TableName{
			Name:   "tbl_name",
			Schema: "tbl_schema",
		},
		TableIndex:   "tbl_index",
		TableColumns: []uint64{1, 2, 3},
		ReferencedTableName: TableName{
			Name:   "reftbl_name",
			Schema: "reftbl_schema",
		},
		ReferencedTableIndex:   "reftbl_index",
		ReferencedTableColumns: []uint64{1, 2, 3},
		OnUpdate:               ForeignKeyReferentialAction_Cascade,
		OnDelete:               ForeignKeyReferentialAction_Cascade,
		UnresolvedFKDetails: UnresolvedFKDetails{
			TableColumns:           []string{"asdf"},
			ReferencedTableColumns: []string{"asdf"},
		},
	}
	for i := 0; i < b.N; i++ {
		_, _ = fk.HashOf()
	}
}
