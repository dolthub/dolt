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

package schema

// RekeyedSchema returns |sch| with the primary key of |target|, or false when |sch| lacks a column of that key or holds it with another type, or has columns or
// indexes whose rows cannot be rekeyed by copying fields. Columns keep their definitions from |sch|, except
// that a column joining the primary key becomes NOT NULL, as ALTER TABLE ... ADD PRIMARY KEY makes it.
func RekeyedSchema(sch, target Schema) (Schema, bool, error) {
	if sch.GetAllCols().StoredSize() != sch.GetAllCols().Size() {
		// Virtual columns are not stored, and so cannot be rekeyed by copying fields.
		return nil, false, nil
	}
	for _, index := range sch.Indexes().AllIndexes() {
		if index.IsFullText() || index.IsVector() || index.IsSpatial() {
			return nil, false, nil
		}
	}
	targetPK := target.GetPKCols()
	for _, tc := range targetPK.GetColumns() {
		col, ok := sch.GetAllCols().GetByTag(tc.Tag)
		if !ok || !col.TypeInfo.ToSqlType().Equals(tc.TypeInfo.ToSqlType()) {
			return nil, false, nil
		}
	}

	cols := sch.GetAllCols().GetColumns()
	newCols := make([]Column, len(cols))
	for i, col := range cols {
		_, inPK := targetPK.GetByTag(col.Tag)
		col.IsPartOfPK = inPK
		if inPK && col.IsNullable() {
			col.Constraints = append(append([]ColConstraint(nil), col.Constraints...), NotNullConstraint{})
		}
		newCols[i] = col
	}
	allCols := NewColCollection(newCols...)
	pkOrdinals := make([]int, targetPK.Size())
	for i, tag := range targetPK.Tags {
		pkOrdinals[i] = allCols.TagToIdx[tag]
	}
	newSch, err := NewSchema(allCols, pkOrdinals, sch.GetCollation(), nil, sch.Checks())
	if err != nil {
		return nil, false, err
	}
	for _, index := range sch.Indexes().AllIndexes() {
		newSch.Indexes().AddIndex(index)
	}
	return newSch, true, nil
}
