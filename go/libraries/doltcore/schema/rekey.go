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

import (
	"slices"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
)

// Rekey returns |sch| with the primary key of |target|.
//
// It returns false when |sch| has a full-text, vector, or spatial index, or
// when a key column of |target| is missing from |sch|, is virtual, or has an
// incompatible type. Key columns match as [ColCollection.MatchColumn]
// describes.
//
// A column that joins the primary key becomes NOT NULL, as ALTER TABLE ... ADD
// PRIMARY KEY makes it. A key column takes the type of |target|, so that its
// stored bytes decode with that type.
func Rekey(sch, target Schema) (Schema, bool, error) {
	for _, index := range sch.Indexes().AllIndexes() {
		if index.IsFullText() || index.IsVector() || index.IsSpatial() {
			return nil, false, nil
		}
	}
	targetPK := target.GetPKCols()
	keyPos := make(map[uint64]int, targetPK.Size())
	for i, tc := range targetPK.cols {
		col, ok := sch.GetAllCols().MatchColumn(target.GetAllCols(), tc)
		if !ok || col.Virtual || !compatibleKeyType(col, tc) {
			return nil, false, nil
		}
		keyPos[col.Tag] = i
	}

	cols := sch.GetAllCols().cols
	newCols := make([]Column, len(cols))
	pkOrdinals := make([]int, targetPK.Size())
	for i, col := range cols {
		pos, inPK := keyPos[col.Tag]
		if inPK {
			// Check nullability before setting IsPartOfPK, which makes IsNullable false.
			if col.IsNullable() {
				col.Constraints = append(slices.Clone(col.Constraints), NotNullConstraint{})
			}
			col.TypeInfo = targetPK.GetByIndex(pos).TypeInfo
			pkOrdinals[pos] = i
		}
		col.IsPartOfPK = inPK
		newCols[i] = col
	}

	var checks CheckCollection
	if sch.Checks() != nil {
		checks = sch.Checks().Copy()
	}
	newSch, err := NewSchema(NewColCollection(newCols...), pkOrdinals, sch.GetCollation(), nil, checks)
	if err != nil {
		return nil, false, err
	}
	newSch.SetComment(sch.GetComment())
	newSch.SetTargetRowSize(sch.GetTargetRowSize())
	for _, index := range sch.Indexes().AllIndexes() {
		newSch.Indexes().AddIndex(index)
	}
	return newSch, true, nil
}

// compatibleKeyType reports whether the key column |from| can be copied as raw
// bytes into the key column |to|.
func compatibleKeyType(from, to Column) bool {
	if from.TypeInfo.Encoding() != to.TypeInfo.Encoding() {
		return false
	}
	fromType := from.TypeInfo.ToSqlType()
	toType := to.TypeInfo.ToSqlType()
	if fromType.Equals(toType) {
		return true
	}
	// Widening a VARCHAR with the same encoding and collation preserves raw
	// key bytes and keeps every existing value valid in the wider type.
	fromStr, ok1 := fromType.(sql.StringType)
	toStr, ok2 := toType.(sql.StringType)
	return ok1 && ok2 &&
		fromStr.Type() == sqltypes.VarChar &&
		toStr.Type() == sqltypes.VarChar &&
		fromStr.Collation().Equals(toStr.Collation()) &&
		fromStr.MaxCharacterLength() <= toStr.MaxCharacterLength()
}
