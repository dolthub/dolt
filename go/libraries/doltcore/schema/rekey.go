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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/sqltypes"
)

// Rekey returns |sch| with the primary key of |target|.
//
// It returns false when |sch| has a full text, vector or spatial index, or
// when a key column of |target| is missing from |sch|, is virtual, or has a
// type that cannot be rekeyed. Key columns match as
// [ColCollection.MatchingColumn] describes.
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
	// keyTags holds the tags in |sch| of the key columns, in key order, and
	// targetCols the target column that each of those tags takes.
	keyTags := make([]uint64, targetPK.Size())
	targetCols := make(map[uint64]Column, targetPK.Size())
	for i, tc := range targetPK.GetColumns() {
		col, ok := sch.GetAllCols().MatchingColumn(target.GetAllCols(), tc)
		if !ok || col.Virtual || !rekeyableType(col, tc) {
			return nil, false, nil
		}
		keyTags[i] = col.Tag
		targetCols[col.Tag] = tc
	}

	cols := sch.GetAllCols().GetColumns()
	newCols := make([]Column, len(cols))
	for i, col := range cols {
		tc, inPK := targetCols[col.Tag]
		wasNullable := col.IsNullable()
		col.IsPartOfPK = inPK
		if inPK {
			col.TypeInfo = tc.TypeInfo
		}
		if inPK && wasNullable {
			col.Constraints = append(append([]ColConstraint(nil), col.Constraints...), NotNullConstraint{})
		}
		newCols[i] = col
	}
	allCols := NewColCollection(newCols...)
	pkOrdinals := make([]int, len(keyTags))
	for i, tag := range keyTags {
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

// rekeyableType reports whether the key column |from| can be rekeyed to |to|
// by copying its stored bytes.
func rekeyableType(from, to Column) bool {
	if from.TypeInfo.ToSqlType().Equals(to.TypeInfo.ToSqlType()) {
		return true
	}
	// Widening a VARCHAR with the same encoding and collation preserves raw
	// key bytes and keeps every existing value valid in the wider type.
	fromStr, ok1 := from.TypeInfo.ToSqlType().(sql.StringType)
	toStr, ok2 := to.TypeInfo.ToSqlType().(sql.StringType)
	if !ok1 || !ok2 {
		return false
	}
	isVarChar := fromStr.Type() == sqltypes.VarChar && toStr.Type() == sqltypes.VarChar
	sameFormat := from.TypeInfo.Encoding() == to.TypeInfo.Encoding() &&
		fromStr.Collation().Equals(toStr.Collation())
	return isVarChar && sameFormat && fromStr.MaxCharacterLength() <= toStr.MaxCharacterLength()
}
