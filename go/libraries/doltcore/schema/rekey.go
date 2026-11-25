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

// Rekey returns |sch| with the primary key of |target|, or false if |sch|
// cannot be rekeyed to |target|.
//
// Primary-key columns in SQL cannot be NULL, so any column promoted into the
// primary key gains an explicit NOT NULL constraint (matching ALTER TABLE ...
// ADD PRIMARY KEY).
//
// Each primary-key column adopts |target|'s type so that binary-coercible
// widenings (such as VARCHAR(10) to VARCHAR(20)) produce a schema whose key
// types match |target| during subsequent diffs and merges.
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
		if !ok || col.Virtual || !isBinaryCoercible(col, tc) {
			return nil, false, nil
		}
		keyPos[col.Tag] = i
	}

	newCols := sch.GetAllCols().GetColumns()
	pkOrdinals := make([]int, targetPK.Size())
	for i := range newCols {
		col := &newCols[i]
		pos, inPK := keyPos[col.Tag]
		if inPK {
			// Check nullability before setting IsPartOfPK, which makes
			// IsNullable return false.
			if col.IsNullable() {
				col.Constraints = append(slices.Clone(col.Constraints), NotNullConstraint{})
			}
			col.TypeInfo = targetPK.GetByIndex(pos).TypeInfo
			pkOrdinals[pos] = i
		}
		col.IsPartOfPK = inPK
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

// isBinaryCoercible reports whether |from| is binary-coercible to |to|.
func isBinaryCoercible(from, to Column) bool {
	if from.TypeInfo.Encoding() != to.TypeInfo.Encoding() {
		return false
	}
	fromType := from.TypeInfo.ToSqlType()
	toType := to.TypeInfo.ToSqlType()
	if fromType.Equals(toType) {
		return true
	}
	// VARCHAR stores variable-length bytes with no max-length padding, and key
	// order depends only on collation, so a wider VARCHAR reads the same bytes
	// and sorts identically.
	fromStr, ok1 := fromType.(sql.StringType)
	toStr, ok2 := toType.(sql.StringType)
	return ok1 && ok2 &&
		fromStr.Type() == sqltypes.VarChar &&
		toStr.Type() == sqltypes.VarChar &&
		fromStr.Collation().Equals(toStr.Collation()) &&
		fromStr.MaxCharacterLength() <= toStr.MaxCharacterLength()
}
