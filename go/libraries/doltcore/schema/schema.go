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

package schema

import (
	"strings"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/store/types"
)

// Schema is an interface for retrieving the columns that make up a schema
type Schema interface {
	// GetPKCols gets the collection of columns which make the primary key.
	GetPKCols() *ColCollection

	// GetNonPKCols gets the collection of columns which are not part of the primary key.
	GetNonPKCols() *ColCollection

	// GetAllCols gets the collection of all columns (pk and non-pk)
	GetAllCols() *ColCollection

	// Indexes returns a collection of all indexes on the table that this schema belongs to.
	Indexes() IndexCollection

	// Checks returns a collection of all check constraints on the table that this schema belongs to.
	Checks() CheckCollection

	// GetPkOrdinals returns a slice of the primary key ordering indexes relative to the schema column ordering
	GetPkOrdinals() []int

	// SetPkOrdinals specifies a primary key column ordering
	SetPkOrdinals([]int) error

	// AddColumn adds a column to this schema in the order given and returns the resulting Schema.
	// The new column cannot be a primary key. To alter primary keys, create a new schema with those keys.
	AddColumn(column Column, order *ColumnOrder) (Schema, error)
}

// ColumnOrder is used in ALTER TABLE statements to change the order of inserted / modified columns.
type ColumnOrder struct {
	First       bool   // True if this column should come first
	AfterColumn string // Set to the name of the column after which this column should appear
}

// ColFromTag returns a schema.Column from a schema and a tag
func ColFromTag(sch Schema, tag uint64) (Column, bool) {
	return sch.GetAllCols().GetByTag(tag)
}

// ColFromName returns a schema.Column from a schema from it's name
func ColFromName(sch Schema, name string) (Column, bool) {
	return sch.GetAllCols().GetByName(name)
}

// ExtractAllColNames returns a map of tag to column name, with one map entry for every column in the schema.
func ExtractAllColNames(sch Schema) (map[uint64]string, error) {
	colNames := make(map[uint64]string)
	err := sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		colNames[tag] = col.Name
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return colNames, nil
}

func IsKeyless(sch Schema) bool {
	return sch.GetPKCols().Size() == 0 &&
		sch.GetAllCols().Size() != 0
}

func HasAutoIncrement(sch Schema) (ok bool) {
	_ = sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		if col.AutoIncrement {
			ok = true
			stop = true
		}
		return
	})
	return
}

// SchemasAreEqual tests equality of two schemas.
func SchemasAreEqual(sch1, sch2 Schema) bool {
	if sch1 == nil && sch2 == nil {
		return true
	} else if sch1 == nil || sch2 == nil {
		return false
	}
	colCollIsEqual := ColCollsAreEqual(sch1.GetAllCols(), sch2.GetAllCols())
	if !colCollIsEqual {
		return false
	}
	return sch1.Indexes().Equals(sch2.Indexes())
}

// TODO: this function never returns an error
// VerifyInSchema tests that the incoming schema matches the schema from the original table
// based on the presence of the column name in the original schema.
func VerifyInSchema(inSch, outSch Schema) (bool, error) {
	inSchCols := inSch.GetAllCols()
	outSchCols := outSch.GetAllCols()

	if inSchCols.Size() != outSchCols.Size() {
		return false, nil
	}

	match := true
	err := inSchCols.Iter(func(tag uint64, inCol Column) (stop bool, err error) {
		_, isValid := outSchCols.GetByNameCaseInsensitive(inCol.Name)

		if !isValid {
			match = false
			return true, nil
		}

		return false, nil
	})

	if err != nil {
		return false, err
	}

	return match, nil
}

// GetSharedCols return all columns in the schema that match the names and types given, which are parallel arrays
// specifying columns to match.
func GetSharedCols(schema Schema, cmpNames []string, cmpKinds []types.NomsKind) []Column {
	existingCols := make(map[string]Column)

	var shared []Column
	_ = schema.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		existingCols[col.Name] = col
		return false, nil
	})

	for i, colName := range cmpNames {
		if col, ok := existingCols[colName]; ok {
			if col.Kind == cmpKinds[i] && strings.ToLower(col.Name) == strings.ToLower(cmpNames[i]) {
				shared = append(shared, col)
			}
		}
	}

	return shared
}

// ArePrimaryKeySetsDiffable checks if two schemas are diffable. Assumes the passed in schema are from the same table
// between commits.
func ArePrimaryKeySetsDiffable(fromSch, toSch Schema) bool {
	if fromSch == nil && toSch == nil {
		return false
	} else if fromSch == nil {
		return true
	} else if fromSch.GetAllCols().Size() == 0 {
		// Empty case
		return true
	}

	// Keyless case for comparing
	if IsKeyless(fromSch) && IsKeyless(toSch) {
		return true
	}

	cc1 := fromSch.GetPKCols()
	cc2 := toSch.GetPKCols()

	if cc1.Size() != cc2.Size() {
		return false
	}

	for i := 0; i < cc1.Size(); i++ {
		c1 := cc1.GetAtIndex(i)
		c2 := cc2.GetAtIndex(i)
		if (c1.Tag != c2.Tag) || (c1.IsPartOfPK != c2.IsPartOfPK) {
			return false
		}
	}

	ords1 := fromSch.GetPkOrdinals()
	ords2 := toSch.GetPkOrdinals()
	if ords1 == nil || ords2 == nil || len(ords1) != len(ords2) {
		return false
	}
	for i := 0; i < len(ords1); i++ {
		if ords1[i] != ords2[i] {
			return false
		}
	}

	return true
}

var ErrUsingSpatialKey = errors.NewKind("can't use Spatial Types as Primary Key for table %s")

// IsColSpatialType returns whether a column's type is a spatial type
func IsColSpatialType(c Column) bool {
	return c.TypeInfo.ToSqlType().Type() == query.Type_GEOMETRY
}

// IsUsingSpatialColAsKey is a utility function that checks for any spatial types being used as a primary key
func IsUsingSpatialColAsKey(sch Schema) bool {
	pkCols := sch.GetPKCols()
	cols := pkCols.GetColumns()
	for _, c := range cols {
		if IsColSpatialType(c) {
			return true
		}
	}
	return false
}

// CopyChecksConstraints copies check constraints from the |from| schema to the |to| schema and returns it
func CopyChecksConstraints(from, to Schema) Schema {
	fromSch, toSch := from.(*schemaImpl), to.(*schemaImpl)
	toSch.checkCollection = fromSch.checkCollection
	return toSch
}

// CopyIndexes copies secondary indexes from the |from| schema to the |to| schema and returns it
func CopyIndexes(from, to Schema) Schema {
	fromSch, toSch := from.(*schemaImpl), to.(*schemaImpl)
	toSch.indexCollection = fromSch.indexCollection
	return toSch
}
