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
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// Schema defines the schema of a table and describes both its SQL schema and storage layout.
//
// For example, a SQL table defined as:
//
//	`CREATE TABLE t (a int, b int, pk2 int, c int, pk1 int, PRIMARY KEY (pk1, pk2));`
//
// Has a corresponding Schema of:
//
//	Schema {
//		PkCols:     [pk1, pk2],
//		NonPkCols:  [a, b, c],
//		AllCols:    [a, b, pk2, c, pk1],
//		PkOrdinals: [4, 2],
//	}
type Schema interface {
	// GetPKCols gets the collection of columns which make the primary key.
	// Columns in this collection are ordered by storage order, which is
	// defined in the 'PRIMARY KEY(...)' clause of a CREATE TABLE statement.
	GetPKCols() *ColCollection

	// GetNonPKCols gets the collection of columns which are not part of the primary key.
	// Columns in this collection are ordered by schema order (display order), which is
	// defined by the order of first occurrence in a CREATE TABLE statement.
	GetNonPKCols() *ColCollection

	// GetAllCols gets the collection of all columns (pk and non-pk)
	// Columns in this collection are ordered by schema order (display order), which is
	// defined by the order of first occurrence in a CREATE TABLE statement.
	GetAllCols() *ColCollection

	// Indexes returns a collection of all indexes on the table that this schema belongs to.
	Indexes() IndexCollection

	// Checks returns a collection of all check constraints on the table that this schema belongs to.
	Checks() CheckCollection

	// GetPkOrdinals returns a slice of schema order positions for the primary key columns. These ith
	// value of this slice contains schema position for the ith column in the PK ColCollection.
	GetPkOrdinals() []int

	// SetPkOrdinals specifies a primary key column ordering. See GetPkOrdinals.
	SetPkOrdinals([]int) error

	// AddColumn adds a column to this schema in the order given and returns the resulting Schema.
	// The new column cannot be a primary key. To alter primary keys, create a new schema with those keys.
	AddColumn(column Column, order *ColumnOrder) (Schema, error)

	// GetMapDescriptors returns the key and value tuple descriptors for this schema.
	GetMapDescriptors() (keyDesc, valueDesc val.TupleDesc)

	// GetKeyDescriptor returns the key tuple descriptor for this schema.
	// If a column has a type that can't appear in a key (such as "address" columns),
	// that column will get converted to equivalent types that can. (Example: text -> varchar)
	GetKeyDescriptor() val.TupleDesc

	// GetKeyDescriptorWithNoConversion returns the a descriptor for the columns used in the key.
	// Unlike `GetKeyDescriptor`, it doesn't attempt to convert columns if they can't appear in a key,
	// and returns them as they are.
	GetKeyDescriptorWithNoConversion() val.TupleDesc

	// GetValueDescriptor returns the value tuple descriptor for this schema.
	GetValueDescriptor() val.TupleDesc

	// GetCollation returns the table's collation.
	GetCollation() Collation

	// SetCollation sets the table's collation.
	SetCollation(collation Collation)

	// GetComment returns the table's comment.
	GetComment() string

	// SetComment sets the table's comment.
	SetComment(comment string)

	// Copy returns a copy of this Schema that can be safely modified independently.
	Copy() Schema
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
	return sch != nil &&
		sch.GetPKCols().Size() == 0 &&
		sch.GetAllCols().Size() != 0
}

func IsVirtual(sch Schema) bool {
	return sch != nil && len(sch.GetAllCols().virtualColumns) > 0
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

// GetAutoIncrementColumn returns the auto increment column if one exists, with an existence boolean
func GetAutoIncrementColumn(sch Schema) (col Column, ok bool) {
	var aiCol Column
	var found bool
	_ = sch.GetAllCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		if col.AutoIncrement {
			aiCol = col
			found = true
			stop = true
		}
		return
	})

	return aiCol, found
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

	// Pks and Non-pks are in the same order as the key tuple and value tuple fields
	if !ColCollsAreEqual(sch1.GetPKCols(), sch2.GetPKCols()) {
		return false
	}

	if !ColCollsAreEqual(sch1.GetNonPKCols(), sch2.GetNonPKCols()) {
		return false
	}

	if sch1.GetCollation() != sch2.GetCollation() {
		return false
	}

	if (sch1.Checks() == nil) != (sch2.Checks() == nil) {
		return false
	}

	if sch1.Checks() != nil && sch2.Checks() != nil &&
		!sch1.Checks().Equals(sch2.Checks()) {
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
			if col.Kind == cmpKinds[i] && strings.EqualFold(col.Name, cmpNames[i]) {
				shared = append(shared, col)
			}
		}
	}

	return shared
}

// ArePrimaryKeySetsDiffable checks if two schemas are diffable. Assumes the
// passed in schema are from the same table between commits. If __DOLT__, then
// it also checks if the underlying SQL types of the columns are equal.
func ArePrimaryKeySetsDiffable(format *types.NomsBinFormat, fromSch, toSch Schema) bool {
	if fromSch == nil && toSch == nil {
		return false
		// Empty case
	} else if fromSch == nil || fromSch.GetAllCols().Size() == 0 ||
		toSch == nil || toSch.GetAllCols().Size() == 0 {
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
		c1 := cc1.GetByIndex(i)
		c2 := cc2.GetByIndex(i)
		if (c1.Tag != c2.Tag) || (c1.IsPartOfPK != c2.IsPartOfPK) {
			return false
		}
		if types.IsFormat_DOLT(format) && !c1.TypeInfo.ToSqlType().Equals(c2.TypeInfo.ToSqlType()) {
			return false
		}
	}

	return true
}

// MapSchemaBasedOnTagAndName can be used to map column values from one schema
// to another schema. A primary key column in |inSch| is mapped to |outSch| if
// they share the same tag. A non-primary key column in |inSch| is mapped to
// |outSch| purely based on the name. It returns ordinal mappings that can be
// use to map key, value val.Tuple's of schema |inSch| to |outSch|. The first
// ordinal map is for keys, and the second is for values. If a column of |inSch|
// is missing in |outSch| then that column's index in the ordinal map holds -1.
func MapSchemaBasedOnTagAndName(inSch, outSch Schema) (val.OrdinalMapping, val.OrdinalMapping, error) {
	keyMapping := make(val.OrdinalMapping, inSch.GetPKCols().Size())
	valMapping := make(val.OrdinalMapping, inSch.GetNonPKCols().Size())

	// if inSch or outSch is empty schema. This can be from added or dropped table.
	if len(inSch.GetAllCols().cols) == 0 || len(outSch.GetAllCols().cols) == 0 {
		return keyMapping, valMapping, nil
	}

	err := inSch.GetPKCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		i := inSch.GetPKCols().TagToIdx[tag]
		if foundCol, ok := outSch.GetPKCols().GetByTag(tag); ok {
			j := outSch.GetPKCols().TagToIdx[foundCol.Tag]
			keyMapping[i] = j
		} else {
			return true, fmt.Errorf("could not map primary key column %s", col.Name)
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}

	err = inSch.GetNonPKCols().Iter(func(tag uint64, col Column) (stop bool, err error) {
		i := inSch.GetNonPKCols().TagToIdx[col.Tag]
		if col, ok := outSch.GetNonPKCols().GetByName(col.Name); ok {
			j := outSch.GetNonPKCols().TagToIdx[col.Tag]
			valMapping[i] = j
		} else {
			valMapping[i] = -1
		}
		return false, nil
	})
	if err != nil {
		return nil, nil, err
	}

	return keyMapping, valMapping, nil
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

// GetKeyColumnTags returns a set.Uint64Set containing the column tags
// of every key column of every primary and secondary index in |sch|.
func GetKeyColumnTags(sch Schema) *set.Uint64Set {
	tags := set.NewUint64Set(sch.GetPKCols().Tags)
	_ = sch.Indexes().Iter(func(index Index) (stop bool, err error) {
		tags.Add(index.IndexedColumnTags()...)
		return
	})
	return tags
}
