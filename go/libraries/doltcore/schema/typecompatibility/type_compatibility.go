// Copyright 2023 Dolthub, Inc.
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

package typecompatibility

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	storetypes "github.com/dolthub/dolt/go/store/types"
)

// TypeCompatibilityChecker checks if type changes are compatible at the storage layer and is used to
// determine if a merge with a type change can be safely automatically resolved. Type change compatibility
// means the two types can be stored in a specific storage format without needing to rewrite all the data
// in a table and without corrupting any other data in the table. You must use a TypeCompatibilityChecker
// instance that is specific to the storage format you are using.
type TypeCompatibilityChecker interface {
	// IsTypeChangeCompatible returns two boolean response params. The first boolean indicates if the change from
	// |from| to |to| is a compatible type change, meaning that table data can safely be converted to the new type
	// (e.g. varchar(100) to varchar(200)). The second boolean indicates if a full table rewrite is needed to
	// convert the existing rows into the new schema.
	//
	// For the DOLT storage format, very few cases (outside of the types being exactly identical) are considered
	// compatible without requiring a full table rewrite. The older LD_1 storage format, has a more forgiving storage
	// layout, so more type changes are considered compatible, generally as long as they are in the same type family/kind.
	IsTypeChangeCompatible(from, to typeinfo.TypeInfo) TypeChangeInfo
}

// NewTypeCompatabilityCheckerForStorageFormat returns a new TypeCompatibilityChecker
// instance for the given storage format.
func NewTypeCompatabilityCheckerForStorageFormat(format *storetypes.NomsBinFormat) TypeCompatibilityChecker {
	switch format {
	case storetypes.Format_DOLT:
		return newDoltTypeCompatibilityChecker()
	case storetypes.Format_LD_1:
		return ld1TypeCompatibilityChecker{}
	default:
		panic("unsupported storage format: " + format.VersionString())
	}
}

// ld1TypeCompatibilityChecker implements TypeCompatibilityChecker for the deprecated LD_1
// storage format. This type should never be directly instantiated; use
// newTypeCompatabilityCheckerForStorageFormat instead.
type ld1TypeCompatibilityChecker struct{}

var _ TypeCompatibilityChecker = ld1TypeCompatibilityChecker{}

// IsTypeChangeCompatible implements TypeCompatibilityChecker.IsTypeChangeCompatible for the
// deprecated LD_1 storage format.
func (l ld1TypeCompatibilityChecker) IsTypeChangeCompatible(from, to typeinfo.TypeInfo) (res TypeChangeInfo) {
	// If the types are exactly identical, then they are always compatible
	fromSqlType := from.ToSqlType()
	toSqlType := to.ToSqlType()
	if fromSqlType.Equals(toSqlType) {
		res.Compatible = true
		return res
	}

	// For the older, LD_1 storage format, our compatibility rules are looser
	if from.NomsKind() != to.NomsKind() {
		return res
	}

	if to.ToSqlType().Type() == query.Type_GEOMETRY {
		// We need to do this because some spatial type changes require a full table check, but not all.
		// TODO: This could be narrowed down to a smaller set of spatial type changes
		return res
	}

	res.Compatible = true
	return res
}

// doltTypeCompatibilityChecker implements TypeCompatibilityChecker for the DOLT storage
// format. This type should never be directly instantiated; use
// newTypeCompatabilityCheckerForStorageFormat instead
type doltTypeCompatibilityChecker struct {
	checkers []typeChangeHandler
}

// newDoltTypeCompatibilityChecker creates a new TypeCompatibilityChecker for the DOLT storage engine,
// pre-configured with all supported type change handlers.
func newDoltTypeCompatibilityChecker() TypeCompatibilityChecker {
	return &doltTypeCompatibilityChecker{
		checkers: []typeChangeHandler{
			stringTypeChangeHandler{},
			enumTypeChangeHandler{},
			setTypeChangeHandler{},
		},
	}
}

var _ TypeCompatibilityChecker = (*doltTypeCompatibilityChecker)(nil)

// IsTypeChangeCompatible implements TypeCompatibilityChecker.IsTypeChangeCompatible for the
// DOLT storage format.
func (d doltTypeCompatibilityChecker) IsTypeChangeCompatible(from, to typeinfo.TypeInfo) (res TypeChangeInfo) {
	// If the types are exactly identical, then they are always compatible
	fromSqlType := from.ToSqlType()
	toSqlType := to.ToSqlType()
	if fromSqlType.Equals(toSqlType) {
		res.Compatible = true
		return res
	}

	// The TypeCompatibility checkers don't support ExtendedTypes added by integrators, so if we see
	// one, return early and report the types are not compatible.
	_, fromExtendedType := fromSqlType.(types.ExtendedType)
	_, toExtendedType := toSqlType.(types.ExtendedType)
	if fromExtendedType || toExtendedType {
		return res
	}

	for _, checker := range d.checkers {
		if checker.canHandle(fromSqlType, toSqlType) {
			subcheckerResult := checker.isCompatible(fromSqlType, toSqlType)
			if subcheckerResult.Compatible {
				return subcheckerResult
			}
		}
	}

	return res
}

// TypeChangeInfo contains details about how a column's type changing during the merge impacts the merge.
// |Compatible| stores whether the merge is still possible.
// |RewriteRows| stores whether the primary index will need to be rewritten.
// |InvalidateSecondaryIndexes| stores whether all secondary indexes will need to be rewritten.
// Typically adding removing, or changing the type of columns will trigger a rewrite of all indexes, because it is
// nontrivial to determine which secondary indexes have been invalidated. However, some changes do not affect the
// primary index, such as collation changes to non-pk columns.
type TypeChangeInfo struct {
	Compatible, RewriteRows, InvalidateSecondaryIndexes bool
}

// typeChangeHandler has the logic to determine if a specific change from one type to another is a compatible
// type change that can be automatically converted, possibly also requiring a full table rewrite to update data.
type typeChangeHandler interface {
	// canHandle returns true if this typeChangeHandler is able to check type change compatibility for the specified
	// |from| and |to| sql.Type instances.
	canHandle(fromSqlType, toSqlType sql.Type) bool
	// isCompatible returns two booleans – the first boolean response parameter indicates if a type change from
	// |fromType| to |toType| is compatible and safe to perform automatically. The second boolean response parameter
	// indicates if the type conversion requires a full table rewrite.
	isCompatible(fromSqlType, toSqlType sql.Type) TypeChangeInfo
}

// stringTypeChangeHandler handles type change compatibility between from string types, i.e. VARCHAR, VARBINARY,
// CHAR, BINARY, TEXT, and BLOB types.
type stringTypeChangeHandler struct{}

var _ typeChangeHandler = (*stringTypeChangeHandler)(nil)

func (s stringTypeChangeHandler) canHandle(fromSqlType, toSqlType sql.Type) bool {
	switch fromSqlType.Type() {
	case query.Type_VARCHAR, query.Type_CHAR, query.Type_TEXT:
		switch toSqlType.Type() {
		case query.Type_VARCHAR, query.Type_CHAR, query.Type_TEXT:
			return true
		default:
			return false
		}
	case query.Type_VARBINARY, query.Type_BINARY, query.Type_BLOB:
		switch toSqlType.Type() {
		case query.Type_VARBINARY, query.Type_BINARY, query.Type_BLOB:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func (s stringTypeChangeHandler) isCompatible(fromSqlType, toSqlType sql.Type) (res TypeChangeInfo) {
	fromStringType := fromSqlType.(types.StringType)
	toStringType := toSqlType.(types.StringType)

	res.Compatible = toStringType.CharacterSet() == fromStringType.CharacterSet() &&
		toStringType.MaxByteLength() >= fromStringType.MaxByteLength()

	collationChanged := toStringType.Collation() != fromStringType.Collation()
	// If the collation changed, we will need to rebuild any secondary indexes on this column.
	if collationChanged {
		res.InvalidateSecondaryIndexes = true
	}

	if res.Compatible {
		// Because inline string types (e.g. VARCHAR, CHAR) have the same encoding, the main case
		// when a table rewrite is required is when moving between an inline string type (e.g. CHAR)
		// and an out-of-band string type (e.g. TEXT).
		fromTypeOutOfBand := outOfBandType(fromSqlType)
		toTypeOutOfBand := outOfBandType(toSqlType)
		if fromTypeOutOfBand != toTypeOutOfBand {
			res.RewriteRows = true
			res.InvalidateSecondaryIndexes = true
		}

		// The exception to this is when converting to a fixed width BINARY(N) field, which requires rewriting the
		// table. This is due to MySQL's handling of BINARY(N) field conversion – any existing values in the table,
		// or its indexes, need to be right padded up to N bytes. Note that MySQL does NOT do a similar conversion
		// when converting to VARBINARY(N).
		if toSqlType.Type() == sqltypes.Binary {
			res.RewriteRows = true
			res.InvalidateSecondaryIndexes = true
		}
	}

	return res
}

// outOfBandType returns true if the specified type |t| is stored outside of a table's index file, for example
// TINYTEXT, TEXT, BLOB, etc.
func outOfBandType(t sql.Type) bool {
	switch t.Type() {
	case sqltypes.Blob, sqltypes.Text:
		return true
	default:
		return false
	}
}

// enumTypeChangeHandler handles type change compatibility checking for changes to an enum type. If a new enum value
// is added to the end of the enum, then the type change is considered compatible and does not require a table rewrite.
type enumTypeChangeHandler struct{}

var _ typeChangeHandler = (*enumTypeChangeHandler)(nil)

// canHandle implements the typeChangeHandler interface.
func (e enumTypeChangeHandler) canHandle(from, to sql.Type) bool {
	return types.IsEnum(from) && types.IsEnum(to)
}

// isCompatible implements the typeChangeHandler interface.
func (e enumTypeChangeHandler) isCompatible(fromSqlType, toSqlType sql.Type) (res TypeChangeInfo) {
	fromEnumType := fromSqlType.(sql.EnumType)
	toEnumType := toSqlType.(sql.EnumType)
	if fromEnumType.NumberOfElements() > toEnumType.NumberOfElements() {
		return res
	}

	// TODO: charset/collation changes may require a table or index rewrite; for now, consider them incompatible
	fromCharSet, fromCollation := fromEnumType.CharacterSet(), fromEnumType.Collation()
	toCharSet, toCollation := toEnumType.CharacterSet(), toEnumType.Collation()
	if fromCharSet != toCharSet || fromCollation != toCollation {
		return res
	}

	// if values have only been added at the end, consider it compatible (i.e. no reordering or removal)
	toEnumValues := toEnumType.Values()
	for i, fromEnumValue := range fromEnumType.Values() {
		if toEnumValues[i] != fromEnumValue {
			return res
		}
	}

	// MySQL uses 1 byte to store enum values that have <= 255 values, and 2 bytes for > 255 values
	// The DOLT storage format *always* uses 2 bytes for all enum values, so table data never needs
	// to be rewritten in this additive case.
	res.Compatible = true
	return res
}

// setTypeChangeHandler handles type change compatibility checking for changes to set types. If a new set value
// is added to the end of the set values, then the change is considered compatible and does not require a table
// rewrite.
type setTypeChangeHandler struct{}

var _ typeChangeHandler = (*setTypeChangeHandler)(nil)

// canHandle implements the typeChangeHandler interface.
func (s setTypeChangeHandler) canHandle(fromType, toType sql.Type) bool {
	return types.IsSet(fromType) && types.IsSet(toType)
}

// isCompatible implements the typeChangeHandler interface.
func (s setTypeChangeHandler) isCompatible(fromType, toType sql.Type) (res TypeChangeInfo) {
	fromSetType := fromType.(sql.SetType)
	toSetType := toType.(sql.SetType)
	if fromSetType.NumberOfElements() > toSetType.NumberOfElements() {
		return res
	}

	// TODO: charset/collation changes may require a table or index rewrite; for now, consider them incompatible
	fromCharSet, fromCollation := fromSetType.CharacterSet(), fromSetType.Collation()
	toCharSet, toCollation := toSetType.CharacterSet(), toSetType.Collation()
	if fromCharSet != toCharSet || fromCollation != toCollation {
		return res
	}

	// Ensure only new values have been added to the end of the set
	toSetValues := toSetType.Values()
	for i, fromSetValue := range fromSetType.Values() {
		if toSetValues[i] != fromSetValue {
			return res
		}
	}

	// The DOLT storage format *always* uses 8 bytes for all set values, so the table data never needs
	// to be rewritten in this additive case.
	res.Compatible = true
	return res
}
