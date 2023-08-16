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

package merge

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
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
	// IsTypeChangeCompatible returns true if the change from |from| to |to| is a compatible type change, meaning
	// table data does not need to be rewritten and no data corruption or overwriting will occur.
	// For the DOLT storage format, very few cases (outside of the types being exactly identical) are considered
	// compatible, but we can widen them over time to optimize additional type changes that can be automatically
	// merged. The older LD_1 storage format, has a more forgiving storage layout, so more type changes are
	// considered compatible, generally as long as they are in the same type family/kind.
	// TODO: Update godocs for return param change
	// TODO: Maybe return a code instead of two booleans?
	IsTypeChangeCompatible(from, to typeinfo.TypeInfo) (bool, bool)
}

// newTypeCompatabilityCheckerForStorageFormat returns a new TypeCompatibilityChecker
// instance for the given storage format.
func newTypeCompatabilityCheckerForStorageFormat(format *storetypes.NomsBinFormat) TypeCompatibilityChecker {
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
func (l ld1TypeCompatibilityChecker) IsTypeChangeCompatible(from, to typeinfo.TypeInfo) (bool, bool) {
	// If the types are exactly identical, then they are always compatible
	fromSqlType := from.ToSqlType()
	toSqlType := to.ToSqlType()
	if fromSqlType.Equals(toSqlType) {
		return true, false
	}

	// For the older, LD_1 storage format, our compatibility rules are looser
	if from.NomsKind() != to.NomsKind() {
		return false, false
	}

	if to.ToSqlType().Type() == query.Type_GEOMETRY {
		// We need to do this because some spatial type changes require a full table check, but not all.
		// TODO: This could be narrowed down to a smaller set of spatial type changes
		return false, false
	}

	return true, false
}

// doltTypeCompatibilityChecker implements TypeCompatibilityChecker for the DOLT storage
// format. This type should never be directly instantiated; use
// newTypeCompatabilityCheckerForStorageFormat instead
type doltTypeCompatibilityChecker struct {
	checkers []typeChangeHandler
}

func newDoltTypeCompatibilityChecker() TypeCompatibilityChecker {
	return &doltTypeCompatibilityChecker{
		checkers: []typeChangeHandler{
			varcharToVarcharTypeChangeHandler{},
			stringToBlobTypeChangeHandler{},
			enumTypeChangeHandler{},
			setTypeChangeHandler{},
		},
	}
}

var _ TypeCompatibilityChecker = (*doltTypeCompatibilityChecker)(nil)

// IsTypeChangeCompatible implements TypeCompatibilityChecker.IsTypeChangeCompatible for the
// DOLT storage format.
// TODO: Explain new return param
func (d doltTypeCompatibilityChecker) IsTypeChangeCompatible(from, to typeinfo.TypeInfo) (bool, bool) {
	// If the types are exactly identical, then they are always compatible
	fromSqlType := from.ToSqlType()
	toSqlType := to.ToSqlType()
	if fromSqlType.Equals(toSqlType) {
		return true, false
	}

	for _, checker := range d.checkers {
		if checker.canHandle(fromSqlType, toSqlType) {
			compatible, requiresRewrite := checker.isCompatible(fromSqlType, toSqlType)
			if compatible {
				return compatible, requiresRewrite
			}
		}
	}

	return false, false
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
	isCompatible(fromSqlType, toSqlType sql.Type) (bool, bool)
}

// varcharToVarcharTypeChangeHandler handles compatibility checking when a varchar type is changed to a new varchar
// type (e.g. VARCHAR(20) -> VARCHAR(100))
type varcharToVarcharTypeChangeHandler struct{}

var _ typeChangeHandler = (*varcharToVarcharTypeChangeHandler)(nil)

func (v varcharToVarcharTypeChangeHandler) canHandle(fromSqlType, toSqlType sql.Type) bool {
	return fromSqlType.Type() == query.Type_VARCHAR && toSqlType.Type() == query.Type_VARCHAR
}

func (v varcharToVarcharTypeChangeHandler) isCompatible(fromSqlType, toSqlType sql.Type) (bool, bool) {
	fromStringType := fromSqlType.(types.StringType)
	toStringType := toSqlType.(types.StringType)
	// Varchar data is stored directly in the index, in a variable length field that includes
	// the data's length, so widening the type doesn't require a rewrite and doesn't affect
	// any existing data.
	return toStringType.Length() >= fromStringType.Length() &&
		toStringType.Collation() == fromStringType.Collation(), false
}

// stringToBlobTypeChangeHandler handles type change compatibility checking when a CHAR or VARCHAR column is changed to
// a TEXT type, and also when a BINARY or VARBINARY column is changed to a BLOB type.
type stringToBlobTypeChangeHandler struct{}

var _ typeChangeHandler = (*stringToBlobTypeChangeHandler)(nil)

func (s stringToBlobTypeChangeHandler) canHandle(fromSqlType, toSqlType sql.Type) bool {
	if (fromSqlType.Type() == query.Type_VARCHAR || fromSqlType.Type() == query.Type_CHAR) && toSqlType.Type() == query.Type_TEXT {
		return true
	}

	// BINARY andVARBINARY types can be converted to BLOB types
	if (fromSqlType.Type() == query.Type_VARBINARY || fromSqlType.Type() == query.Type_BINARY) && toSqlType.Type() == query.Type_BLOB {
		return true
	}

	return false
}

func (s stringToBlobTypeChangeHandler) isCompatible(fromSqlType, toSqlType sql.Type) (bool, bool) {
	fromStringType, ok := fromSqlType.(sql.StringType)
	if !ok {
		// TODO: we need to return an error here!
		panic(fmt.Sprintf("unexpected type: %T", fromSqlType))
		return false, false
	}

	toStringType, ok := toSqlType.(sql.StringType)
	if !ok {
		// TODO: we need to return an error here!
		panic(fmt.Sprintf("unexpected type: %T", toSqlType))
		return false, false
	}

	// If the current type has a longer length setting than the new type, disallow the automatic conversion
	if fromStringType.Length() > toStringType.Length() {
		// TODO: As a future optimization, we could check the data to see if it would fit in the new type and
		//       accept the type conversion if all data fits in the new type.
		return false, false
	}
	if toStringType.Collation() != fromStringType.Collation() {
		// TODO: If the charsets are different, we need to re-encode the data and rewrite the table
		// TODO: Although... if the collation changes... that could change the sorting order... so that would
		//       also require a rewrite, right?
		return false, false
	}

	return true, true
}

// enumTypeChangeHandler handles type change compatibility checking for changes to an enum type. If a new enum value
// is added to the end of the enum, then the type change is considered compatible and does not require a table rewrite.
type enumTypeChangeHandler struct{}

var _ typeChangeHandler = (*enumTypeChangeHandler)(nil)

func (e enumTypeChangeHandler) canHandle(from, to sql.Type) bool {
	return types.IsEnum(from) && types.IsEnum(to)
}

func (e enumTypeChangeHandler) isCompatible(fromSqlType, toSqlType sql.Type) (bool, bool) {
	fromEnumType := fromSqlType.(sql.EnumType)
	toEnumType := toSqlType.(sql.EnumType)
	if fromEnumType.NumberOfElements() > toEnumType.NumberOfElements() {
		return false, false
	}

	// TODO: charset/collation changes may require a table or index rewrite; for now, consider them incompatible
	fromCharSet, fromCollation := fromEnumType.CharacterSet(), fromEnumType.Collation()
	toCharSet, toCollation := toEnumType.CharacterSet(), toEnumType.Collation()
	if fromCharSet != toCharSet || fromCollation != toCollation {
		return false, false
	}

	// if values have only been added at the end, consider it compatible (i.e. no reordering or removal)
	toEnumValues := toEnumType.Values()
	for i, fromEnumValue := range fromEnumType.Values() {
		if toEnumValues[i] != fromEnumValue {
			return false, false
		}
	}

	// MySQL uses 1 byte to store enum values that have <= 255 values, and 2 bytes for > 255 values
	// The DOLT storage format *always* uses 2 bytes for all enum values, so table data never needs
	// to be rewritten in this additive case.
	return true, false
}

// setTypeChangeHandler handles type change compatibility checking for changes to set types. If a new set value
// is added to the end of the set values, then the change is considered compatible and does not require a table
// rewrite.
type setTypeChangeHandler struct{}

var _ typeChangeHandler = (*setTypeChangeHandler)(nil)

func (s setTypeChangeHandler) canHandle(fromType, toType sql.Type) bool {
	return types.IsSet(fromType) && types.IsSet(toType)
}

func (s setTypeChangeHandler) isCompatible(fromType, toType sql.Type) (bool, bool) {
	fromSetType := fromType.(sql.SetType)
	toSetType := toType.(sql.SetType)
	if fromSetType.NumberOfElements() > toSetType.NumberOfElements() {
		return false, false
	}

	// TODO: charset/collation changes may require a table or index rewrite; for now, consider them incompatible
	fromCharSet, fromCollation := fromSetType.CharacterSet(), fromSetType.Collation()
	toCharSet, toCollation := toSetType.CharacterSet(), toSetType.Collation()
	if fromCharSet != toCharSet || fromCollation != toCollation {
		return false, false
	}

	// Ensure only new values have been added to the end of the set
	toSetValues := toSetType.Values()
	for i, fromSetValue := range fromSetType.Values() {
		if toSetValues[i] != fromSetValue {
			return false, false
		}
	}

	// The DOLT storage format *always* uses 8 bytes for all set values, so the table data never needs
	// to be rewritten in this additive case.
	return true, false
}
