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
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	storetypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"
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
	IsTypeChangeCompatible(from, to typeinfo.TypeInfo) bool
}

// newTypeCompatabilityCheckerForStorageFormat returns a new TypeCompatibilityChecker
// instance for the given storage format.
func newTypeCompatabilityCheckerForStorageFormat(format *storetypes.NomsBinFormat) TypeCompatibilityChecker {
	switch format {
	case storetypes.Format_DOLT:
		return doltTypeCompatibilityChecker{}
	case storetypes.Format_LD_1:
		return ld1TypeCompatibilityChecker{}
	default:
		panic("unsupported storage format: " + format.VersionString())
	}
}

// doltTypeCompatibilityChecker implements TypeCompatibilityChecker for the DOLT storage
// format. This type should never be directly instantiated; use
// newTypeCompatabilityCheckerForStorageFormat instead
type doltTypeCompatibilityChecker struct{}

var _ TypeCompatibilityChecker = doltTypeCompatibilityChecker{}

// IsTypeChangeCompatible implements TypeCompatibilityChecker.IsTypeChangeCompatible for the
// DOLT storage format.
func (d doltTypeCompatibilityChecker) IsTypeChangeCompatible(from, to typeinfo.TypeInfo) bool {
	// If the types are exactly identical, then they are always compatible
	fromSqlType := from.ToSqlType()
	toSqlType := to.ToSqlType()
	if fromSqlType.Equals(toSqlType) {
		return true
	}

	switch {
	case types.IsEnum(fromSqlType) && types.IsEnum(toSqlType):
		fromEnumType := fromSqlType.(sql.EnumType)
		toEnumType := toSqlType.(sql.EnumType)
		if fromEnumType.NumberOfElements() > toEnumType.NumberOfElements() {
			return false
		}

		// TODO: charset/collation changes may require a table or index rewrite; for now, consider them incompatible
		fromCharSet, fromCollation := fromEnumType.CharacterSet(), fromEnumType.Collation()
		toCharSet, toCollation := toEnumType.CharacterSet(), toEnumType.Collation()
		if fromCharSet != toCharSet || fromCollation != toCollation {
			return false
		}

		// if values have only been added at the end, consider it compatible (i.e. no reordering or removal)
		toEnumValues := toEnumType.Values()
		for i, fromEnumValue := range fromEnumType.Values() {
			if toEnumValues[i] != fromEnumValue {
				return false
			}
		}

		// MySQL uses 1 byte to store enum values that have <= 255 values, and 2 bytes for > 255 values
		// The DOLT storage format *always* uses 2 bytes for all enum values, so table data never needs
		// to be rewritten in this additive case.
		return true

	case types.IsSet(fromSqlType) && types.IsSet(toSqlType):
		fromSetType := fromSqlType.(sql.SetType)
		toSetType := toSqlType.(sql.SetType)
		if fromSetType.NumberOfElements() > toSetType.NumberOfElements() {
			return false
		}

		// TODO: charset/collation changes may require a table or index rewrite; for now, consider them incompatible
		fromCharSet, fromCollation := fromSetType.CharacterSet(), fromSetType.Collation()
		toCharSet, toCollation := toSetType.CharacterSet(), toSetType.Collation()
		if fromCharSet != toCharSet || fromCollation != toCollation {
			return false
		}

		// Ensure only new values have been added to the end of the set
		toSetValues := toSetType.Values()
		for i, fromSetValue := range fromSetType.Values() {
			if toSetValues[i] != fromSetValue {
				return false
			}
		}

		// The DOLT storage format *always* uses 8 bytes for all set values, so the table data never needs
		// to be rewritten in this additive case.
		return true

	default:
		// Otherwise, all type changes are considered incompatible for the DOLT storage format, and
		// require user intervention to merge them safely.
		return false
	}
}

// ld1TypeCompatibilityChecker implements TypeCompatibilityChecker for the deprecated LD_1
// storage format. This type should never be directly instantiated; use
// newTypeCompatabilityCheckerForStorageFormat instead.
type ld1TypeCompatibilityChecker struct{}

var _ TypeCompatibilityChecker = ld1TypeCompatibilityChecker{}

// IsTypeChangeCompatible implements TypeCompatibilityChecker.IsTypeChangeCompatible for the
// deprecated LD_1 storage format.
func (l ld1TypeCompatibilityChecker) IsTypeChangeCompatible(from, to typeinfo.TypeInfo) bool {
	// If the types are exactly identical, then they are always compatible
	fromSqlType := from.ToSqlType()
	toSqlType := to.ToSqlType()
	if fromSqlType.Equals(toSqlType) {
		return true
	}

	// For the older, LD_1 storage format, our compatibility rules are looser
	if from.NomsKind() != to.NomsKind() {
		return false
	}

	if to.ToSqlType().Type() == query.Type_GEOMETRY {
		// We need to do this because some spatial type changes require a full table check, but not all.
		// TODO: This could be narrowed down to a smaller set of spatial type changes
		return false
	}

	return true
}
