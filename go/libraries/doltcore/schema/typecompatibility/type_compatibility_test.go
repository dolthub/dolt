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
	"context"
	"reflect"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	storetypes "github.com/dolthub/dolt/go/store/types"
)

type typeChangeCompatibilityTest struct {
	name                       string
	from                       typeinfo.TypeInfo
	to                         typeinfo.TypeInfo
	compatible                 bool
	rewrite                    bool
	invalidateSecondaryIndexes bool
}

// Enum test data
var abcEnum = typeinfo.CreateEnumTypeFromSqlEnumType(gmstypes.MustCreateEnumType([]string{"a", "b", "c"}, sql.Collation_Default))
var abcdEnum = typeinfo.CreateEnumTypeFromSqlEnumType(gmstypes.MustCreateEnumType([]string{"a", "b", "c", "d"}, sql.Collation_Default))
var acdEnum = typeinfo.CreateEnumTypeFromSqlEnumType(gmstypes.MustCreateEnumType([]string{"a", "c", "d"}, sql.Collation_Default))
var abcEnumCi = typeinfo.CreateEnumTypeFromSqlEnumType(gmstypes.MustCreateEnumType([]string{"a", "b", "c"}, sql.Collation_utf8mb3_general_ci))
var abcdEnumCi = typeinfo.CreateEnumTypeFromSqlEnumType(gmstypes.MustCreateEnumType([]string{"a", "b", "c", "d"}, sql.Collation_utf8mb3_general_ci))

// Set test data
var abcSet = typeinfo.CreateSetTypeFromSqlSetType(gmstypes.MustCreateSetType([]string{"a", "b", "c"}, sql.Collation_Default))
var abcdSet = typeinfo.CreateSetTypeFromSqlSetType(gmstypes.MustCreateSetType([]string{"a", "b", "c", "d"}, sql.Collation_Default))
var acdSet = typeinfo.CreateSetTypeFromSqlSetType(gmstypes.MustCreateSetType([]string{"a", "c", "d"}, sql.Collation_Default))
var abcSetCi = typeinfo.CreateSetTypeFromSqlSetType(gmstypes.MustCreateSetType([]string{"a", "b", "c"}, sql.Collation_utf8mb3_general_ci))
var abcdSetCi = typeinfo.CreateSetTypeFromSqlSetType(gmstypes.MustCreateSetType([]string{"a", "b", "c", "d"}, sql.Collation_utf8mb3_general_ci))

// Geometry test data
var geo = typeinfo.CreateGeometryTypeFromSqlGeometryType(gmstypes.GeometryType{SRID: uint32(4326), DefinedSRID: true})
var point = typeinfo.CreatePointTypeFromSqlPointType(gmstypes.PointType{SRID: uint32(4326), DefinedSRID: true})

// String type test data
var varchar10 = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 10, sql.Collation_Default))
var varchar10ci = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 10, sql.Collation_utf8mb4_0900_ai_ci))
var varchar10bin = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 10, sql.Collation_utf8mb4_0900_bin))
var varchar10utf16bin = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 10, sql.Collation_utf16_bin))
var varchar20 = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 20, sql.Collation_Default))
var varchar300 = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 300, sql.Collation_Default))
var varchar10BinaryCollation = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 10, sql.Collation_binary))
var tinyText = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.Text, 255, sql.Collation_Default))
var text = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.Text, 65_535, sql.Collation_Default))
var mediumText = typeinfo.CreateVarStringTypeFromSqlType(gmstypes.MustCreateString(sqltypes.Text, 16_777_215, sql.Collation_Default))

// Binary type test data
var varbinary10 = mustCreateType(gmstypes.MustCreateString(sqltypes.VarBinary, 10, sql.Collation_binary))
var blob = mustCreateType(gmstypes.MustCreateString(sqltypes.Blob, 65_535, sql.Collation_binary))
var mediumBlob = mustCreateType(gmstypes.MustCreateBinary(sqltypes.Blob, 16_777_215))

// ExtendedType test data
var extendedTypeInfo = typeinfo.CreateExtendedTypeFromSqlType(extendedType{})

// TestLd1IsTypeChangeCompatible tests that the LD1 TypeCompatibilityChecker implementation
// correctly computes compatibility between types.
func TestLd1IsTypeChangeCompatible(t *testing.T) {
	compatChecker := NewTypeCompatabilityCheckerForStorageFormat(storetypes.Format_LD_1)
	runTypeCompatibilityTests(t, compatChecker, []typeChangeCompatibilityTest{
		{
			name:       "equivalent types are compatible",
			from:       typeinfo.Int64Type,
			to:         typeinfo.Int64Type,
			compatible: true,
		}, {
			name:       "ints: small to large type changes are compatible",
			from:       typeinfo.Int8Type,
			to:         typeinfo.Int16Type,
			compatible: true,
		}, {
			name:       "ints: large to small type changes are compatible",
			from:       typeinfo.Int64Type,
			to:         typeinfo.Int16Type,
			compatible: true,
		}, {
			name:       "enums: additive changes are compatible",
			from:       abcEnum,
			to:         abcdEnum,
			compatible: true,
		}, {
			// NOTE: LD_1 considers these compatible, even though it probably shouldn't. This matches the existing
			//       behavior for LD_1, so we're preserving it, since we don't want to invest more in LD_1.
			name:       "enums: subtractive changes are compatible",
			from:       abcdEnum,
			to:         abcEnum,
			compatible: true,
		}, {
			// NOTE: This should be incompatible, but preserving the existing behavior since we don't want
			//       to invest more in LD_1.
			name:       "enums: collation changes are incompatible",
			from:       abcEnum,
			to:         abcdEnum,
			compatible: true,
		}, {
			name:       "sets: additive set changes are compatible",
			from:       abcSet,
			to:         abcdSet,
			compatible: true,
		}, {
			// NOTE: This should be incompatible, but preserving the existing behavior since we don't want
			//       to invest more in LD_1.
			name:       "sets: subtractive changes are compatible",
			from:       abcdSet,
			to:         abcSet,
			compatible: true,
		}, {
			// NOTE: This should be incompatible, but preserving the existing behavior since we don't want
			//       to invest more in LD_1.
			name:       "sets: collation changes are incompatible",
			from:       abcSet,
			to:         abcdSet,
			compatible: true,
		}, {
			name:       "geometry: identical types are compatible",
			from:       geo,
			to:         geo,
			compatible: true,
		}, {
			name:       "geometry: non-identical types are not compatible",
			from:       geo,
			to:         point,
			compatible: false,
		},
	})
}

// TestDoltIsTypeChangeCompatible tests that the DOLT TypeCompatibilityChecker implementation
// correctly computes compatibility between types.
func TestDoltIsTypeChangeCompatible(t *testing.T) {
	compatChecker := NewTypeCompatabilityCheckerForStorageFormat(storetypes.Format_DOLT)
	runTypeCompatibilityTests(t, compatChecker, []typeChangeCompatibilityTest{
		{
			name:       "equivalent types are compatible",
			from:       typeinfo.Int64Type,
			to:         typeinfo.Int64Type,
			compatible: true,
		}, {
			name:       "int family: small to large type changes are incompatible",
			from:       typeinfo.Int8Type,
			to:         typeinfo.Int16Type,
			compatible: false,
		}, {
			name:       "int family: large to small type changes are incompatible",
			from:       typeinfo.Int64Type,
			to:         typeinfo.Int16Type,
			compatible: false,
		}, {
			name:       "additive enum changes are compatible",
			from:       abcEnum,
			to:         abcdEnum,
			compatible: true,
		}, {
			name:       "subtractive enum changes are incompatible",
			from:       abcdEnum,
			to:         abcEnum,
			compatible: false,
		}, {
			name:       "enum collation changes are incompatible",
			from:       abcEnum,
			to:         abcEnumCi,
			compatible: false,
		}, {
			name:       "additive set changes are compatible",
			from:       abcSet,
			to:         abcdSet,
			compatible: true,
		}, {
			name:       "subtractive set changes are incompatible",
			from:       abcdSet,
			to:         abcSet,
			compatible: false,
		}, {
			name:       "set collation changes are incompatible",
			from:       abcSet,
			to:         abcSetCi,
			compatible: false,
		}, {
			name:       "geometry: identical types are compatible",
			from:       geo,
			to:         geo,
			compatible: true,
		}, {
			name:       "geometry: non-identical types are incompatible",
			from:       geo,
			to:         point,
			compatible: false,
		},

		// Charset changes
		{
			name:       "incompatible: VARCHAR(10) charset change",
			from:       varchar10bin,
			to:         varchar10utf16bin,
			compatible: false,
		},

		// Collation changes
		{
			name:                       "compatible: VARCHAR(10) collation change",
			from:                       varchar10ci,
			to:                         varchar10bin,
			compatible:                 true,
			rewrite:                    false,
			invalidateSecondaryIndexes: true,
		},

		// Type width changes
		{
			name:       "type widening: VARCHAR(10) to VARCHAR(20)",
			from:       varchar10,
			to:         varchar20,
			compatible: true,
			rewrite:    false,
		}, {
			name:       "type narrowing: VARCHAR(20) to VARCHAR(10)",
			from:       varchar20,
			to:         varchar10,
			compatible: false,
		}, {
			name:                       "type widening: VARCHAR to TEXT",
			from:                       varchar10,
			to:                         text,
			compatible:                 true,
			rewrite:                    true,
			invalidateSecondaryIndexes: true,
		}, {
			name:       "type narrowing: TEXT to VARCHAR(10)",
			from:       text,
			to:         varchar10,
			compatible: false,
		}, {
			name:                       "type widening: TINYTEXT to VARCHAR(300)",
			from:                       tinyText,
			to:                         varchar300,
			compatible:                 true,
			rewrite:                    true,
			invalidateSecondaryIndexes: true,
		}, {
			name:                       "type widening: varbinary to BLOB",
			from:                       varbinary10,
			to:                         blob,
			compatible:                 true,
			rewrite:                    true,
			invalidateSecondaryIndexes: true,
		}, {
			name:       "type narrowing: BLOB to varbinary",
			from:       blob,
			to:         varbinary10,
			compatible: false,
		}, {
			name:                       "type widening: TEXT to MEDIUMTEXT",
			from:                       text,
			to:                         mediumText,
			compatible:                 true,
			rewrite:                    false,
			invalidateSecondaryIndexes: false,
		}, {
			name:       "type narrowing: MEDIUMTEXT to TEXT",
			from:       mediumText,
			to:         text,
			compatible: false,
		}, {
			name:                       "type widening: BLOB to MEDIUMBLOB",
			from:                       blob,
			to:                         mediumBlob,
			compatible:                 true,
			rewrite:                    false,
			invalidateSecondaryIndexes: false,
		}, {
			name:       "type narrowing: MEDIUMBLOB to BLOB",
			from:       mediumBlob,
			to:         blob,
			compatible: false,
		}, {
			name:       "type narrowing: VARCHAR(300) to TINYTEXT",
			from:       varchar300,
			to:         tinyText,
			compatible: false,
		},

		// Incompatible types
		{
			name:       "incompatible: VARCHAR(10) collation change",
			from:       varchar10,
			to:         varchar10BinaryCollation,
			compatible: false,
		}, {
			name:       "incompatible: BLOB to TEXT",
			from:       blob,
			to:         text,
			compatible: false,
		}, {
			name:       "incompatible: VARBINARY(10) to TEXT",
			from:       varbinary10,
			to:         text,
			compatible: false,
		},

		// Extended types
		{
			name:       "incompatible: VARBINARY(10) to ExtendedType",
			from:       varbinary10,
			to:         extendedTypeInfo,
			compatible: false,
		},
		{
			name:       "incompatible: ExtendedType to TEXT",
			from:       extendedTypeInfo,
			to:         text,
			compatible: false,
		},
		{
			name:       "incompatible: ExtendedType to ExtendedType",
			from:       extendedTypeInfo,
			to:         extendedTypeInfo,
			compatible: false,
		},
	})
}

func runTypeCompatibilityTests(t *testing.T, compatChecker TypeCompatibilityChecker, tests []typeChangeCompatibilityTest) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compatibilityResults := compatChecker.IsTypeChangeCompatible(tt.from, tt.to)
			assert.Equal(t, tt.compatible, compatibilityResults.Compatible, "expected compatible to be %t, but was %t", tt.compatible, compatibilityResults.Compatible)
			assert.Equal(t, tt.rewrite, compatibilityResults.RewriteRows, "expected rewrite required to be %t, but was %t", tt.rewrite, compatibilityResults.RewriteRows)
			assert.Equal(t, tt.invalidateSecondaryIndexes, compatibilityResults.InvalidateSecondaryIndexes, "expected secondary index rewrite to be %t, but was %t", tt.invalidateSecondaryIndexes, compatibilityResults.InvalidateSecondaryIndexes)

		})
	}
}

// mustCreateType creates a new Dolt TypeInfo instance from the specified sql.Type. If any problems are encountered
// creating the type, this method will panic.
func mustCreateType(sqlType sql.Type) typeinfo.TypeInfo {
	mediumBlob, err := typeinfo.FromSqlType(sqlType)
	if err != nil {
		panic(err)
	}
	return mediumBlob
}

// extendedType is a no-op implementation of gmstypes.ExtendedType, used for testing type compatibility with extended types.
type extendedType struct{}

var _ gmstypes.ExtendedType = extendedType{}

func (e extendedType) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	panic("unimplemented")
}

func (e extendedType) Compare(ctx context.Context, i interface{}, i2 interface{}) (int, error) {
	panic("unimplemented")
}

func (e extendedType) Convert(ctx context.Context, i interface{}) (interface{}, sql.ConvertInRange, error) {
	panic("unimplemented")
}

func (e extendedType) ConvertToType(ctx *sql.Context, typ gmstypes.ExtendedType, val any) (any, error) {
	panic("unimplemented")
}

func (e extendedType) Equals(otherType sql.Type) bool {
	return false
}

func (e extendedType) MaxTextResponseByteLength(ctx *sql.Context) uint32 {
	panic("unimplemented")
}

func (e extendedType) Promote() sql.Type {
	panic("unimplemented")
}

func (e extendedType) SQL(ctx *sql.Context, dest []byte, v interface{}) (sqltypes.Value, error) {
	panic("unimplemented")
}

func (e extendedType) Type() query.Type {
	panic("unimplemented")
}

func (e extendedType) ValueType() reflect.Type {
	panic("unimplemented")
}

func (e extendedType) Zero() interface{} {
	panic("unimplemented")
}

func (e extendedType) String() string {
	panic("unimplemented")
}

func (e extendedType) SerializedCompare(ctx context.Context, v1 []byte, v2 []byte) (int, error) {
	panic("unimplemented")
}

func (e extendedType) SerializeValue(ctx context.Context, val any) ([]byte, error) {
	panic("unimplemented")
}

func (e extendedType) DeserializeValue(ctx context.Context, val []byte) (any, error) {
	panic("unimplemented")
}

func (e extendedType) FormatValue(val any) (string, error) {
	panic("unimplemented")
}

func (e extendedType) MaxSerializedWidth() gmstypes.ExtendedTypeSerializedWidth {
	panic("unimplemented")
}
