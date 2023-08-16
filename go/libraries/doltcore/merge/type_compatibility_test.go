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
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	storetypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
)

type typeChangeCompatibilityTest struct {
	name     string
	from     typeinfo.TypeInfo
	to       typeinfo.TypeInfo
	expected bool
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

// TestLd1IsTypeChangeCompatible tests that the LD1 TypeCompatibilityChecker implementation
// correctly computes compatibility between types.
func TestLd1IsTypeChangeCompatible(t *testing.T) {
	compatChecker := newTypeCompatabilityCheckerForStorageFormat(storetypes.Format_LD_1)
	runTypeCompatibilityTests(t, compatChecker, []typeChangeCompatibilityTest{
		{
			name:     "equivalent types are compatible",
			from:     typeinfo.Int64Type,
			to:       typeinfo.Int64Type,
			expected: true,
		}, {
			name:     "ints: small to large type changes are compatible",
			from:     typeinfo.Int8Type,
			to:       typeinfo.Int16Type,
			expected: true,
		}, {
			name:     "ints: large to small type changes are compatible",
			from:     typeinfo.Int64Type,
			to:       typeinfo.Int16Type,
			expected: true,
		}, {
			name:     "enums: additive changes are compatible",
			from:     abcEnum,
			to:       abcdEnum,
			expected: true,
		}, {
			// NOTE: LD_1 considers these compatible, even though it probably shouldn't. This matches the existing
			//       behavior for LD_1, so we're preserving it, since we don't want to invest more in LD_1.
			name:     "enums: subtractive changes are compatible",
			from:     abcdEnum,
			to:       abcEnum,
			expected: true,
		}, {
			// NOTE: This should be incompatible, but preserving the existing behavior since we don't want
			//       to invest more in LD_1.
			name:     "enums: collation changes are incompatible",
			from:     abcEnum,
			to:       abcdEnum,
			expected: true,
		}, {
			name:     "sets: additive set changes are compatible",
			from:     abcSet,
			to:       abcdSet,
			expected: true,
		}, {
			// NOTE: This should be incompatible, but preserving the existing behavior since we don't want
			//       to invest more in LD_1.
			name:     "sets: subtractive changes are compatible",
			from:     abcdSet,
			to:       abcSet,
			expected: true,
		}, {
			// NOTE: This should be incompatible, but preserving the existing behavior since we don't want
			//       to invest more in LD_1.
			name:     "sets: collation changes are incompatible",
			from:     abcSet,
			to:       abcdSet,
			expected: true,
		}, {
			name:     "geometry: identical types are compatible",
			from:     geo,
			to:       geo,
			expected: true,
		}, {
			name:     "geometry: non-identical types are not compatible",
			from:     geo,
			to:       point,
			expected: false,
		},
	})
}

// TestDoltIsTypeChangeCompatible tests that the DOLT TypeCompatibilityChecker implementation
// correctly computes compatibility between types.
func TestDoltIsTypeChangeCompatible(t *testing.T) {
	compatChecker := newTypeCompatabilityCheckerForStorageFormat(storetypes.Format_DOLT)
	runTypeCompatibilityTests(t, compatChecker, []typeChangeCompatibilityTest{
		{
			name:     "equivalent types are compatible",
			from:     typeinfo.Int64Type,
			to:       typeinfo.Int64Type,
			expected: true,
		}, {
			name:     "int family: small to large type changes are incompatible",
			from:     typeinfo.Int8Type,
			to:       typeinfo.Int16Type,
			expected: false,
		}, {
			name:     "int family: large to small type changes are incompatible",
			from:     typeinfo.Int64Type,
			to:       typeinfo.Int16Type,
			expected: false,
		}, {
			name:     "additive enum changes are compatible",
			from:     abcEnum,
			to:       abcdEnum,
			expected: true,
		}, {
			name:     "subtractive enum changes are incompatible",
			from:     abcdEnum,
			to:       abcEnum,
			expected: false,
		}, {
			name:     "enum collation changes are incompatible",
			from:     abcEnum,
			to:       abcEnumCi,
			expected: false,
		}, {
			name:     "additive set changes are compatible",
			from:     abcSet,
			to:       abcdSet,
			expected: true,
		}, {
			name:     "subtractive set changes are incompatible",
			from:     abcdSet,
			to:       abcSet,
			expected: false,
		}, {
			name:     "set collation changes are incompatible",
			from:     abcSet,
			to:       abcSetCi,
			expected: false,
		}, {
			name:     "geometry: identical types are compatible",
			from:     geo,
			to:       geo,
			expected: true,
		}, {
			name:     "geometry: non-identical types are incompatible",
			from:     geo,
			to:       point,
			expected: false,
		},
	})
}

func runTypeCompatibilityTests(t *testing.T, compatChecker TypeCompatibilityChecker, tests []typeChangeCompatibilityTest) {
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, requiresRewrite := compatChecker.IsTypeChangeCompatible(tt.from, tt.to)
			assert.Equal(t, tt.expected, got)
			assert.False(t, requiresRewrite)
		})
	}
}
