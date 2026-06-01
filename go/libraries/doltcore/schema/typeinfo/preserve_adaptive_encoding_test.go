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

package typeinfo

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/val"
)

// TestPreserveAdaptiveEncoding_TextStaysLegacy guards the regression where an ALTER
// MODIFY on a TEXT column that was originally written under address encoding (e.g. a
// table created on Dolt 1.x and modified after upgrading to 2.0.7+) would silently flip
// the persisted storage encoding from StringAddrEnc(23) to StringAdaptiveEnc(135) while
// the on-disk row data was left in the legacy raw-hash layout, eventually panicking
// adaptive readers with `invalid hash length: 19`.
//
// The function under test is the helper that ModifyColumn (and the rewrite-path schema
// builder) call after `FromSqlType` returns a fresh TypeInfo with enc=0. It must
// preserve the original column's persisted encoding even when the global
// UseAdaptiveEncoding flag is true.
func TestPreserveAdaptiveEncoding_TextStaysLegacy(t *testing.T) {
	// Force the global adaptive-encoding default ON, which is the dangerous setting
	// the bug triggered under (Dolt 2.0.7+ default).
	prev := UseAdaptiveEncoding
	UseAdaptiveEncoding = true
	t.Cleanup(func() { UseAdaptiveEncoding = prev })

	// Existing column: TEXT originally persisted under StringAddrEnc (legacy 1.x).
	existing := (&blobStringType{sqlStringType: gmstypes.Text}).
		WithEncoding(val.StringAddrEnc)
	require.Equal(t, val.StringAddrEnc, existing.Encoding(),
		"sanity: existing column should report legacy address encoding")

	// Fresh column from FromSqlType (TEXT → LONGTEXT widening): enc=0, falls back to
	// adaptive under the global flag.
	freshTI, err := FromSqlType(gmstypes.LongText)
	require.NoError(t, err)
	require.Equal(t, val.StringAdaptiveEnc, freshTI.Encoding(),
		"sanity: a fresh TypeInfo with enc=0 must default to adaptive under the global flag — "+
			"this is the dangerous behaviour the fix neutralises")

	// Apply the preservation helper. It must pin the fresh TypeInfo back onto the
	// legacy encoding so the persisted schema is not silently re-tagged.
	preserved := PreserveAdaptiveEncoding(existing, freshTI)
	require.Equal(t, val.StringAddrEnc, preserved.Encoding(),
		"PreserveAdaptiveEncoding must keep the legacy StringAddr encoding on a TEXT→LONGTEXT widening, "+
			"NOT the global StringAdaptive default — otherwise on-disk row data drifts from the schema record")

	// And the preserved TypeInfo must still represent LONGTEXT semantically (the
	// caller only meant to preserve storage encoding, not block the SQL type widening).
	require.Equal(t, gmstypes.LongText.MaxCharacterLength(),
		preserved.ToSqlType().(interface{ MaxCharacterLength() int64 }).MaxCharacterLength(),
		"PreserveAdaptiveEncoding must not regress the new SQL type (LONGTEXT) when pinning the encoding")
}

// TestPreserveAdaptiveEncoding_BlobStaysLegacy mirrors the TEXT case for BLOB columns,
// which suffer the same flip from BytesAddrEnc → BytesAdaptiveEnc under the global flag.
func TestPreserveAdaptiveEncoding_BlobStaysLegacy(t *testing.T) {
	prev := UseAdaptiveEncoding
	UseAdaptiveEncoding = true
	t.Cleanup(func() { UseAdaptiveEncoding = prev })

	existing := (&varBinaryType{sqlBinaryType: gmstypes.Blob}).
		WithEncoding(val.BytesAddrEnc)
	require.Equal(t, val.BytesAddrEnc, existing.Encoding())

	freshTI, err := FromSqlType(gmstypes.LongBlob)
	require.NoError(t, err)
	require.Equal(t, val.BytesAdaptiveEnc, freshTI.Encoding(),
		"sanity: a fresh BLOB TypeInfo with enc=0 must default to adaptive under the global flag")

	preserved := PreserveAdaptiveEncoding(existing, freshTI)
	require.Equal(t, val.BytesAddrEnc, preserved.Encoding(),
		"PreserveAdaptiveEncoding must keep the legacy BytesAddr encoding on a BLOB→LONGBLOB widening")
}

// TestPreserveAdaptiveEncoding_FamilyMismatch verifies the helper is a no-op when the
// old and new TypeInfos are different families (e.g. VARCHAR → TEXT, TEXT → BLOB). In
// those cases the column kind changes and the new column has no legacy on-disk format
// to honour — falling through to the new TypeInfo's default is correct.
func TestPreserveAdaptiveEncoding_FamilyMismatch(t *testing.T) {
	prev := UseAdaptiveEncoding
	UseAdaptiveEncoding = true
	t.Cleanup(func() { UseAdaptiveEncoding = prev })

	// VARCHAR (varStringType) → TEXT (blobStringType): different families.
	varchar, err := FromSqlType(gmstypes.MustCreateString(sqltypes.VarChar, 100, sql.Collation_Default))
	require.NoError(t, err)
	text, err := FromSqlType(gmstypes.Text)
	require.NoError(t, err)

	preserved := PreserveAdaptiveEncoding(varchar, text)
	require.Equal(t, text.Encoding(), preserved.Encoding(),
		"family mismatch must be a no-op: new TypeInfo's default encoding stays unchanged")
}

// TestPreserveAdaptiveEncoding_NilInputs verifies the helper is robust to nil inputs —
// callers in tables.go don't currently pass nil, but defensive callers (e.g. schema
// rebuild loops that skip absent columns) shouldn't crash.
func TestPreserveAdaptiveEncoding_NilInputs(t *testing.T) {
	text, err := FromSqlType(gmstypes.Text)
	require.NoError(t, err)

	require.Equal(t, text, PreserveAdaptiveEncoding(nil, text), "nil old must return new unchanged")
	require.Nil(t, PreserveAdaptiveEncoding(text, nil), "nil new must return nil unchanged")
}

// TestPreserveAdaptiveEncoding_NonAdaptiveTypePassthrough verifies that fixed-encoding
// type families (INT, DECIMAL, etc.) flow through unmodified — the helper only adjusts
// the variable-encoded TEXT/BLOB families.
func TestPreserveAdaptiveEncoding_NonAdaptiveTypePassthrough(t *testing.T) {
	intTI, err := FromSqlType(gmstypes.Int64)
	require.NoError(t, err)
	preserved := PreserveAdaptiveEncoding(intTI, intTI)
	require.Equal(t, intTI.Encoding(), preserved.Encoding(),
		"INT encoding is fixed — helper must not mutate it")
}

// TestPreserveAdaptiveEncoding_JsonStaysLegacy guards the JSON adaptive-encoding family.
// JSON columns persisted under JSONAddrEnc on 1.x must survive an ALTER on the same table
// (e.g., dropping or adding an unrelated column on the rewrite path) without silently
// flipping to JsonAdaptiveEnc. The reader-side compat absorbs the panic the way it does
// for TEXT/BLOB, but the persisted schema record drifting away from the on-disk value
// layout is itself the regression class this fix is supposed to close.
func TestPreserveAdaptiveEncoding_JsonStaysLegacy(t *testing.T) {
	prev := UseAdaptiveEncoding
	UseAdaptiveEncoding = true
	t.Cleanup(func() { UseAdaptiveEncoding = prev })

	existing := (&jsonType{jsonType: gmstypes.JSON.(gmstypes.JsonType)}).
		WithEncoding(val.JSONAddrEnc)
	require.Equal(t, val.JSONAddrEnc, existing.Encoding(),
		"sanity: a legacy JSON column should report JSONAddrEnc")

	freshTI, err := FromSqlType(gmstypes.JSON)
	require.NoError(t, err)
	require.Equal(t, val.JsonAdaptiveEnc, freshTI.Encoding(),
		"sanity: a fresh JSON TypeInfo with enc=0 must default to adaptive under the global flag")

	preserved := PreserveAdaptiveEncoding(existing, freshTI)
	require.Equal(t, val.JSONAddrEnc, preserved.Encoding(),
		"PreserveAdaptiveEncoding must keep the legacy JSONAddrEnc on a JSON-column-touching ALTER")
}

// TestPreserveAdaptiveEncoding_GeometryFamiliesStayLegacy covers all 8 concrete
// geometry TypeInfos: POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING,
// MULTIPOLYGON, GEOMETRYCOLLECTION, and the polymorphic GEOMETRY catch-all. Each must
// pin its own legacy GeomAddrEnc and refuse to fall back to the global GeomAdaptiveEnc
// default.
func TestPreserveAdaptiveEncoding_GeometryFamiliesStayLegacy(t *testing.T) {
	prev := UseAdaptiveEncoding
	UseAdaptiveEncoding = true
	t.Cleanup(func() { UseAdaptiveEncoding = prev })

	// Each (existing, fresh) pair is the same concrete type in the same family. We
	// construct existing under the legacy GeomAddrEnc and assert the helper pins.
	pairs := []struct {
		name     string
		existing TypeInfo
		fresh    TypeInfo
	}{
		{
			name: "Point",
			existing: (&pointType{sqlPointType: gmstypes.PointType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &pointType{sqlPointType: gmstypes.PointType{}},
		},
		{
			name: "LineString",
			existing: (&linestringType{sqlLineStringType: gmstypes.LineStringType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &linestringType{sqlLineStringType: gmstypes.LineStringType{}},
		},
		{
			name: "Polygon",
			existing: (&polygonType{sqlPolygonType: gmstypes.PolygonType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &polygonType{sqlPolygonType: gmstypes.PolygonType{}},
		},
		{
			name: "MultiPoint",
			existing: (&multipointType{sqlMultiPointType: gmstypes.MultiPointType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &multipointType{sqlMultiPointType: gmstypes.MultiPointType{}},
		},
		{
			name: "MultiLineString",
			existing: (&multilinestringType{sqlMultiLineStringType: gmstypes.MultiLineStringType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &multilinestringType{sqlMultiLineStringType: gmstypes.MultiLineStringType{}},
		},
		{
			name: "MultiPolygon",
			existing: (&multipolygonType{sqlMultiPolygonType: gmstypes.MultiPolygonType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &multipolygonType{sqlMultiPolygonType: gmstypes.MultiPolygonType{}},
		},
		{
			name: "GeometryCollection",
			existing: (&geomcollType{sqlGeomCollType: gmstypes.GeomCollType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &geomcollType{sqlGeomCollType: gmstypes.GeomCollType{}},
		},
		{
			name: "Geometry",
			existing: (&geometryType{sqlGeometryType: gmstypes.GeometryType{}}).
				WithEncoding(val.GeomAddrEnc),
			fresh: &geometryType{sqlGeometryType: gmstypes.GeometryType{}},
		},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			require.Equal(t, val.GeomAddrEnc, p.existing.Encoding(),
				"sanity: legacy %s must report GeomAddrEnc", p.name)
			require.Equal(t, val.GeomAdaptiveEnc, p.fresh.Encoding(),
				"sanity: fresh %s with enc=0 must default to GeomAdaptiveEnc under the global flag", p.name)

			preserved := PreserveAdaptiveEncoding(p.existing, p.fresh)
			require.Equal(t, val.GeomAddrEnc, preserved.Encoding(),
				"PreserveAdaptiveEncoding must pin the legacy GeomAddrEnc on a same-family %s ALTER", p.name)
		})
	}
}

// TestPreserveAdaptiveEncoding_GeometryCrossFamilyIsNoOp verifies that swapping one
// concrete geometry type for another (e.g., POINT → LINESTRING) is treated as a family
// mismatch and the helper returns the fresh TypeInfo unchanged. The geometry encodings
// happen to be assignment-compatible at the val.Encoding level (all GeomAddrEnc /
// GeomAdaptiveEnc / GeometryEnc), but each concrete type has its own on-disk row layout,
// so a cross-family pin would be semantically wrong.
func TestPreserveAdaptiveEncoding_GeometryCrossFamilyIsNoOp(t *testing.T) {
	prev := UseAdaptiveEncoding
	UseAdaptiveEncoding = true
	t.Cleanup(func() { UseAdaptiveEncoding = prev })

	existing := (&pointType{sqlPointType: gmstypes.PointType{}}).WithEncoding(val.GeomAddrEnc)
	fresh := &linestringType{sqlLineStringType: gmstypes.LineStringType{}}

	preserved := PreserveAdaptiveEncoding(existing, fresh)
	require.Equal(t, fresh.Encoding(), preserved.Encoding(),
		"POINT → LINESTRING is a cross-family change — helper must NOT pin the POINT encoding onto the new LINESTRING")
}
