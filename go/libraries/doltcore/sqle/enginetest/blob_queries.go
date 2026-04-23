// Copyright 2021 Dolthub, Inc.
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

package enginetest

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/store/hash"
)

func makeTestBytes(size int, firstbyte byte) []byte {
	bytes := make([]byte, size)
	bytes[0] = firstbyte
	return bytes
}

// A 4000 byte file starting with 0x00 and then consisting of all zeros.
// This is larger than default target tuple size for outlining adaptive types.
// We expect a tuple to always store this value out-of-band
var fullSizeBytes = makeTestBytes(4000, 0)
var fullSizeString = string(fullSizeBytes)
var fullSizeHash = hash.Hash{0x69, 0xe1, 0x2f, 0x49, 0xd1, 0x5e, 0xc0, 0x7f, 0x8f, 0x59, 0xab, 0xac, 0xb0, 0xf, 0x76, 0x3e, 0x7d, 0xf6, 0x5e, 0xb5}

// A 2000 byte file starting with 0x01 and then consisting of all zeros.
// This is over half of the default target tuple size for outlining adaptive types.
// We expect a tuple to be able to store this value inline once, but not twice.
var halfSizeBytes = makeTestBytes(2000, 1)
var halfSizeString = string(halfSizeBytes)
var halfSizeHash = hash.Hash{0x31, 0x0c, 0x97, 0x33, 0x73, 0xa8, 0xbe, 0xcb, 0xbf, 0xc8, 0x83, 0x3a, 0xbf, 0x51, 0x40, 0x8a, 0x7e, 0x43, 0x1a, 0xf1}

// A 10 byte file starting with 0x02 and then consisting of 10 zero bytes.
// This is file is smaller than an address hash.
// We expect a tuple to never store this value out-of-band.
var tinyBytes = makeTestBytes(10, 2)
var tinyString = string(tinyBytes)

type AdaptiveEncodingTestColumnType byte

const (
	AdaptiveEncodingTestType_Blob AdaptiveEncodingTestColumnType = iota
	AdaptiveEncodingTestType_Text
)

func (a AdaptiveEncodingTestColumnType) String() string {
	if a == AdaptiveEncodingTestType_Blob {
		return "Blob"
	}
	return "Text"
}

type AdaptiveEncodingTestPurpose byte

const (
	AdaptiveEncodingTestPurpose_Representation AdaptiveEncodingTestPurpose = iota
	AdaptiveEncodingTestPurpose_Correctness
)

func (a AdaptiveEncodingTestPurpose) String() string {
	if a == AdaptiveEncodingTestPurpose_Representation {
		return "Representation"
	}
	return "Correctness"
}

func MakeBigAdaptiveEncodingQueriesSetup(columnType AdaptiveEncodingTestColumnType) []setup.SetupScript {
	var typename string
	switch columnType {
	case AdaptiveEncodingTestType_Blob:
		typename = "longblob"
	case AdaptiveEncodingTestType_Text:
		typename = "longtext"
	}
	return []setup.SetupScript{{
		fmt.Sprintf(`create table blobt (i char(1) primary key, b %s);`, typename),
		fmt.Sprintf(`create table blobt2 (i char(2) primary key, b1 %s, b2 %s);`, typename, typename),
		`insert into blobt values
    ('F', LOAD_FILE('testdata/fullSize')),
    ('H', LOAD_FILE('testdata/halfSize')),
    ('T', LOAD_FILE('testdata/tinyFile')),
	('N', NULL)`,
		`insert into blobt2 values
    ('FF', LOAD_FILE('testdata/fullSize'), LOAD_FILE('testdata/fullSize')),
    ('HF', LOAD_FILE('testdata/halfSize'), LOAD_FILE('testdata/fullSize')),
    ('TF', LOAD_FILE('testdata/tinyFile'), LOAD_FILE('testdata/fullSize')),
    ('NF', NULL, LOAD_FILE('testdata/fullSize')),
	('FH', LOAD_FILE('testdata/fullSize'), LOAD_FILE('testdata/halfSize')),
	('HH', LOAD_FILE('testdata/halfSize'), LOAD_FILE('testdata/halfSize')),
	('TH', LOAD_FILE('testdata/tinyFile'), LOAD_FILE('testdata/halfSize')),
	('NH', NULL, LOAD_FILE('testdata/halfSize')),
    ('FT', LOAD_FILE('testdata/fullSize'), LOAD_FILE('testdata/tinyFile')),
    ('HT', LOAD_FILE('testdata/halfSize'), LOAD_FILE('testdata/tinyFile')),
    ('TT', LOAD_FILE('testdata/tinyFile'), LOAD_FILE('testdata/tinyFile')),
    ('NT', NULL, LOAD_FILE('testdata/tinyFile')),
    ('FN', LOAD_FILE('testdata/fullSize'), NULL),
    ('HN', LOAD_FILE('testdata/halfSize'), NULL),
    ('TN', LOAD_FILE('testdata/tinyFile'), NULL),
    ('NN', NULL, NULL)`,
	}}
}

var BigBlobWriteQueries = []queries.WriteQueryTest{
	{
		WriteQuery:          "INSERT INTO blobt VALUES(4, LOAD_FILE('testdata/test1.png'))",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "select sha1(b) from blobt where i = 4",
		ExpectedSelect:      []sql.Row{{"012bcb75a319f2913614a5170fc046fb6c49ee86"}},
	},
}

func MakeBigAdaptiveEncodingQueries(columnType AdaptiveEncodingTestColumnType, testPurpose AdaptiveEncodingTestPurpose) []queries.QueryTest {
	var fullSize interface{}
	var halfSize interface{}
	var tiny interface{}
	if columnType == AdaptiveEncodingTestType_Blob {
		fullSize = fullSizeBytes
		halfSize = halfSizeBytes
		tiny = tinyBytes
	} else {
		// columnType == AdaptiveEncodingTestType_Text
		fullSize = fullSizeString
		halfSize = halfSizeString
		tiny = tinyString
	}

	var fullSizeOutOfLineRepr interface{}
	var halfSizeOutOfLineRepr interface{}
	var wrapBehavior queries.WrapBehavior
	if testPurpose == AdaptiveEncodingTestPurpose_Representation {
		wrapBehavior = queries.WrapBehavior_Hash
		fullSizeOutOfLineRepr = fullSizeHash
		halfSizeOutOfLineRepr = halfSizeHash
	} else {
		// testPurpose == AdaptiveEncodingTestPurpose_Correctness
		// For this test, always unwrap values and expect their normalized form (either bytes or string, never hash)
		wrapBehavior = queries.WrapBehavior_Unwrap
		fullSizeOutOfLineRepr = fullSize
		halfSizeOutOfLineRepr = halfSize
	}

	return []queries.QueryTest{
		{
			// Large files are always stored out-of-band
			Query:        "select b from blobt where i = 'F'",
			Expected:     []sql.Row{{fullSizeOutOfLineRepr}},
			WrapBehavior: wrapBehavior,
		},
		{
			// Files that can fit within a tuple are stored inline.
			Query:        "select b from blobt where i = 'H'",
			Expected:     []sql.Row{{halfSize}},
			WrapBehavior: wrapBehavior,
		},
		{
			// When a tuple with multiple adaptive columns is too large, columns are moved out-of-band from largest to smallest.
			// However, strings smaller than the address size (20 bytes) are never stored out-of-band.
			Query:        "select i, b1, b2 from blobt2",
			WrapBehavior: wrapBehavior,
			Expected: []sql.Row{
				{"FF", fullSizeOutOfLineRepr, fullSizeOutOfLineRepr},
				{"HF", halfSize, fullSizeOutOfLineRepr},
				{"TF", tiny, fullSizeOutOfLineRepr},
				{"NF", nil, fullSizeOutOfLineRepr},
				{"FH", fullSizeOutOfLineRepr, halfSize},
				{"HH", halfSizeOutOfLineRepr, halfSize},
				{"TH", tiny, halfSize},
				{"NH", nil, halfSize},
				{"FT", fullSizeOutOfLineRepr, tiny},
				{"HT", halfSize, tiny},
				{"TT", tiny, tiny},
				{"NT", nil, tiny},
				{"FN", fullSizeOutOfLineRepr, nil},
				{"HN", halfSize, nil},
				{"TN", tiny, nil},
				{"NN", nil, nil},
			},
		},
		{
			// An inlined adaptive column can be used in a filter.
			Query:        "select i from blobt where b = LOAD_FILE('testdata/fullSize')",
			WrapBehavior: wrapBehavior,
			Expected:     []sql.Row{{"F"}},
		},
		{
			// An out-of-line adaptive column can be used in a filter.
			Query:        "select i from blobt where b = LOAD_FILE('testdata/halfSize')",
			WrapBehavior: wrapBehavior,
			Expected:     []sql.Row{{"H"}},
		},
		{
			// An adaptive column can be used in a filter when it doesn't have the same encoding in all rows.
			Query:        "select i from blobt2 where b1 = LOAD_FILE('testdata/halfSize')",
			WrapBehavior: wrapBehavior,
			Expected:     []sql.Row{{"HF"}, {"HH"}, {"HT"}, {"HN"}},
		},
		{
			// An adaptive column can be used in a filter when it doesn't have the same encoding in all rows.
			Query:        "select i from blobt2 where b2 = LOAD_FILE('testdata/halfSize')",
			WrapBehavior: wrapBehavior,
			Expected:     []sql.Row{{"FH"}, {"HH"}, {"TH"}, {"NH"}},
		},
	}
}

var AdaptiveEncodingScripts = []queries.ScriptTest{
	{
		Name: "blob length function",
		SetUpScript: []string{
			`CREATE TABLE blobdata (
  pk          INT NOT NULL PRIMARY KEY,
  c_varbinary VARBINARY(255),
  c_tinyblob  TINYBLOB,
  c_blob      BLOB,
  c_medblob   MEDIUMBLOB,
  c_longblob  LONGBLOB
);`,
			`INSERT INTO blobdata VALUES
  (1, 'varbin-old-1', 'tiny-old-1', 'blob-old-1', 'med-old-1', 'long-old-1'),
  (2, 'varbin-old-2', 'tiny-old-2', REPEAT('b', 60000), REPEAT('m', 70000), REPEAT('l', 90000));`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "SELECT LENGTH(c_medblob) FROM blobdata where pk = 2 order by 1",
				Expected: []sql.Row{{70000}},
			},
			{
				Query:    "SELECT LENGTH(c_varbinary), LENGTH(c_tinyblob), LENGTH(c_blob), LENGTH(c_medblob), LENGTH(c_longblob) FROM blobdata where pk = 2 order by 1",
				Expected: []sql.Row{{12, 10, 60000, 70000, 90000}},
			},
		},
	},
	{
		// Tests a single adaptive (LONGBLOB) column surrounded by non-adaptive INT columns.
		// The non-adaptive columns contribute 8 bytes to the value tuple's inline size, so the
		// adaptive column is outlined when len(b)+9 > 2048, i.e. len(b) >= 2040.
		// Also exercises sizes near the varint 1→2 byte boundary (240/241 bytes) and values
		// too small to ever be outlined (≤ 20 bytes, where out-of-band would cost more).
		Name: "single adaptive column interleaved with non-adaptive columns",
		SetUpScript: []string{
			`CREATE TABLE t_ae_single (
				pk INT NOT NULL PRIMARY KEY,
				a  INT,
				b  LONGBLOB,
				c  INT
			)`,
			`INSERT INTO t_ae_single VALUES
				(1, 10, REPEAT('a', 100),  20),
				(2, 10, REPEAT('a', 2039), 20),
				(3, 10, REPEAT('a', 2040), 20),
				(4, 10, REPEAT('a', 5000), 20),
				(5, 10, NULL,              20),
				(6, 10, REPEAT('a', 19),   20),
				(7, 10, REPEAT('a', 240),  20),
				(8, 10, REPEAT('a', 241),  20)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b) FROM t_ae_single ORDER BY pk",
				Expected: []sql.Row{
					{1, 100},
					{2, 2039},
					{3, 2040},
					{4, 5000},
					{5, nil},
					{6, 19},
					{7, 240},
					{8, 241},
				},
			},
			{
				// Inline value (pk=1): filter and read work correctly.
				Query:    "SELECT LENGTH(b) FROM t_ae_single WHERE b = REPEAT('a', 100)",
				Expected: []sql.Row{{100}},
			},
			{
				// Out-of-band value (pk=3): filter and read work correctly.
				Query:    "SELECT LENGTH(b) FROM t_ae_single WHERE b = REPEAT('a', 2040)",
				Expected: []sql.Row{{2040}},
			},
			{
				// Non-adaptive columns are unaffected by adaptive encoding decisions.
				Query:    "SELECT pk, a, c FROM t_ae_single WHERE pk = 3",
				Expected: []sql.Row{{3, 10, 20}},
			},
		},
	},
	{
		// Tests two adaptive columns with a non-adaptive INT column between them.
		// The adaptive encoding algorithm outlines columns in largest-savings-first order.
		// When savings are equal the stable sort preserves column order, so the leftmost
		// adaptive column is outlined first.
		Name: "two adaptive columns interleaved with non-adaptive columns, largest outlined first",
		SetUpScript: []string{
			`CREATE TABLE t_ae_two (
				pk INT NOT NULL PRIMARY KEY,
				b1 LONGBLOB,
				a  INT,
				b2 LONGBLOB
			)`,
			// Value tuple: b1(adaptive), a(4 bytes), b2(adaptive).
			// inlineSize = (1+len1) + 4 + (1+len2) = len1+len2+6.
			// Outlining triggers when len1+len2+6 > 2048.
			`INSERT INTO t_ae_two VALUES
				(1, REPEAT('a', 1000), 1, REPEAT('b', 1000)),
				(2, REPEAT('a', 1500), 2, REPEAT('b', 1500)),
				(3, REPEAT('a', 3000), 3, REPEAT('b',  100)),
				(4, REPEAT('a',  100), 4, REPEAT('b', 3000)),
				(5, REPEAT('a', 3000), 5, REPEAT('b', 3000)),
				(6, NULL,              6, REPEAT('b', 1500))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b1), LENGTH(b2) FROM t_ae_two ORDER BY pk",
				Expected: []sql.Row{
					{1, 1000, 1000},
					{2, 1500, 1500},
					{3, 3000, 100},
					{4, 100, 3000},
					{5, 3000, 3000},
					{6, nil, 1500},
				},
			},
			{
				// pk=2: equal sizes → b1 (leftmost) outlined first, b2 stays inline.
				Query:    "SELECT LENGTH(b1) FROM t_ae_two WHERE b2 = REPEAT('b', 1500) AND pk = 2",
				Expected: []sql.Row{{1500}},
			},
			{
				// pk=3: b1 is larger → b1 outlined, b2 remains inline.
				Query:    "SELECT LENGTH(b2) FROM t_ae_two WHERE b1 = REPEAT('a', 3000) AND pk = 3",
				Expected: []sql.Row{{100}},
			},
			{
				// pk=4: b2 is larger → b2 outlined, b1 remains inline.
				Query:    "SELECT LENGTH(b1) FROM t_ae_two WHERE b2 = REPEAT('b', 3000) AND pk = 4",
				Expected: []sql.Row{{100}},
			},
			{
				// pk=6: NULL b1 does not interfere with outlining decision for b2.
				Query:    "SELECT LENGTH(b2) FROM t_ae_two WHERE pk = 6",
				Expected: []sql.Row{{1500}},
			},
		},
	},
	{
		// Tests three adaptive columns with non-adaptive INT columns between them.
		// Verifies priority ordering (b1 large, b2 medium, b3 tiny: b1 outlined first),
		// equal-savings ordering (both b1 and b2 outlined for pk=2), and values that are
		// too small to ever benefit from out-of-band storage (b3 = 10 bytes).
		Name: "three adaptive columns with non-adaptive columns between them",
		SetUpScript: []string{
			`CREATE TABLE t_ae_three (
				pk INT NOT NULL PRIMARY KEY,
				b1 LONGBLOB,
				a1 INT,
				b2 LONGBLOB,
				a2 INT,
				b3 LONGBLOB
			)`,
			// Value tuple: b1, a1(4), b2, a2(4), b3.
			// inlineSize = (1+len1)+4+(1+len2)+4+(1+len3) = len1+len2+len3+11.
			`INSERT INTO t_ae_three VALUES
				(1, REPEAT('a', 5000), 1, REPEAT('b', 1500), 1, REPEAT('c', 10)),
				(2, REPEAT('a', 2000), 2, REPEAT('b', 2000), 2, REPEAT('c', 10)),
				(3, REPEAT('a', 3000), 3, REPEAT('b', 3000), 3, REPEAT('c', 3000)),
				(4, REPEAT('a', 5000), 4, REPEAT('b', 5000), 4, REPEAT('c', 10))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b1), LENGTH(b2), LENGTH(b3) FROM t_ae_three ORDER BY pk",
				Expected: []sql.Row{
					{1, 5000, 1500, 10},
					{2, 2000, 2000, 10},
					{3, 3000, 3000, 3000},
					{4, 5000, 5000, 10},
				},
			},
			{
				// b3 is 10 bytes (out-of-band would cost 21 bytes, savings < 0)
				// so it is never outlined regardless of other columns' sizes.
				Query:    "SELECT pk FROM t_ae_three WHERE b3 = REPEAT('c', 10) ORDER BY pk",
				Expected: []sql.Row{{1}, {2}, {4}},
			},
			{
				// All three columns outlined for pk=3; verify all are readable.
				Query:    "SELECT pk FROM t_ae_three WHERE b1 = REPEAT('a', 3000) AND b2 = REPEAT('b', 3000) AND b3 = REPEAT('c', 3000)",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		// Tests adaptive column sizes at the SQLite4 varint 1→2 byte encoding boundary.
		// Values of 240 bytes use a 1-byte varint in out-of-band encoding; values of 241
		// bytes use a 2-byte varint. With 9 equal-sized columns whose combined inline size
		// exceeds 2048, the stable-sort outlining algorithm outlines only b1 (leftmost).
		Name: "adaptive encoding sizes at varint 1-to-2 byte encoding boundary",
		SetUpScript: []string{
			`CREATE TABLE t_ae_varint (
				pk INT NOT NULL PRIMARY KEY,
				b1 LONGBLOB,
				b2 LONGBLOB,
				b3 LONGBLOB,
				b4 LONGBLOB,
				b5 LONGBLOB,
				b6 LONGBLOB,
				b7 LONGBLOB,
				b8 LONGBLOB,
				b9 LONGBLOB
			)`,
			// 9 columns × 240 bytes: combined inline = 9×241 = 2169 > 2048.
			// 240-byte values use a 1-byte varint out-of-band; savings = 220 each.
			// Only b1 is outlined (after outlining it, total drops to 1949 ≤ 2048).
			`INSERT INTO t_ae_varint VALUES
				(1,
				 REPEAT('a', 240), REPEAT('b', 240), REPEAT('c', 240),
				 REPEAT('d', 240), REPEAT('e', 240), REPEAT('f', 240),
				 REPEAT('g', 240), REPEAT('h', 240), REPEAT('i', 240))`,
			// 9 columns × 241 bytes: combined inline = 9×242 = 2178 > 2048.
			// 241-byte values cross into 2-byte varint territory.
			// Only b1 is outlined (total drops to 1957 ≤ 2048).
			`INSERT INTO t_ae_varint VALUES
				(2,
				 REPEAT('a', 241), REPEAT('b', 241), REPEAT('c', 241),
				 REPEAT('d', 241), REPEAT('e', 241), REPEAT('f', 241),
				 REPEAT('g', 241), REPEAT('h', 241), REPEAT('i', 241))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b1), LENGTH(b9) FROM t_ae_varint ORDER BY pk",
				Expected: []sql.Row{
					{1, 240, 240},
					{2, 241, 241},
				},
			},
			{
				// b1 is outlined for both rows; verify correct out-of-band read.
				Query:    "SELECT pk FROM t_ae_varint WHERE b1 = REPEAT('a', 240)",
				Expected: []sql.Row{{1}},
			},
			{
				// b9 is inline for both rows; verify correct inline read.
				Query:    "SELECT pk FROM t_ae_varint WHERE b9 = REPEAT('i', 240)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT pk FROM t_ae_varint WHERE b1 = REPEAT('a', 241)",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT pk FROM t_ae_varint WHERE b9 = REPEAT('i', 241)",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		// Tests the exact outlining threshold for a single adaptive column with no other
		// value-tuple columns. The value tuple contains only b, so inlineSize = 1 + len(b).
		// The condition to outline is 1+len > 2048, meaning len=2047 stays inline but
		// len=2048 is outlined. Also tests that PutAdaptiveFromInline immediately stores
		// values out-of-band when they individually exceed the 2048-byte target.
		Name: "single adaptive column: 2047-byte value stays inline, 2048-byte value is outlined",
		SetUpScript: []string{
			`CREATE TABLE t_ae_threshold (pk INT NOT NULL PRIMARY KEY, b LONGBLOB)`,
			`INSERT INTO t_ae_threshold VALUES
				(1, REPEAT('x', 2046)),
				(2, REPEAT('x', 2047)),
				(3, REPEAT('x', 2048)),
				(4, REPEAT('x', 2049))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b) FROM t_ae_threshold ORDER BY pk",
				Expected: []sql.Row{
					{1, 2046},
					{2, 2047},
					{3, 2048},
					{4, 2049},
				},
			},
			{
				Query:    "SELECT pk FROM t_ae_threshold WHERE b = REPEAT('x', 2046) ORDER BY pk",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT pk FROM t_ae_threshold WHERE b = REPEAT('x', 2047) ORDER BY pk",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT pk FROM t_ae_threshold WHERE b = REPEAT('x', 2048) ORDER BY pk",
				Expected: []sql.Row{{3}},
			},
			{
				Query:    "SELECT pk FROM t_ae_threshold WHERE b = REPEAT('x', 2049) ORDER BY pk",
				Expected: []sql.Row{{4}},
			},
		},
	},
	{
		// Tests five adaptive columns whose combined tracked inline size approaches 64KB.
		// Values of 12000 bytes each exceed the 2048-byte per-value threshold, so they are
		// immediately written out-of-band during tuple construction, but their inline sizes
		// are still tracked for the outlining accounting (5 × 12001 = 60005 bytes total).
		// Also tests a mixed row where three large values sit alongside two medium values,
		// verifying that the priority ordering correctly outlines the large values first and
		// that only as many medium values are outlined as needed.
		Name: "multiple large adaptive columns with combined inline size approaching 64KB",
		SetUpScript: []string{
			`CREATE TABLE t_ae_large_combined (
				pk INT NOT NULL PRIMARY KEY,
				b1 LONGBLOB,
				b2 LONGBLOB,
				b3 LONGBLOB,
				b4 LONGBLOB,
				b5 LONGBLOB
			)`,
			// All five columns at 12000 bytes: each individually exceeds 2048 so each is
			// immediately stored out-of-band. Combined tracked inline size ≈ 60 KB.
			`INSERT INTO t_ae_large_combined VALUES
				(1,
				 REPEAT('a', 12000), REPEAT('b', 12000), REPEAT('c', 12000),
				 REPEAT('d', 12000), REPEAT('e', 12000))`,
			// Three 12000-byte (immediately OOB) plus two 1000-byte (initially inline).
			// Combined tracked inline = 3×12001 + 2×1001 = 38005.
			// Outlining order: b1, b2, b3 first (largest savings); then b4 is outlined
			// to bring total below 2048. b5 stays inline.
			`INSERT INTO t_ae_large_combined VALUES
				(2,
				 REPEAT('a', 12000), REPEAT('b', 12000), REPEAT('c', 12000),
				 REPEAT('d', 1000),  REPEAT('e', 1000))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b1), LENGTH(b2), LENGTH(b3), LENGTH(b4), LENGTH(b5) FROM t_ae_large_combined ORDER BY pk",
				Expected: []sql.Row{
					{1, 12000, 12000, 12000, 12000, 12000},
					{2, 12000, 12000, 12000, 1000, 1000},
				},
			},
			{
				// b1 is out-of-band for both rows; verify filter and read.
				Query:    "SELECT pk FROM t_ae_large_combined WHERE b1 = REPEAT('a', 12000) ORDER BY pk",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				// pk=1 b5 is out-of-band (12000 bytes); verify correct retrieval.
				Query:    "SELECT pk FROM t_ae_large_combined WHERE b5 = REPEAT('e', 12000)",
				Expected: []sql.Row{{1}},
			},
			{
				// pk=2 b4 was outlined during BuildPermissive; verify correct retrieval.
				Query:    "SELECT pk FROM t_ae_large_combined WHERE b4 = REPEAT('d', 1000)",
				Expected: []sql.Row{{2}},
			},
			{
				// pk=2 b5 remains inline; verify correct retrieval.
				Query:    "SELECT pk FROM t_ae_large_combined WHERE b5 = REPEAT('e', 1000)",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		// Tests five medium-sized adaptive columns where the combined size requires selective
		// outlining: the algorithm outlines columns from largest savings to smallest until the
		// tuple falls within the 2048-byte target, leaving the remaining columns inline.
		Name: "many medium adaptive columns with selective outlining",
		SetUpScript: []string{
			`CREATE TABLE t_ae_selective (
				pk INT NOT NULL PRIMARY KEY,
				b1 LONGBLOB,
				b2 LONGBLOB,
				b3 LONGBLOB,
				b4 LONGBLOB,
				b5 LONGBLOB
			)`,
			// Row 1: 5 × 1500 bytes; inlineSize = 5×1501 = 7505 > 2048.
			// All have equal savings (~1480); stable sort outlines b1..b4 in order.
			// After outlining 4 columns: 7505 - 4×1480 = 1585 ≤ 2048. b5 stays inline.
			`INSERT INTO t_ae_selective VALUES
				(1,
				 REPEAT('a', 1500), REPEAT('b', 1500), REPEAT('c', 1500),
				 REPEAT('d', 1500), REPEAT('e', 1500))`,
			// Row 2: descending sizes; b1 has the most savings and is outlined first.
			// After outlining b1 (~savings 1980), total ≈ 1825 ≤ 2048: b2..b5 stay inline.
			`INSERT INTO t_ae_selective VALUES
				(2,
				 REPEAT('a', 2000), REPEAT('b', 1000), REPEAT('c', 500),
				 REPEAT('d', 200),  REPEAT('e', 100))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b1), LENGTH(b2), LENGTH(b3), LENGTH(b4), LENGTH(b5) FROM t_ae_selective ORDER BY pk",
				Expected: []sql.Row{
					{1, 1500, 1500, 1500, 1500, 1500},
					{2, 2000, 1000, 500, 200, 100},
				},
			},
			{
				// b5 stays inline for row 1; verify filter and correct value.
				Query:    "SELECT pk FROM t_ae_selective WHERE b5 = REPEAT('e', 1500)",
				Expected: []sql.Row{{1}},
			},
			{
				// b1..b4 outlined for row 1; verify all are readable.
				Query:    "SELECT pk FROM t_ae_selective WHERE b1 = REPEAT('a', 1500) AND b4 = REPEAT('d', 1500)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT LENGTH(b1), LENGTH(b2), LENGTH(b3), LENGTH(b4), LENGTH(b5) FROM t_ae_selective WHERE pk = 2",
				Expected: []sql.Row{{2000, 1000, 500, 200, 100}},
			},
			{
				// b1 outlined for row 2; b2..b5 inline. Verify mix of storage formats.
				Query:    "SELECT pk FROM t_ae_selective WHERE b1 = REPEAT('a', 2000) AND b5 = REPEAT('e', 100)",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		// Tests that the adaptive encoding system correctly handles tuples whose combined
		// tracked inline size is much greater than 64KB (the tuple format's hard limit).
		// Each 30000-byte value individually exceeds 2048 and is immediately stored
		// out-of-band; the combined tracked inline size is 5 × 30001 ≈ 150 KB.
		// The second row mixes very large values with medium ones; after the three large
		// columns bring the total below 2048, the two medium columns stay inline.
		Name: "combined LONGBLOB values much greater than 64KB",
		SetUpScript: []string{
			`CREATE TABLE t_ae_large (
				pk INT NOT NULL PRIMARY KEY,
				b1 LONGBLOB,
				b2 LONGBLOB,
				b3 LONGBLOB,
				b4 LONGBLOB,
				b5 LONGBLOB
			)`,
			// Row 1: all five columns at 30000 bytes; combined tracked inline ≈ 150 KB.
			`INSERT INTO t_ae_large VALUES
				(1,
				 REPEAT('a', 30000), REPEAT('b', 30000), REPEAT('c', 30000),
				 REPEAT('d', 30000), REPEAT('e', 30000))`,
			// Row 2: b1..b3 at 30000 bytes (immediately OOB); b4 at 1500 bytes and
			// b5 at 100 bytes (initially inline). Combined tracked inline ≈ 91 KB.
			// After outlining b1..b3 the total drops to ~1671 bytes ≤ 2048, so b4 and b5
			// remain inline even though the combined size was far above the format limit.
			`INSERT INTO t_ae_large VALUES
				(2,
				 REPEAT('a', 30000), REPEAT('b', 30000), REPEAT('c', 30000),
				 REPEAT('d', 1500),  REPEAT('e', 100))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, LENGTH(b1), LENGTH(b2), LENGTH(b3), LENGTH(b4), LENGTH(b5) FROM t_ae_large ORDER BY pk",
				Expected: []sql.Row{
					{1, 30000, 30000, 30000, 30000, 30000},
					{2, 30000, 30000, 30000, 1500, 100},
				},
			},
			{
				Query:    "SELECT pk FROM t_ae_large WHERE b1 = REPEAT('a', 30000) ORDER BY pk",
				Expected: []sql.Row{{1}, {2}},
			},
			{
				// b4 is inline for row 2; verify it is readable.
				Query:    "SELECT pk FROM t_ae_large WHERE b4 = REPEAT('d', 1500)",
				Expected: []sql.Row{{2}},
			},
			{
				// b5 is inline for row 2; verify it is readable.
				Query:    "SELECT pk FROM t_ae_large WHERE b5 = REPEAT('e', 100)",
				Expected: []sql.Row{{2}},
			},
		},
	},
	{
		// Tests that the outlining threshold is based on the storage byte length of a TEXT
		// value, not its character count. The character 'ñ' (U+00F1) encodes as two UTF-8
		// bytes. A string of 1024 'ñ' characters is 2048 bytes and is outlined, while a
		// string of 2047 ASCII 'a' characters is 2047 bytes and stays inline — even though
		// the ASCII string has nearly twice the character count.
		// Additionally tests that 3-byte ('€', U+20AC) and 4-byte ('😀', U+1F600) UTF-8
		// characters follow the same byte-length rule.
		Name: "LONGTEXT outlining uses byte length not character count",
		SetUpScript: []string{
			`CREATE TABLE t_ae_text_enc (
				pk INT NOT NULL PRIMARY KEY,
				t  LONGTEXT CHARACTER SET utf8mb4
			)`,
			// Single-byte ASCII: 2047 chars = 2047 bytes → inline (2048 ≤ 2048).
			`INSERT INTO t_ae_text_enc VALUES (1, REPEAT('a', 2047))`,
			// Single-byte ASCII: 2048 chars = 2048 bytes → outlined (2049 > 2048).
			`INSERT INTO t_ae_text_enc VALUES (2, REPEAT('a', 2048))`,
			// 2-byte UTF-8 ('ñ'): 1023 chars = 2046 bytes → inline (2047 ≤ 2048).
			// This has far fewer characters than row 1 but the same ballpark byte length.
			`INSERT INTO t_ae_text_enc VALUES (3, REPEAT('ñ', 1023))`,
			// 2-byte UTF-8 ('ñ'): 1024 chars = 2048 bytes → outlined (2049 > 2048).
			// Same character count as an ASCII string that is well under the threshold,
			// but outlined because its storage byte length crosses the 2048-byte limit.
			`INSERT INTO t_ae_text_enc VALUES (4, REPEAT('ñ', 1024))`,
			// 3-byte UTF-8 ('€'): 682 chars = 2046 bytes → inline (2047 ≤ 2048).
			`INSERT INTO t_ae_text_enc VALUES (5, REPEAT('€', 682))`,
			// 3-byte UTF-8 ('€'): 683 chars = 2049 bytes → outlined (2050 > 2048).
			`INSERT INTO t_ae_text_enc VALUES (6, REPEAT('€', 683))`,
			// 4-byte UTF-8 ('😀'): 511 chars = 2044 bytes → inline (2045 ≤ 2048).
			`INSERT INTO t_ae_text_enc VALUES (7, REPEAT('😀', 511))`,
			// 4-byte UTF-8 ('😀'): 512 chars = 2048 bytes → outlined (2049 > 2048).
			`INSERT INTO t_ae_text_enc VALUES (8, REPEAT('😀', 512))`,
			// Small values that are never outlined regardless of encoding.
			`INSERT INTO t_ae_text_enc VALUES (9,  REPEAT('a', 15))`,
			`INSERT INTO t_ae_text_enc VALUES (10, REPEAT('ñ', 10))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// CHAR_LENGTH returns character count; LENGTH returns storage byte count.
				// The outlining threshold is a byte count (2048), not a character count.
				Query: "SELECT pk, CHAR_LENGTH(t), LENGTH(t) FROM t_ae_text_enc ORDER BY pk",
				Expected: []sql.Row{
					{1, 2047, 2047}, // ASCII inline
					{2, 2048, 2048}, // ASCII outlined
					{3, 1023, 2046}, // 2-byte inline  (fewer chars than row 1, similar bytes)
					{4, 1024, 2048}, // 2-byte outlined (same chars as ASCII 1024, but 2× bytes)
					{5, 682, 2046},  // 3-byte inline
					{6, 683, 2049},  // 3-byte outlined
					{7, 511, 2044},  // 4-byte inline
					{8, 512, 2048},  // 4-byte outlined
					{9, 15, 15},     // tiny ASCII, always inline
					{10, 10, 20},    // tiny 2-byte, always inline (20 bytes < 21-byte OOB cost)
				},
			},
			{
				// pk=1 (2047 ASCII bytes, inline) and pk=4 (1024 ñ chars = 2048 bytes, outlined)
				// both return correct values, demonstrating correct storage regardless of encoding.
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('a', 2047)",
				Expected: []sql.Row{{1}},
			},
			{
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('a', 2048)",
				Expected: []sql.Row{{2}},
			},
			{
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('ñ', 1023)",
				Expected: []sql.Row{{3}},
			},
			{
				// 1024 'ñ' characters = 2048 bytes: outlined because byte length ≥ 2048,
				// even though 1024 ASCII characters would be far below the threshold.
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('ñ', 1024)",
				Expected: []sql.Row{{4}},
			},
			{
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('€', 682)",
				Expected: []sql.Row{{5}},
			},
			{
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('€', 683)",
				Expected: []sql.Row{{6}},
			},
			{
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('😀', 511)",
				Expected: []sql.Row{{7}},
			},
			{
				Query:    "SELECT pk FROM t_ae_text_enc WHERE t = REPEAT('😀', 512)",
				Expected: []sql.Row{{8}},
			},
		},
	},
	{
		// Tests multiple LONGTEXT columns containing multibyte UTF-8 characters where the
		// combined storage size is much greater than 64KB. Also verifies interleaving with
		// non-adaptive columns, that CHARACTER_LENGTH and LENGTH diverge for multibyte text,
		// and that the selective outlining algorithm correctly handles a mix of immediately
		// out-of-band values and medium inline values.
		Name: "multiple LONGTEXT columns with multibyte characters and combined size much greater than 64KB",
		SetUpScript: []string{
			`CREATE TABLE t_ae_text_large (
				pk INT NOT NULL PRIMARY KEY,
				a  INT,
				t1 LONGTEXT CHARACTER SET utf8mb4,
				b  INT,
				t2 LONGTEXT CHARACTER SET utf8mb4,
				c  INT,
				t3 LONGTEXT CHARACTER SET utf8mb4,
				d  INT,
				t4 LONGTEXT CHARACTER SET utf8mb4,
				e  INT,
				t5 LONGTEXT CHARACTER SET utf8mb4
			)`,
			// Row 1: five columns each with 15000 'ñ' characters = 30000 bytes.
			// Combined tracked inline ≈ 5×30001 = 150005 bytes, much greater than 64 KB.
			// All five are immediately stored out-of-band (30001 >> 2048).
			`INSERT INTO t_ae_text_large VALUES
				(1, 1,
				 REPEAT('ñ', 15000), 2,
				 REPEAT('ñ', 15000), 3,
				 REPEAT('ñ', 15000), 4,
				 REPEAT('ñ', 15000), 5,
				 REPEAT('ñ', 15000))`,
			// Row 2: t1..t3 at 15000 'ñ' chars (30000 bytes, immediately OOB),
			// t4 at 1000 'ñ' chars (2000 bytes, initially inline),
			// t5 at 100 'ñ' chars (200 bytes, inline).
			// Combined tracked inline ≈ 3×30001 + 4 + 2001 + 4 + 201 = 92213 bytes.
			// Outlining: t1..t3 first (largest savings), then t4.
			// After outlining t1..t3+t4 the total drops below 2048; t5 stays inline.
			`INSERT INTO t_ae_text_large VALUES
				(2, 1,
				 REPEAT('ñ', 15000), 2,
				 REPEAT('ñ', 15000), 3,
				 REPEAT('ñ', 15000), 4,
				 REPEAT('ñ', 1000),  5,
				 REPEAT('ñ', 100))`,
			// Row 3: single large column to verify 3-byte characters at large scale.
			// t1 = 10000 '€' chars = 30000 bytes; others small (tiny + NULL mix).
			`INSERT INTO t_ae_text_large VALUES
				(3, 1,
				 REPEAT('€', 10000), 2,
				 REPEAT('€', 100), 3,
				 NULL, 4,
				 REPEAT('€', 50), 5,
				 REPEAT('a', 200))`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				// CHAR_LENGTH ≠ LENGTH for multibyte text: CHAR_LENGTH counts characters,
				// LENGTH counts storage bytes (each 'ñ' is 2 bytes).
				Query: "SELECT pk, CHAR_LENGTH(t1), LENGTH(t1), CHAR_LENGTH(t5), LENGTH(t5) FROM t_ae_text_large ORDER BY pk",
				Expected: []sql.Row{
					{1, 15000, 30000, 15000, 30000},
					{2, 15000, 30000, 100, 200},
					{3, 10000, 30000, 200, 200},
				},
			},
			{
				// Row 1: all five columns outlined, all readable.
				Query:    "SELECT CHAR_LENGTH(t1), CHAR_LENGTH(t2), CHAR_LENGTH(t3), CHAR_LENGTH(t4), CHAR_LENGTH(t5) FROM t_ae_text_large WHERE pk = 1",
				Expected: []sql.Row{{15000, 15000, 15000, 15000, 15000}},
			},
			{
				// Row 2: t5 stays inline (100 'ñ' chars = 200 bytes).
				Query:    "SELECT pk FROM t_ae_text_large WHERE t5 = REPEAT('ñ', 100)",
				Expected: []sql.Row{{2}},
			},
			{
				// Row 2: t4 outlined in BuildPermissive; still readable.
				Query:    "SELECT pk FROM t_ae_text_large WHERE t4 = REPEAT('ñ', 1000)",
				Expected: []sql.Row{{2}},
			},
			{
				// Row 3: t1 has 10000 '€' chars = 30000 bytes; t3 is NULL.
				Query:    "SELECT CHAR_LENGTH(t1), LENGTH(t1), CHAR_LENGTH(t2), LENGTH(t2), LENGTH(t3) FROM t_ae_text_large WHERE pk = 3",
				Expected: []sql.Row{{10000, 30000, 100, 300, nil}},
			},
			{
				Query:    "SELECT pk FROM t_ae_text_large WHERE t1 = REPEAT('€', 10000)",
				Expected: []sql.Row{{3}},
			},
		},
	},
	{
		// Tests a table containing both LONGTEXT (StringAdaptiveEnc) and LONGBLOB
		// (BytesAdaptiveEnc) adaptive columns interleaved with non-adaptive INT columns.
		// Verifies that the two different adaptive encoding types coexist correctly in
		// the same tuple and that multibyte text characters are handled as bytes.
		Name: "mixed LONGTEXT and LONGBLOB adaptive columns with multibyte text",
		SetUpScript: []string{
			`CREATE TABLE t_ae_mixed (
				pk INT NOT NULL PRIMARY KEY,
				a  INT,
				t  LONGTEXT CHARACTER SET utf8mb4,
				b  INT,
				bl LONGBLOB,
				c  INT
			)`,
			// Value tuple: a(4), t(adaptive text), b(4), bl(adaptive blob), c(4).
			// inlineSize = 4 + (1+byte_len_t) + 4 + (1+len_bl) + 4 = byte_len_t+len_bl+14.

			// Both inline: t=1000 ASCII chars (1000 bytes), bl=1000 bytes.
			// total = 1000+1000+14 = 2014 ≤ 2048.
			`INSERT INTO t_ae_mixed VALUES (1, 1, REPEAT('a', 1000), 2, REPEAT('x', 1000), 3)`,

			// t outlined (2-byte chars): t=1024 'ñ' chars = 2048 bytes, bl=100 bytes.
			// total = 2048+100+14 = 2162 > 2048.
			// savings t=2048, savings bl=80; t outlined first.
			`INSERT INTO t_ae_mixed VALUES (2, 1, REPEAT('ñ', 1024), 2, REPEAT('x', 100), 3)`,

			// bl outlined: t=100 ASCII chars (100 bytes), bl=3000 bytes.
			// total = 100+3000+14 = 3114 > 2048.
			// savings bl=2980, savings t=80; bl outlined first.
			`INSERT INTO t_ae_mixed VALUES (3, 1, REPEAT('a', 100), 2, REPEAT('x', 3000), 3)`,

			// Both outlined: t=3000 'ñ' chars (6000 bytes), bl=3000 bytes.
			// Both immediately OOB (individual sizes >> 2048).
			`INSERT INTO t_ae_mixed VALUES (4, 1, REPEAT('ñ', 3000), 2, REPEAT('x', 3000), 3)`,

			// NULL text: only bl contributes to inline size.
			`INSERT INTO t_ae_mixed VALUES (5, 1, NULL, 2, REPEAT('x', 1500), 3)`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query: "SELECT pk, CHAR_LENGTH(t), LENGTH(t), LENGTH(bl) FROM t_ae_mixed ORDER BY pk",
				Expected: []sql.Row{
					{1, 1000, 1000, 1000},
					{2, 1024, 2048, 100},
					{3, 100, 100, 3000},
					{4, 3000, 6000, 3000},
					{5, nil, nil, 1500},
				},
			},
			{
				// pk=2: t (2-byte chars, outlined) and bl (inline) both readable.
				Query:    "SELECT pk FROM t_ae_mixed WHERE t = REPEAT('ñ', 1024) AND bl = REPEAT('x', 100)",
				Expected: []sql.Row{{2}},
			},
			{
				// pk=3: bl (outlined) and t (inline) both readable.
				Query:    "SELECT pk FROM t_ae_mixed WHERE bl = REPEAT('x', 3000) AND t = REPEAT('a', 100)",
				Expected: []sql.Row{{3}},
			},
			{
				// pk=4: both t (6000 bytes, 3000 'ñ') and bl (3000 bytes) outlined.
				Query:    "SELECT pk FROM t_ae_mixed WHERE t = REPEAT('ñ', 3000) AND bl = REPEAT('x', 3000)",
				Expected: []sql.Row{{4}},
			},
			{
				// pk=5: NULL text does not interfere with bl outlining.
				Query:    "SELECT pk FROM t_ae_mixed WHERE t IS NULL AND bl = REPEAT('x', 1500)",
				Expected: []sql.Row{{5}},
			},
			{
				// Non-adaptive columns (a, b, c) are unaffected by adaptive encoding.
				Query:    "SELECT a, b, c FROM t_ae_mixed WHERE pk = 2",
				Expected: []sql.Row{{1, 2, 3}},
			},
		},
	},
}

func MakeBigAdaptiveEncodingWriteQueries(columnType AdaptiveEncodingTestColumnType, testPurpose AdaptiveEncodingTestPurpose) []queries.WriteQueryTest {
	var fullSize interface{}
	var halfSize interface{}
	var tiny interface{}
	if columnType == AdaptiveEncodingTestType_Blob {
		fullSize = fullSizeBytes
		halfSize = halfSizeBytes
		tiny = tinyBytes
	} else {
		// columnType == AdaptiveEncodingTestType_Text
		fullSize = fullSizeString
		halfSize = halfSizeString
		tiny = tinyString
	}

	var fullSizeOutOfLineRepr interface{}
	var wrapBehavior queries.WrapBehavior
	if testPurpose == AdaptiveEncodingTestPurpose_Representation {
		wrapBehavior = queries.WrapBehavior_Hash
		fullSizeOutOfLineRepr = fullSizeHash
	} else {
		// testPurpose == AdaptiveEncodingTestPurpose_Correctness
		// For this test, always unwrap values and expect their normalized form (either bytes or string, never hash)
		wrapBehavior = queries.WrapBehavior_Unwrap
		fullSizeOutOfLineRepr = fullSize
	}

	return []queries.WriteQueryTest{
		{
			// Tuples containing adaptive columns should be independent of how the tuple was created.
			// And adaptive values are always outlined starting from the left.
			// This means that in a table with two adaptive columns where both columns were previously stored out-of line,
			// Decreasing the size of the second column may allow both columns to be stored inline.
			WriteQuery:          "UPDATE blobt2 SET b2 = LOAD_FILE('testdata/tinyFile') WHERE i = 'HH'",
			ExpectedWriteResult: []sql.Row{{queries.NewUpdateResult(1, 1)}},
			SelectQuery:         "select i, b1, b2 from blobt2 where i = 'HH'",
			WrapBehavior:        wrapBehavior,
			ExpectedSelect:      []sql.Row{{"HH", halfSize, tiny}},
		},
		{
			// Similar to the above, dropping a column can change whether the other column is inlined.
			WriteQuery:          "ALTER TABLE blobt2 DROP COLUMN b2",
			ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
			SelectQuery:         "select i, b1 from blobt2",
			WrapBehavior:        wrapBehavior,
			ExpectedSelect: []sql.Row{
				{"FF", fullSizeOutOfLineRepr},
				{"HF", halfSize},
				{"TF", tiny},
				{"NF", nil},
				{"FH", fullSizeOutOfLineRepr},
				{"HH", halfSize},
				{"TH", tiny},
				{"NH", nil},
				{"FT", fullSizeOutOfLineRepr},
				{"HT", halfSize},
				{"TT", tiny},
				{"NT", nil},
				{"FN", fullSizeOutOfLineRepr},
				{"HN", halfSize},
				{"TN", tiny},
				{"NN", nil}},
		},
		{
			// Test creating an index on an adaptive encoding column, matching against out-of-band values
			WriteQuery:          "CREATE INDEX bidx ON blobt2 (b1(10))",
			ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
			SelectQuery:         "select i, b1 FROM blobt2 WHERE BINARY b1 LIKE '\\0%'",
			WrapBehavior:        wrapBehavior,
			ExpectedSelect: []sql.Row{
				{"FF", fullSizeOutOfLineRepr},
				{"FH", fullSizeOutOfLineRepr},
				{"FT", fullSizeOutOfLineRepr},
				{"FN", fullSizeOutOfLineRepr},
			},
		},
		{
			// Test creating an index on an adaptive encoding column, matching against inline values
			WriteQuery:          "ALTER TABLE blobt2 ADD INDEX (b2(5))",
			ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
			SelectQuery:         "select i, b2 FROM blobt2 WHERE BINARY b2 LIKE '\x01%'",
			WrapBehavior:        wrapBehavior,
			ExpectedSelect: []sql.Row{
				{"FH", halfSize},
				{"HH", halfSize},
				{"TH", halfSize},
				{"NH", halfSize},
			},
		},
	}
}
