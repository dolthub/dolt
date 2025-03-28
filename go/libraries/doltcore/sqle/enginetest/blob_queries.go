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
var fullSizeHash = hash.Hash{0x69, 0xe1, 0x2f, 0x49, 0xd1, 0x5e, 0xc0, 0x7f, 0x8f, 0x59, 0xab, 0xac, 0xb0, 0xf, 0x76, 0x3e, 0x7d, 0xf6, 0x5e, 0xb5}

// A 2000 byte file starting with 0x01 and then consisting of all zeros.
// This is over half of the default target tuple size for outlining adaptive types.
// We expect a tuple to be able to store this value inline once, but not twice.
var halfSizeBytes = makeTestBytes(2000, 1)
var halfSizeHash = hash.Hash{0x31, 0x0c, 0x97, 0x33, 0x73, 0xa8, 0xbe, 0xcb, 0xbf, 0xc8, 0x83, 0x3a, 0xbf, 0x51, 0x40, 0x8a, 0x7e, 0x43, 0x1a, 0xf1}

// A 10 byte file starting with 0x02 and then consisting of 10 zero bytes.
// This is file is smaller than an address hash.
// We expect a tuple to never store this value out-of-band.
var tinyBytes = makeTestBytes(10, 2)

var BigAdaptiveBlobQueriesSetup = []setup.SetupScript{{
	`create table blobt (i char(1) primary key, b longblob);`,
	`create table blobt2 (i char(2) primary key, b1 longblob, b2 longblob);`,
	`insert into blobt values
    ('F', LOAD_FILE('testdata/fullSize')),
    ('H', LOAD_FILE('testdata/halfSize')),
    ('T', LOAD_FILE('testdata/tinyFile'))`,
	`insert into blobt2 values
    ('FF', LOAD_FILE('testdata/fullSize'), LOAD_FILE('testdata/fullSize')),
    ('HF', LOAD_FILE('testdata/halfSize'), LOAD_FILE('testdata/fullSize')),
    ('TF', LOAD_FILE('testdata/tinyFile'), LOAD_FILE('testdata/fullSize')),
	('FH', LOAD_FILE('testdata/fullSize'), LOAD_FILE('testdata/halfSize')),
	('HH', LOAD_FILE('testdata/halfSize'), LOAD_FILE('testdata/halfSize')),
	('TH', LOAD_FILE('testdata/tinyFile'), LOAD_FILE('testdata/halfSize')),
    ('FT', LOAD_FILE('testdata/fullSize'), LOAD_FILE('testdata/tinyFile')),
    ('HT', LOAD_FILE('testdata/halfSize'), LOAD_FILE('testdata/tinyFile')),
    ('TT', LOAD_FILE('testdata/tinyFile'), LOAD_FILE('testdata/tinyFile'))`,
}}

var BigBlobWriteQueries = []queries.WriteQueryTest{
	{
		WriteQuery:          "INSERT INTO blobt VALUES(4, LOAD_FILE('testdata/test1.png'))",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(1)}},
		SelectQuery:         "select sha1(b) from blobt where i = 4",
		ExpectedSelect:      []sql.Row{{"012bcb75a319f2913614a5170fc046fb6c49ee86"}},
	},
}

var BigAdaptiveBlobQueries = []queries.QueryTest{
	{
		// Large files are always stored out-of-band
		Query:        "select b from blobt where i = 'F'",
		Expected:     []sql.Row{{fullSizeHash}},
		WrapBehavior: queries.WrapBehavior_Hash,
	},
	{
		// Files that can fit within a tuple are stored inline.
		Query:        "select b from blobt where i = 'H'",
		Expected:     []sql.Row{{halfSizeBytes}},
		WrapBehavior: queries.WrapBehavior_Hash,
	},
	{
		// When a tuple with multiple adaptive columns is too large, columns are moved out-of-band from left to right.
		// However, strings smaller than the address size (20 bytes) are never stored out-of-band.
		Query:        "select i, b1, b2 from blobt2",
		WrapBehavior: queries.WrapBehavior_Hash,
		Expected: []sql.Row{
			{"FF", fullSizeHash, fullSizeHash},
			{"HF", halfSizeHash, fullSizeHash},
			{"TF", tinyBytes, fullSizeHash},
			{"FH", fullSizeHash, halfSizeBytes},
			{"HH", halfSizeHash, halfSizeBytes},
			{"TH", tinyBytes, halfSizeBytes},
			{"FT", fullSizeHash, tinyBytes},
			{"HT", halfSizeBytes, tinyBytes},
			{"TT", tinyBytes, tinyBytes},
		},
	},
	{
		// An inlined adaptive column can be used in a filter.
		Query:        "select i from blobt where b = LOAD_FILE('testdata/fullSize')",
		WrapBehavior: queries.WrapBehavior_Hash,
		Expected:     []sql.Row{{"F"}},
	},
	{
		// An out-of-line adaptive column can be used in a filter.
		Query:        "select i from blobt where b = LOAD_FILE('testdata/halfSize')",
		WrapBehavior: queries.WrapBehavior_Hash,
		Expected:     []sql.Row{{"H"}},
	},
	{
		// An adaptive column can be used in a filter when it doesn't have the same encoding in all rows.
		Query:        "select i from blobt2 where b1 = LOAD_FILE('testdata/halfSize')",
		WrapBehavior: queries.WrapBehavior_Hash,
		Expected:     []sql.Row{{"HF"}, {"HH"}, {"HT"}},
	},
	{
		// An adaptive column can be used in a filter when it doesn't have the same encoding in all rows.
		Query:        "select i from blobt2 where b2 = LOAD_FILE('testdata/halfSize')",
		WrapBehavior: queries.WrapBehavior_Hash,
		Expected:     []sql.Row{{"FH"}, {"HH"}, {"TH"}},
	},
}

var BigAdaptiveBlobWriteQueries = []queries.WriteQueryTest{
	{
		// Tuples containing adaptive columns should be independent of how the tuple was created.
		// And adaptive values are always outlined starting from the left.
		// This means that in a table with two adaptive columns where both columns were previously stored out-of line,
		// Decreasing the size of the second column may allow both columns to be stored inline.
		WriteQuery:          "UPDATE blobt2 SET b2 = LOAD_FILE('testdata/tinyFile') WHERE i = 'HH'",
		ExpectedWriteResult: []sql.Row{{queries.NewUpdateResult(1, 1)}},
		SelectQuery:         "select i, b1, b2 from blobt2 where i = 'HH'",
		WrapBehavior:        queries.WrapBehavior_Hash,
		ExpectedSelect:      []sql.Row{{"HH", halfSizeBytes, tinyBytes}},
	},
	{
		// Similar to the above, dropping a column can change whether the other column is inlined.
		WriteQuery:          "ALTER TABLE blobt2 DROP COLUMN b2",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "select i, b1 from blobt2",
		WrapBehavior:        queries.WrapBehavior_Hash,
		ExpectedSelect: []sql.Row{
			{"FF", fullSizeHash},
			{"HF", halfSizeBytes},
			{"TF", tinyBytes},
			{"FH", fullSizeHash},
			{"HH", halfSizeBytes},
			{"TH", tinyBytes},
			{"FT", fullSizeHash},
			{"HT", halfSizeBytes},
			{"TT", tinyBytes}},
	},
	{
		// Test creating an index on an adaptive encoding column, matching against out-of-band values
		WriteQuery:          "ALTER TABLE blobt2 ADD INDEX (b1(10))",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "select i, b1 FROM blobt2 WHERE BINARY b1 LIKE '\\0%'",
		WrapBehavior:        queries.WrapBehavior_Hash,
		ExpectedSelect: []sql.Row{
			{"FF", fullSizeHash},
			{"FH", fullSizeHash},
			{"FT", fullSizeHash},
		},
	},
	{
		// Test creating an index on an adaptive encoding column, matching against inline values
		WriteQuery:          "ALTER TABLE blobt2 ADD INDEX (b2(5))",
		ExpectedWriteResult: []sql.Row{{types.NewOkResult(0)}},
		SelectQuery:         "select i, b2 FROM blobt2 WHERE BINARY b2 LIKE '\x01%'",
		WrapBehavior:        queries.WrapBehavior_Hash,
		ExpectedSelect: []sql.Row{
			{"FH", halfSizeBytes},
			{"HH", halfSizeBytes},
			{"TH", halfSizeBytes},
		},
	},
}
