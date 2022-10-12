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

package diff_test

import (
	"testing"

	"github.com/dolthub/dolt/go/store/val"
)

type identityTest struct {
	name    string
	left    []table
	right   []table
	matches []match
	// non-matching tables omitted
}

type match struct {
	leftTbl, rightTbl string
	columnMatches     [][2]string
	// non-matching columns omitted
}

type table struct {
	name string
	cols []column
}

type column struct {
	name string
	enc  val.Encoding
	pk   bool

	// simulates heuristic column matching
	// based on sampling fields from row data
	sample []int
}

const (
	heuristicMatchThreshold = 0.5
)

// Table matching follows a conservative algorithm:
// matching tables must have the same name and the same set
// of primary key column types (empty set for keyless tables).
//
// This algorithm could be extended to handle table renames
// by matching tables with equal primary key column types
// based on a heuristic sampling method. We could also expose
// user-defined mappings that manually specify table matches.
func TestTableMatching(t *testing.T) {
	var tests = []identityTest{
		{
			name: "smoke test",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.Int32Enc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.Int32Enc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "pk"},
						{"c0", "c0"},
					},
				},
			},
		},
		{
			name: "primary key rename",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "a", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.Int32Enc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "b", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.Int32Enc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "a"},
						{"c0", "c0"},
					},
				},
			},
		},
		{
			name: "keyless table",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int32Enc},
						{name: "c1", enc: val.Int32Enc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int32Enc},
						{name: "c1", enc: val.Int32Enc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"c0", "c0"},
						{"c1", "c1"},
					},
				},
			},
		},
		{
			name: "table rename",
			left: []table{
				{
					name: "t1",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.Int32Enc},
					},
				},
			},
			right: []table{
				{
					name: "t2",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.Int32Enc},
					},
				},
			},
			matches: []match{ /* no matches */ },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testIdentity(t, test)
		})
	}
}

// Column matching follows table matching,
// primary keys have already been matched.
// Matching for non-primary-key is as follows:
//  1. equal name and type are matched
//     2a. keyless tables take union of remaining columns
//     2b. pk tables attempt to heuristically match remaining
//     columns of equal types by sampling rows values
func TestColumnMatching(t *testing.T) {
	var tests = []identityTest{
		{
			name: "extra unmatched columns",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "a", enc: val.DatetimeEnc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "b", enc: val.GeometryEnc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "pk"},
						// columns 'a', 'b' unmatched
					},
				},
			},
		},
		{
			name: "unmatched columns with name collision",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.YearEnc},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "c0", enc: val.JSONEnc},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "pk"},
						// columns 'c0', 'c0' unmatched
					},
				},
			},
		},
		{
			name: "heuristic column matching",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "a", enc: val.Int64Enc, sample: []int{1, 2, 3, 4, 5}},
						{name: "b", enc: val.Int64Enc, sample: []int{6, 7, 8, 9, 10}},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "pk", enc: val.Int32Enc, pk: true},
						{name: "x", enc: val.Int64Enc, sample: []int{1, 2, 3, -4, -5}},
						{name: "y", enc: val.Int64Enc, sample: []int{6, 7, -8, -9, -10}},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"pk", "pk"},
						{"a", "x"},
						// columns 'b', 'y' unmatched
					},
				},
			},
		},
		{
			name: "keyless table union",
			left: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int32Enc, sample: []int{1, 2, 3, 4}},
						{name: "c1", enc: val.Int32Enc, sample: []int{5, 6, 7, 8}},
					},
				},
			},
			right: []table{
				{
					name: "t",
					cols: []column{
						{name: "c0", enc: val.Int32Enc, sample: []int{1, 2, 3, 4}},
						{name: "c2", enc: val.Int32Enc, sample: []int{5, 6, 7, 8}},
					},
				},
			},
			matches: []match{
				{
					leftTbl: "t", rightTbl: "t",
					columnMatches: [][2]string{
						{"c0", "c0"},
						// columns 'c1', 'c2' unmatched
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testIdentity(t, test)
		})
	}
}

func testIdentity(t *testing.T, test identityTest) {
	t.Skip("implement me")
}
