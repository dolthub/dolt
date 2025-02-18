// Copyright 2022 Dolthub, Inc.
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
	"bytes"
	"context"
	"unicode/utf8"

	"github.com/dolthub/dolt/go/store/val"

	"github.com/dolthub/go-mysql-server/sql"
)

type CollationTupleComparator struct {
	Collations []sql.CollationID // CollationIDs are implemented as uint16
}

var _ val.TupleComparator = CollationTupleComparator{}

// Compare implements TupleComparator
func (c CollationTupleComparator) Compare(ctx context.Context, left, right val.Tuple, desc val.TupleDesc) (cmp int) {
	fast := desc.GetFixedAccess()
	for i := range fast {
		start, stop := fast[i][0], fast[i][1]
		cmp = collationCompare(ctx, desc.Types[i], c.Collations[i], left[start:stop], right[start:stop])
		if cmp != 0 {
			return cmp
		}
	}

	off := len(fast)
	for i, typ := range desc.Types[off:] {
		j := i + off
		cmp = collationCompare(ctx, typ, c.Collations[j], left.GetField(j), right.GetField(j))
		if cmp != 0 {
			return cmp
		}
	}
	return
}

// CompareValues implements TupleComparator
func (c CollationTupleComparator) CompareValues(ctx context.Context, index int, left, right []byte, typ val.Type) int {
	return collationCompare(ctx, typ, c.Collations[index], left, right)
}

// Prefix implements TupleComparator
func (c CollationTupleComparator) Prefix(n int) val.TupleComparator {
	newCollations := make([]sql.CollationID, n)
	copy(newCollations, c.Collations)
	return CollationTupleComparator{newCollations}
}

// Suffix implements TupleComparator
func (c CollationTupleComparator) Suffix(n int) val.TupleComparator {
	newCollations := make([]sql.CollationID, n)
	copy(newCollations, c.Collations[len(c.Collations)-n:])
	return CollationTupleComparator{newCollations}
}

// Validated implements TupleComparator
func (c CollationTupleComparator) Validated(types []val.Type) val.TupleComparator {
	if len(c.Collations) > len(types) {
		panic("too many collations compared to type encoding")
	}
	i := 0
	for ; i < len(c.Collations); i++ {
		if types[i].Enc == val.StringEnc && c.Collations[i] == sql.Collation_Unspecified {
			c.Collations[i] = sql.Collation_Default
		}
	}
	if len(c.Collations) == len(types) {
		return c
	}
	newCollations := make([]sql.CollationID, len(types))
	copy(newCollations, c.Collations)
	for ; i < len(newCollations); i++ {
		if types[i].Enc == val.StringEnc {
			panic("string type encoding is missing its collation")
		}
		newCollations[i] = sql.Collation_Unspecified
	}
	return CollationTupleComparator{Collations: newCollations}
}

func collationCompare(ctx context.Context, typ val.Type, collation sql.CollationID, left, right []byte) int {
	// order NULLs first
	if left == nil || right == nil {
		if bytes.Equal(left, right) {
			return 0
		} else if left == nil {
			return -1
		} else {
			return 1
		}
	}

	if typ.Enc == val.StringEnc {
		return compareCollatedStrings(collation, left[:len(left)-1], right[:len(right)-1])
	} else {
		return val.DefaultTupleComparator{}.CompareValues(ctx, 0, left, right, typ)
	}
}

func compareCollatedStrings(collation sql.CollationID, left, right []byte) int {
	i := 0
	for i < len(left) && i < len(right) {
		if left[i] != right[i] {
			break
		}
		i++
	}
	if i >= len(left) || i >= len(right) {
		if len(left) < len(right) {
			return -1
		} else if len(left) > len(right) {
			return 1
		} else {
			return 0
		}
	}

	li := i
	for ; li > 0 && !utf8.RuneStart(left[li]); li-- {
	}
	left = left[li:]

	ri := i
	for ; ri > 0 && !utf8.RuneStart(right[ri]); ri-- {
	}
	right = right[ri:]

	getRuneWeight := collation.Sorter()
	for len(left) > 0 && len(right) > 0 {
		// Binary strings aren't handled through this function, so it is safe to use the utf8 functions
		leftRune, leftRead := utf8.DecodeRune(left)
		rightRune, rightRead := utf8.DecodeRune(right)
		if leftRead == utf8.RuneError || rightRead == utf8.RuneError {
			// Malformed strings sort after well-formed strings, and we consider two malformed strings to be equal
			if leftRead == utf8.RuneError && rightRead != utf8.RuneError {
				return 1
			} else if leftRead != utf8.RuneError && rightRead == utf8.RuneError {
				return -1
			} else {
				return 0
			}
		}
		if leftRune != rightRune {
			leftWeight := getRuneWeight(leftRune)
			rightWeight := getRuneWeight(rightRune)
			if leftWeight < rightWeight {
				return -1
			} else if leftWeight > rightWeight {
				return 1
			}
		}
		left = left[leftRead:]
		right = right[rightRead:]
	}

	// Strings are equal up to the compared length, so shorter strings sort before longer strings
	if len(left) < len(right) {
		return -1
	} else if len(left) > len(right) {
		return 1
	} else {
		return 0
	}
}
