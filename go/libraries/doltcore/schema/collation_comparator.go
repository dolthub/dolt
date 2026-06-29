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

	"github.com/dolthub/dolt/go/store/val"

	"github.com/dolthub/go-mysql-server/sql"
)

type CollationTupleComparator struct {
	Collations []sql.CollationID // CollationIDs are implemented as uint16
	vs         val.ValueStore
}

var _ val.TupleComparator = CollationTupleComparator{}

// Compare implements TupleComparator
func (c CollationTupleComparator) Compare(ctx context.Context, left, right val.Tuple, desc *val.TupleDesc) (cmp int, err error) {
	fast := desc.GetFixedAccess()
	off := len(fast)
	var start, stop val.ByteSize
	for i := 0; i < off; i++ {
		stop = fast[i]
		cmp, err = collationCompare(ctx, desc.Types[i], c.Collations[i], left[start:stop], right[start:stop], c.vs)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
		start = stop
	}

	for i, typ := range desc.Types[off:] {
		j := i + off
		cmp, err = collationCompare(ctx, typ, c.Collations[j], left.GetField(j), right.GetField(j), c.vs)
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			return cmp, nil
		}
	}
	return
}

// CompareValues implements TupleComparator
func (c CollationTupleComparator) CompareValues(ctx context.Context, index int, left, right []byte, typ val.Type) (int, error) {
	return collationCompare(ctx, typ, c.Collations[index], left, right, c.vs)
}

// Prefix implements TupleComparator
func (c CollationTupleComparator) Prefix(n int) val.TupleComparator {
	newCollations := make([]sql.CollationID, n)
	copy(newCollations, c.Collations)
	return CollationTupleComparator{Collations: newCollations, vs: c.vs}
}

// Suffix implements TupleComparator
func (c CollationTupleComparator) Suffix(n int) val.TupleComparator {
	newCollations := make([]sql.CollationID, n)
	copy(newCollations, c.Collations[len(c.Collations)-n:])
	return CollationTupleComparator{Collations: newCollations, vs: c.vs}
}

// Validated implements TupleComparator
func (c CollationTupleComparator) Validated(types []val.Type) val.TupleComparator {
	if len(c.Collations) > len(types) {
		panic("too many collations compared to type encoding")
	}
	i := 0
	for ; i < len(c.Collations); i++ {
		if isCollatedStringEnc(types[i].Enc) && c.Collations[i] == sql.Collation_Unspecified {
			c.Collations[i] = sql.Collation_Default
		}
	}
	if len(c.Collations) == len(types) {
		return c
	}
	newCollations := make([]sql.CollationID, len(types))
	copy(newCollations, c.Collations)
	for ; i < len(newCollations); i++ {
		if isCollatedStringEnc(types[i].Enc) {
			panic("string type encoding is missing its collation")
		}
		if isCollatedStringEnc(types[i].Enc) {
			newCollations[i] = sql.Collation_Default
		} else {
			newCollations[i] = sql.Collation_Unspecified
		}
	}
	return CollationTupleComparator{Collations: newCollations, vs: c.vs}
}

func isCollatedStringEnc(enc val.Encoding) bool {
	return enc == val.StringEnc || enc == val.StringAdaptiveEnc
}

// WithValueStore implements TupleComparator
func (c CollationTupleComparator) WithValueStore(vs val.ValueStore) val.TupleComparator {
	return CollationTupleComparator{Collations: c.Collations, vs: vs}
}

func collationCompare(ctx context.Context, typ val.Type, collation sql.CollationID, left, right []byte, vs val.ValueStore) (int, error) {
	// order NULLs first
	if left == nil || right == nil {
		if bytes.Equal(left, right) {
			return 0, nil
		} else if left == nil {
			return -1, nil
		} else {
			return 1, nil
		}
	}

	switch typ.Enc {
	case val.StringEnc:
		return val.CompareCollatedStrings(collation, left[:len(left)-1], right[:len(right)-1]), nil
	case val.StringAdaptiveEnc:
		return vs.CompareAdaptiveCollatedStrings(ctx, left, right, collation)
	default:
		return (&val.DefaultTupleComparator{}).WithValueStore(vs).CompareValues(ctx, 0, left, right, typ)
	}
}
