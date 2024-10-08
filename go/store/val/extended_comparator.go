// Copyright 2024 Dolthub, Inc.
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

package val

// ExtendedTupleComparator is a comparator that properly handles extended types.
type ExtendedTupleComparator struct {
	innerCmp TupleComparator
	handlers []TupleTypeHandler
}

// TODO: compare performance of rolling this logic into the DefaultTupleComparator (nil check or generic handlers that call compare)
var _ TupleComparator = ExtendedTupleComparator{}

// Compare implements the TupleComparator interface.
func (c ExtendedTupleComparator) Compare(left, right Tuple, desc TupleDesc) (cmp int) {
	fast := desc.GetFixedAccess()
	for i := range fast {
		start, stop := fast[i][0], fast[i][1]
		cmp = c.CompareValues(i, left[start:stop], right[start:stop], desc.Types[i])
		if cmp != 0 {
			return cmp
		}
	}

	off := len(fast)
	for i, typ := range desc.Types[off:] {
		j := i + off
		cmp = c.CompareValues(j, left.GetField(j), right.GetField(j), typ)
		if cmp != 0 {
			return cmp
		}
	}
	return
}

// CompareValues implements the TupleComparator interface.
func (c ExtendedTupleComparator) CompareValues(index int, left, right []byte, typ Type) int {
	switch typ.Enc {
	case ExtendedEnc:
		cmp, err := c.handlers[index].SerializedCompare(left, right)
		if err != nil {
			panic(err)
		}
		return cmp

	case ExtendedAddrEnc:
		// For address encodings, we can't directly compare the content since we don't have a NodeStore reference
		// to look up value in the address, so we just compare the content hash. This is enough to tell us if
		// values are equivalent, but not enough to give correct sorting by content. This is the same behavior as
		// the default TupleComparator used for Dolt, but it prevents us from using TEXT types in secondary indexes
		// without first applying an implicit/hidden prefix length.
		return compareAddr(readAddr(left), readAddr(right))

	default:
		return compare(typ, left, right)
	}
}

// Prefix implements the TupleComparator interface.
func (c ExtendedTupleComparator) Prefix(n int) TupleComparator {
	return ExtendedTupleComparator{c.innerCmp.Prefix(n), c.handlers[:n]}
}

// Suffix implements the TupleComparator interface.
func (c ExtendedTupleComparator) Suffix(n int) TupleComparator {
	return ExtendedTupleComparator{c.innerCmp.Suffix(n), c.handlers[n:]}
}

// Validated implements the TupleComparator interface.
func (c ExtendedTupleComparator) Validated(types []Type) TupleComparator {
	// If our inner comparator is an ExtendedTupleComparator, then we should use its inner comparator to reduce redundancy.
	var innerCmp TupleComparator
	if extendedInner, ok := c.innerCmp.(ExtendedTupleComparator); ok {
		innerCmp = extendedInner.innerCmp.Validated(types)
	} else {
		innerCmp = c.innerCmp.Validated(types)
	}
	if len(c.handlers) == 0 {
		c.handlers = make([]TupleTypeHandler, len(types))
	} else if len(c.handlers) != len(types) {
		panic("invalid handler count compared to types")
	}
	hasHandler := false
	for i, handler := range c.handlers {
		switch types[i].Enc {
		case ExtendedEnc, ExtendedAddrEnc:
			if handler == nil {
				panic("extended encoding requires a handler")
			} else {
				hasHandler = true
			}
		}
	}
	if !hasHandler {
		return innerCmp
	}
	return ExtendedTupleComparator{innerCmp, c.handlers}
}
