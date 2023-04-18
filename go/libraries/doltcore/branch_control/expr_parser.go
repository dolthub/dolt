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

package branch_control

import (
	"math"
	"sync"
	"unicode/utf8"

	"github.com/dolthub/go-mysql-server/sql"
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

const (
	singleMatch  = -1 // Equivalent to the single match character '_'
	anyMatch     = -2 // Equivalent to the any-length match character '%'
	columnMarker = -3 // Marks the start of a new column
)

// invalidMatchExpression is a match expression that does not match anything
var invalidMatchExpression = MatchExpression{math.MaxUint32, nil}

// matchExprPool is a pool for MatchExpression slices. Provides a significant performance benefit.
var matchExprPool = &sync.Pool{
	New: func() any {
		return make([]MatchExpression, 0, 32)
	},
}

// indexPool is a pool for index slices (such as those returned by Match). Provides a decent performance benefit.
var indexPool = &sync.Pool{
	New: func() any {
		return make([]uint32, 0, 32)
	},
}

// MatchExpression represents a parsed expression that may be matched against. It contains a list of sort orders, which
// each represent a comparable value to determine whether any given character is a match. A character's sort order is
// obtained from a collation. Also contains its index in the table. MatchExpression contents are not meant to be
// comparable to one another, therefore please use the index to compare equivalence.
type MatchExpression struct {
	CollectionIndex uint32  // CollectionIndex represents this expression's index in its parent slice.
	SortOrders      []int32 // These are the sort orders that will be compared against when matching a given rune.
}

// FoldExpression folds the given expression into its smallest form. Expressions have two wildcard operators:
// '_' and '%'. '_' matches exactly one character, and it can be any character. '%' can match zero or more of any
// character. Taking these two ops into account, the configurations "%_" and "_%" both resolve to matching one or more
// of any character. However, the "_%" form is more economical, as you enforce the single match first before checking
// for remaining matches. Similarly, "%%" is equivalent to a single '%'. Both of these rules are applied in this
// function, guaranteeing that the returned expression is the smallest form that still exactly represents the original.
//
// This also assumes that '\' is the escape character.
func FoldExpression(str string) string {
	// This loop only terminates when we complete a run where no substitutions were made. Substitutions are applied
	// linearly, therefore it's possible that one substitution may create an opportunity for another substitution.
	// To keep the code simple, we continue looping until we have nothing more to do.
	for true {
		newStrRunes := make([]rune, 0, len(str))
		// Skip next is set whenever we encounter the escape character, which is used to explicitly match against '_' and '%'
		skipNext := false
		// Consider next is set whenever we encounter an unescaped '%', indicating we may need to apply the substitutions
		considerNext := false
		for _, r := range str {
			if skipNext {
				skipNext = false
				newStrRunes = append(newStrRunes, r)
				continue
			} else if considerNext {
				considerNext = false
				switch r {
				case '\\':
					newStrRunes = append(newStrRunes, '%', r) // False alarm, reinsert % before this rune
					skipNext = true                           // We also need to ignore the next rune
				case '_':
					newStrRunes = append(newStrRunes, r, '%') // Replacing %_ with _%
				case '%':
					newStrRunes = append(newStrRunes, r) // Replacing %% with %
				default:
					newStrRunes = append(newStrRunes, '%', r) // False alarm, reinsert % before this rune
				}
				continue
			}

			switch r {
			case '\\':
				newStrRunes = append(newStrRunes, r)
				skipNext = true
			case '%':
				considerNext = true
			default:
				newStrRunes = append(newStrRunes, r)
			}
		}
		// If the very last rune is '%', then this will be true and we need to append it to the end
		if considerNext {
			newStrRunes = append(newStrRunes, '%')
		}
		newStr := string(newStrRunes)
		if str == newStr {
			break
		}
		str = newStr
	}
	return str
}

// ParseExpression parses the given string expression into a slice of sort ints, which will be used in a MatchExpression.
// Returns nil if the string is too long. Assumes that the given string expression has already been folded.
func ParseExpression(str string, collation sql.CollationID) []int32 {
	if len(str) > math.MaxUint16 {
		return nil
	}

	sortFunc := collation.Sorter()
	var orders []int32
	escaped := false
	for _, r := range str {
		if escaped {
			escaped = false
			orders = append(orders, sortFunc(r))
		} else {
			switch r {
			case '\\':
				escaped = true
			case '%':
				orders = append(orders, anyMatch)
			case '_':
				orders = append(orders, singleMatch)
			default:
				orders = append(orders, sortFunc(r))
			}
		}
	}
	return orders
}

// Match takes the match expression collection, and returns a slice of which collection indexes matched against the
// given string. The given indices may be used to further reduce the match expression collection, which will also reduce
// the total number of comparisons as they're narrowed down.
//
// It is vastly more performant to return a slice of collection indexes here, rather than a slice of match expressions.
// This is true even when the match expressions are pooled. The reason is unknown, but as we only need the collection
// indexes anyway, we discard the match expressions and return only their indexes.
func Match(matchExprCollection []MatchExpression, str string, collation sql.CollationID) []uint32 {
	sortFunc := collation.Sorter()
	// Grab the first rune and also remove it from the string
	r, rSize := utf8.DecodeRuneInString(str)
	str = str[rSize:]
	// Grab a slice from the pool, which reduces the GC pressure.
	matchSubset := matchExprPool.Get().([]MatchExpression)[:0]
	// We do a pass using the first rune over all expressions to get the subset that we'll be testing against
	for _, testExpr := range matchExprCollection {
		if matched, next, extra := testExpr.Matches(sortFunc(r)); matched {
			if extra.IsValid() {
				matchSubset = append(matchSubset, next, extra)
			} else {
				matchSubset = append(matchSubset, next)
			}
		}
	}
	// Bail early if there are no matches here
	if len(matchSubset) == 0 {
		matchExprPool.Put(matchSubset)
		// We return a slice from the index pool as we later will return it to the pool. We don't want to stick a
		// nil/empty slice into the pool.
		return indexPool.Get().([]uint32)[:0]
	}

	// This is the slice that we'll put matches into. This will also flip to become the match subset. This way we reuse
	// the underlying arrays. We also grab this from the pool.
	matches := matchExprPool.Get().([]MatchExpression)[:0]
	// Now that we have our set of expressions to test, we loop over the remainder of the input string
	for _, r = range str {
		for _, testExpr := range matchSubset {
			if matched, next, extra := testExpr.Matches(sortFunc(r)); matched {
				if extra.IsValid() {
					matches = append(matches, next, extra)
				} else {
					matches = append(matches, next)
				}
			}
		}
		// Swap the two, and put the slice of matches to be at the beginning of the previous subset array to reuse it
		matches, matchSubset = matchSubset[:0], matches
	}
	matchExprPool.Put(matches)

	// Grab the indices of all valid matches
	validMatches := indexPool.Get().([]uint32)[:0]
	for _, match := range matchSubset {
		if match.IsAtEnd() && (len(validMatches) == 0 ||
			(len(validMatches) > 0 && match.CollectionIndex != validMatches[len(validMatches)-1])) {
			validMatches = append(validMatches, match.CollectionIndex)
		}
	}
	matchExprPool.Put(matchSubset)
	return validMatches
}

// Matches returns true when the given sort order matches the expectation of the calling match expression. Returns a
// reduced match expression as `next`, which should take the place of the calling match function. In the event of a
// branch, returns the branching match expression as `extra`.
//
// Branches occur when the '%' operator sees that the given sort order matches the sort order after the '%'. As it
// cannot be determined which path is the correct one (whether to consume the '%' or continue using it), a branch is
// created. The `extra` should be checked for validity by calling IsValid.
func (matchExpr MatchExpression) Matches(sortOrder int32) (matched bool, next MatchExpression, extra MatchExpression) {
	if len(matchExpr.SortOrders) == 0 {
		return false, invalidMatchExpression, invalidMatchExpression
	}
	switch matchExpr.SortOrders[0] {
	case singleMatch:
		if sortOrder < singleMatch {
			return false, invalidMatchExpression, invalidMatchExpression
		}
		return true, MatchExpression{matchExpr.CollectionIndex, matchExpr.SortOrders[1:]}, invalidMatchExpression
	case anyMatch:
		if len(matchExpr.SortOrders) > 1 && matchExpr.SortOrders[1] == sortOrder {
			return true, matchExpr, MatchExpression{matchExpr.CollectionIndex, matchExpr.SortOrders[2:]}
		}
		return true, matchExpr, invalidMatchExpression
	default:
		if sortOrder == matchExpr.SortOrders[0] {
			return true, MatchExpression{matchExpr.CollectionIndex, matchExpr.SortOrders[1:]}, invalidMatchExpression
		} else {
			return false, invalidMatchExpression, invalidMatchExpression
		}
	}
}

// IsValid returns whether the match expression is valid. An invalid MatchExpression will have a collection index that
// is at the maximum value for an uint32.
func (matchExpr MatchExpression) IsValid() bool {
	return matchExpr.CollectionIndex < math.MaxUint32
}

// IsAtEnd returns whether the match expression has matched every character. There is a special case where, if the last
// character is '%', it is considered to be at the end.
func (matchExpr MatchExpression) IsAtEnd() bool {
	return len(matchExpr.SortOrders) == 0 || (len(matchExpr.SortOrders) == 1 && matchExpr.SortOrders[0] == anyMatch)
}

// Serialize returns the offset for the MatchExpression written to the given builder.
func (matchExpr MatchExpression) Serialize(b *flatbuffers.Builder) flatbuffers.UOffsetT {
	_ = serial.BranchControlMatchExpressionStartSortOrdersVector(b, len(matchExpr.SortOrders))
	for i := len(matchExpr.SortOrders) - 1; i >= 0; i-- {
		b.PrependInt32(matchExpr.SortOrders[i])
	}
	sortOrdersOffset := b.EndVector(len(matchExpr.SortOrders))

	serial.BranchControlMatchExpressionStart(b)
	serial.BranchControlMatchExpressionAddIndex(b, matchExpr.CollectionIndex)
	serial.BranchControlMatchExpressionAddSortOrders(b, sortOrdersOffset)
	return serial.BranchControlMatchExpressionEnd(b)
}

// deserializeMatchExpression populates the MatchExpression with the data from the flatbuffers representation.
func deserializeMatchExpression(fb *serial.BranchControlMatchExpression) MatchExpression {
	matchExpr := MatchExpression{
		CollectionIndex: fb.Index(),
		SortOrders:      make([]int32, fb.SortOrdersLength()),
	}
	for i := 0; i < fb.SortOrdersLength(); i++ {
		matchExpr.SortOrders[i] = fb.SortOrders(i)
	}
	return matchExpr
}
