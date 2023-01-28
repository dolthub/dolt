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

package branch_control

import (
	"math"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	aiciSorter = sql.Collation_utf8mb4_0900_ai_ci.Sorter()
	sortFuncs  = []func(r rune) int32{aiciSorter, aiciSorter, sql.Collation_utf8mb4_0900_bin.Sorter(), aiciSorter}
)

// MatchNode contains a collection of sort orders that allow for an optimized level of traversal compared to
// MatchExpression due to the sharing of like sort orders, reducing the overall number of comparisons needed.
type MatchNode struct {
	SortOrders  []int32              // These are the sort orders that will be compared against when matching a given rune.
	Divergences map[int32]*MatchNode // These are the children of this node that each represent a divergence in the sort orders.
	Data        *MatchNodeData       // This is the collection of data that the node holds. Will be nil if it's not a destination node.
}

// MatchNodeData is the data contained in a destination MatchNode.
type MatchNodeData struct {
	Permissions Permissions
	RowIndex    uint32
}

// MatchResult contains the data and expression length of a successful match.
type MatchResult struct {
	MatchNodeData
	Count uint32
}

// matchNodeCounted is an intermediary node used while processing matches that records the length of the match so far.
// This may be used to distinguish between which matches are the longest.
type matchNodeCounted struct {
	MatchNode
	Count uint32
}

// matchNodeCountedPool is a pool for MatchNodeCounted.
var matchNodeCountedPool = &sync.Pool{
	New: func() any {
		return make([]matchNodeCounted, 0, 16)
	},
}

// concatenatedSortOrderPool is a pool for concatenated sort orders.
var concatenatedSortOrderPool = &sync.Pool{
	New: func() any {
		return make([]int32, 0, 128)
	},
}

// Match returns a collection of results based on the given strings or expressions. When the parameters represent
// standard strings, then this simply matches those strings against the parsed expressions. However, if the parameters
// represent expressions, then this matches against all parsed expressions that are either duplicates or supersets of
// the given expressions. This allows the user to "match" against new expressions to see if they are already covered.
func (mn *MatchNode) Match(database, branch, user, host string) []MatchResult {
	allSortOrders := mn.parseExpression(database, branch, user, host)
	defer func() {
		concatenatedSortOrderPool.Put(allSortOrders)
	}()

	// This is the slice that we'll put matches into. This will also flip to become the match subset. This way we reuse
	// the underlying arrays. We grab this from the pool. These are not pointers, as we modify the data inside to
	// simplify the loop's logic.
	matches := matchNodeCountedPool.Get().([]matchNodeCounted)[:0]
	// This is the slice we'll iterate over. We also grab this from the pool.
	matchSubset := matchNodeCountedPool.Get().([]matchNodeCounted)[:0]
	matchSubset = append(matchSubset, matchNodeCounted{
		MatchNode: *mn,
		Count:     0,
	})

	// Loop over the entire set of sort orders
	for _, sortOrder := range allSortOrders {
		for _, node := range matchSubset {
			if len(node.SortOrders) == 0 {
				// At most we'll look at three divergences that may match, we can ignore all other divergences
				if divergence, ok := node.Divergences[singleMatch]; ok {
					matches = processMatch(matches, matchNodeCounted{
						MatchNode: *divergence,
						Count:     node.Count,
					}, sortOrder)
				}
				if divergence, ok := node.Divergences[anyMatch]; ok {
					matches = processMatch(matches, matchNodeCounted{
						MatchNode: *divergence,
						Count:     node.Count,
					}, sortOrder)
				}
				if divergence, ok := node.Divergences[sortOrder]; ok {
					matches = processMatch(matches, matchNodeCounted{
						MatchNode: *divergence,
						Count:     node.Count,
					}, sortOrder)
				}
				continue
			}
			matches = processMatch(matches, node, sortOrder)
		}
		// Swap the two, and put the slice of matches to be at the beginning of the previous subset array to reuse it
		matches, matchSubset = matchSubset[:0], matches
	}
	// We're done with the matches slice, so put it back in the pool
	matchNodeCountedPool.Put(matches)

	// The subset may contain partial matches (which do not count), so we filter for only complete matches
	results := make([]MatchResult, 0, len(matchSubset))
	for _, node := range matchSubset {
		if node.Data != nil {
			if len(node.SortOrders) == 0 {
				results = append(results, MatchResult{
					MatchNodeData: *node.Data,
					Count:         node.Count,
				})
			} else if len(node.SortOrders) == 1 && node.SortOrders[0] == anyMatch {
				results = append(results, MatchResult{
					MatchNodeData: *node.Data,
					Count:         node.Count + 1,
				})
			}
		}
	}
	// Now we're done with the subset slice, so put it back in the pool
	matchNodeCountedPool.Put(matchSubset)
	return results
}

// processMatch handles the behavior of how to process a sort order against a node. Returns a new slice with any newly
// appended nodes (which should overwrite the first parameter in the calling function).
func processMatch(matches []matchNodeCounted, node matchNodeCounted, sortOrder int32) []matchNodeCounted {
	switch node.SortOrders[0] {
	case singleMatch:
		if sortOrder < singleMatch {
			return matches
		}
		node.SortOrders = node.SortOrders[1:]
		node.Count += 1
		matches = append(matches, node)
	case anyMatch:
		// Since any match can be a zero-length match, we need to check if we also match the next sort order
		if len(node.SortOrders) > 1 && node.SortOrders[1] == sortOrder {
			matches = append(matches, matchNodeCounted{
				MatchNode: MatchNode{
					SortOrders:  node.SortOrders[2:],
					Divergences: node.Divergences,
					Data:        node.Data,
				},
				Count: node.Count + 2,
			})
		}
		// Any match cannot match a separator as they represent column boundaries
		if sortOrder != separator {
			matches = append(matches, node)
		}
	default:
		// NOTE: it's worth mentioning that separators only match with themselves, so no need for special logic
		if sortOrder == node.SortOrders[0] {
			node.SortOrders = node.SortOrders[1:]
			node.Count += 1
			matches = append(matches, node)
		}
	}
	return matches
}

// Add will add the given expressions to the node hierarchy. If the expressions already exists, then this overwrites
// the pre-existing entry. Assumes that the given expressions have already been folded.
func (mn *MatchNode) Add(databaseExpr, branchExpr, userExpr, hostExpr string, data MatchNodeData) {
	root := mn
	allSortOrders := mn.parseExpression(databaseExpr, branchExpr, userExpr, hostExpr)
	defer func() {
		concatenatedSortOrderPool.Put(allSortOrders)
	}()

	remainingRootSortOrders := root.SortOrders
	allSortOrdersMaxIndex := len(allSortOrders) - 1
ParentLoop:
	for i, sortOrder := range allSortOrders {
		if remainingRootSortOrders[0] == sortOrder {
			if len(remainingRootSortOrders) > 1 && i < allSortOrdersMaxIndex {
				// There are more sort orders on both sides, so we simply continue
				remainingRootSortOrders = remainingRootSortOrders[1:]
				continue
			} else if len(remainingRootSortOrders) > 1 && i == allSortOrdersMaxIndex {
				// We have more sort orders on the root, but no more in our expressions, so we put the remaining root
				// sort orders as a divergence and set this as a destination node
				root.Divergences = map[int32]*MatchNode{remainingRootSortOrders[1]: {
					SortOrders:  remainingRootSortOrders[1:],
					Divergences: root.Divergences,
					Data:        root.Data,
				}}
				root.SortOrders = root.SortOrders[:len(root.SortOrders)-len(remainingRootSortOrders)+1]
				root.Data = &data
				break
			} else if len(remainingRootSortOrders) == 1 && i < allSortOrdersMaxIndex {
				// We've run out of sort orders on the root, but still have more from divergences, so check if there's a
				// matching divergence
				nextSortOrder := allSortOrders[i+1]
				if divergence, ok := root.Divergences[nextSortOrder]; ok {
					remainingRootSortOrders = divergence.SortOrders
					root = root.Divergences[nextSortOrder]
					continue ParentLoop
				}
				// None of the divergences matched, so we create a new one and add it. As we're using a pool, we need to
				// create a new slice.
				originalSortOrders := allSortOrders[i+1:]
				newSortOrders := make([]int32, len(originalSortOrders))
				copy(newSortOrders, originalSortOrders)
				root.Divergences[newSortOrders[0]] = &MatchNode{
					SortOrders:  newSortOrders,
					Divergences: nil,
					Data:        &data,
				}
				break
			} else {
				// We have no more sort orders on either side so this is an exact match, therefore we update the data
				root.Data = &data
				break
			}
		} else {
			// Since the sort orders do not match, we create a divergence here with the remaining expressions' sort
			// orders, and move the root's remaining sort orders to its own divergence.
			splitRoot := &MatchNode{
				SortOrders:  remainingRootSortOrders,
				Divergences: root.Divergences,
				Data:        root.Data,
			}
			// As we're using a pool, we need to create a new slice
			originalSortOrders := allSortOrders[i:]
			newSortOrders := make([]int32, len(originalSortOrders))
			copy(newSortOrders, originalSortOrders)
			newDivergence := &MatchNode{
				SortOrders:  newSortOrders,
				Divergences: nil,
				Data:        &data,
			}
			root.SortOrders = root.SortOrders[:len(root.SortOrders)-len(remainingRootSortOrders)]
			root.Divergences = map[int32]*MatchNode{splitRoot.SortOrders[0]: splitRoot, newDivergence.SortOrders[0]: newDivergence}
			// As the root's data is now in the split, we set the data here to nil as it's no longer a destination node.
			// If it wasn't a destination node, then nothing changes (we just set the split's data to nil as well).
			root.Data = nil
			break
		}
	}
}

// Remove will remove the given expressions to the node hierarchy. If the expressions do not exist, then nothing
// happens. Assumes that the given expressions have already been folded.
func (mn *MatchNode) Remove(databaseExpr, branchExpr, userExpr, hostExpr string) uint32 {
	root := mn
	allSortOrders := mn.parseExpression(databaseExpr, branchExpr, userExpr, hostExpr)
	defer func() {
		concatenatedSortOrderPool.Put(allSortOrders)
	}()

	// We track the parent of the root node so that we can delete its divergence if applicable
	var rootParent *MatchNode = nil
	childIndex := int32(0)

	remainingRootSortOrders := root.SortOrders
	allSortOrdersMaxIndex := len(allSortOrders) - 1
	removedIndex := uint32(math.MaxUint32)
ParentLoop:
	for i, sortOrder := range allSortOrders {
		if remainingRootSortOrders[0] == sortOrder {
			if len(remainingRootSortOrders) > 1 && i < allSortOrdersMaxIndex {
				// There are more sort orders on both sides, so we simply continue
				remainingRootSortOrders = remainingRootSortOrders[1:]
				continue
			} else if len(remainingRootSortOrders) > 1 && i == allSortOrdersMaxIndex {
				// We have more sort orders on the root, but no more in our expressions, so this set of expressions
				// don't have a match
				break
			} else if len(remainingRootSortOrders) == 1 && i < allSortOrdersMaxIndex {
				// We've run out of sort orders on the root, but still have more from the expressions, so check if a
				// divergence will match the next sort order from the expressions
				nextSortOrder := allSortOrders[i+1]
				if divergence, ok := root.Divergences[nextSortOrder]; ok {
					remainingRootSortOrders = divergence.SortOrders
					rootParent = root
					childIndex = nextSortOrder
					root = divergence
					continue ParentLoop
				}
				// None of the divergences matched, so this set of expressions don't have a match
				break
			} else {
				// We have no more sort orders on either side so this is an exact match.
				// If it's a destination node, then we mark it as no longer being one.
				if root.Data != nil {
					removedIndex = root.Data.RowIndex
				}
				root.Data = nil
				if len(root.Divergences) == 1 {
					// Since there is only a single divergence, we merge it with this node
					for _, divergence := range root.Divergences {
						// The fact that you gotta do a range + break to get a single map element is silly
						root.SortOrders = append(root.SortOrders, divergence.SortOrders...)
						root.Data = divergence.Data
						root.Divergences = nil
						break
					}
				} else if len(root.Divergences) == 0 && rootParent != nil {
					// With no divergences, we can remove this node from the parent
					delete(rootParent.Divergences, childIndex)
					// If the parent only has a single divergence, and it's not a destination node, we can merge that
					// divergence with the parent
					if len(rootParent.Divergences) == 1 && rootParent.Data == nil {
						// Since there is only a single divergence, we merge it with this node
						for _, divergence := range rootParent.Divergences {
							// It was silly a few lines ago, and it's still silly here
							rootParent.SortOrders = append(rootParent.SortOrders, divergence.SortOrders...)
							rootParent.Data = divergence.Data
							rootParent.Divergences = nil
						}
					}
				}
				// If this node has multiple divergences then we have nothing more to do
				break
			}
		} else {
			// Since the sort orders do not match, that means that this set of expressions don't have a match
			break
		}
	}
	return removedIndex
}

// parseExpression parses expressions into a concatenated collection of sort orders. The returned slice belongs to the
// pool, which, if possible, should be returned once it is no longer needed. As this function doesn't distinguish
// between strings and expressions, it assumes any given expressions have already been folded.
func (mn *MatchNode) parseExpression(database, branch, user, host string) []int32 {
	if len(database) > math.MaxUint16 {
		database = database[:math.MaxUint16]
	}
	if len(branch) > math.MaxUint16 {
		branch = branch[:math.MaxUint16]
	}
	if len(user) > math.MaxUint16 {
		user = user[:math.MaxUint16]
	}
	if len(host) > math.MaxUint16 {
		host = host[:math.MaxUint16]
	}

	allSortOrders := concatenatedSortOrderPool.Get().([]int32)[:0]
	for i, str := range []string{database, branch, user, host} {
		escaped := false
		sortFunc := sortFuncs[i]
		allSortOrders = append(allSortOrders, separator)
		for _, r := range str {
			if escaped {
				escaped = false
				allSortOrders = append(allSortOrders, sortFunc(r))
			} else {
				switch r {
				case '\\':
					escaped = true
				case '%':
					allSortOrders = append(allSortOrders, anyMatch)
				case '_':
					allSortOrders = append(allSortOrders, singleMatch)
				default:
					allSortOrders = append(allSortOrders, sortFunc(r))
				}
			}
		}
	}
	return allSortOrders
}
