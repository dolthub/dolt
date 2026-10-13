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

package commitgraph

import (
	"container/heap"
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/store/hash"
)

// GhostCommitError is returned when a walk reaches a ghost commit, whose parents are unknown.
type GhostCommitError struct {
	Hash hash.Hash
}

func (e *GhostCommitError) Error() string {
	return fmt.Sprintf("ghost commit encountered: %s", e.Hash)
}

// AncestryIndex is implemented by a HashResolver that can tell whether a commit is an ancestor of another without
// walking the history between them.
type AncestryIndex interface {
	// IsAncestor reports whether |ancestor| is |descendant| or one of its ancestors.
	IsAncestor(ctx context.Context, ancestor *CommitInfo, descendant hash.Hash) (bool, error)
	// GhostHeight returns the height of |ghost|, a parent of |child|.
	GhostHeight(ctx context.Context, ghost hash.Hash, child *CommitInfo) (uint64, error)
}

const (
	reachableFromLeft uint8 = 1 << iota
	reachableFromRight
	ancestorOfMergeBase
)

// GetMergeBases returns every best common ancestor of |left| and |right|: each common ancestor that is not an ancestor
// of another common ancestor, the same set as `git merge-base --all`. The result is empty when there is no common
// ancestor. |left| is resolved through |leftResolver| and |right| through |rightResolver|; every other commit is
// resolved through the resolver of the commit it was first reached from.
//
// When a side's resolver is an AncestryIndex, only the other side is walked, and not below its common commits; if both
// are, the side with the shorter tip is walked. Otherwise both sides are walked. A GhostCommitError is returned when
// the walk reaches a ghost commit whose unknown parents could change the result.
func GetMergeBases(ctx context.Context, leftResolver HashResolver, left hash.Hash, rightResolver HashResolver, right hash.Hash) ([]hash.Hash, error) {
	q := mergeBaseQueue{newPQ()}
	leftTip, err := q.get(ctx, leftResolver, left)
	if err != nil {
		return nil, err
	}
	rightTip, err := q.get(ctx, rightResolver, right)
	if err != nil {
		return nil, err
	}
	for _, tip := range []*entry{leftTip, rightTip} {
		if tip.info.IsGhost {
			return nil, &GhostCommitError{Hash: tip.info.Hash}
		}
	}

	leftIndex, leftIndexed := leftResolver.(AncestryIndex)
	rightIndex, rightIndexed := rightResolver.(AncestryIndex)
	if leftIndexed && (!rightIndexed || rightTip.info.Height <= leftTip.info.Height) {
		return walkOneSide(ctx, q, rightTip, leftIndex, left)
	} else if rightIndexed {
		return walkOneSide(ctx, q, leftTip, rightIndex, right)
	}
	return walkBothSides(ctx, q, leftTip, rightTip)
}

// walkBothSides walks down from |leftTip| and |rightTip|, flagging the commits reachable from each. Any ghost commit
// it reaches is an error.
func walkBothSides(ctx context.Context, q mergeBaseQueue, leftTip, rightTip *entry) ([]hash.Hash, error) {
	unresolved := 0
	markReachable := func(r HashResolver, h hash.Hash, flags uint8) error {
		e, err := q.get(ctx, r, h)
		if err != nil {
			return err
		}
		if e.info.IsGhost {
			return &GhostCommitError{Hash: h}
		}
		prev := e.flags
		e.flags |= flags
		if !e.queued {
			e.queued = true
			heap.Push(q, e)
			if flags&ancestorOfMergeBase == 0 {
				unresolved++
			}
		} else if prev&ancestorOfMergeBase == 0 && flags&ancestorOfMergeBase != 0 {
			unresolved--
		}
		return nil
	}

	if err := markReachable(leftTip.resolver, leftTip.info.Hash, reachableFromLeft); err != nil {
		return nil, err
	}
	if err := markReachable(rightTip.resolver, rightTip.info.Hash, reachableFromRight); err != nil {
		return nil, err
	}
	var mergeBases []hash.Hash
	for unresolved > 0 {
		e := heap.Pop(q).(*entry)
		if e.flags&ancestorOfMergeBase == 0 {
			unresolved--
			if e.flags&reachableFromLeft != 0 && e.flags&reachableFromRight != 0 {
				mergeBases = append(mergeBases, e.info.Hash)
				e.flags |= ancestorOfMergeBase
			}
		}
		// Every commit left to visit is an ancestor of a merge base, and so are |e|'s parents. Stopping here keeps the
		// walk from reaching a ghost that cannot change the result.
		if unresolved == 0 && e.flags&ancestorOfMergeBase != 0 {
			break
		}
		for _, p := range e.info.Parents {
			if err := markReachable(e.resolver, p, e.flags); err != nil {
				return nil, err
			}
		}
	}
	return mergeBases, nil
}

// walkOneSide walks down from |tip| until it reaches commits that |index| reports as ancestors of |indexedTip|. Every
// best common ancestor is reached this way: a path to it through another common commit would make it an ancestor of
// that commit. A common commit is a merge base unless it is an ancestor of a merge base already found, which, being
// taller, is found first. A ghost commit is an error unless it is an ancestor of a merge base, because its unknown
// parents could lead to a common commit.
func walkOneSide(ctx context.Context, q mergeBaseQueue, tip *entry, index AncestryIndex, indexedTip hash.Hash) ([]hash.Hash, error) {
	var mergeBases []hash.Hash
	isBelowMergeBase := func(info *CommitInfo) (bool, error) {
		for _, b := range mergeBases {
			if below, err := index.IsAncestor(ctx, info, b); err != nil || below {
				return below, err
			}
		}
		return false, nil
	}

	tip.queued = true
	heap.Push(q, tip)
	for q.Len() > 0 {
		e := heap.Pop(q).(*entry)
		common, err := index.IsAncestor(ctx, e.info, indexedTip)
		if err != nil {
			return nil, err
		}
		if common {
			below, err := isBelowMergeBase(e.info)
			if err != nil {
				return nil, err
			}
			if below {
				continue
			}
			if e.info.IsGhost {
				return nil, &GhostCommitError{Hash: e.info.Hash}
			}
			mergeBases = append(mergeBases, e.info.Hash)
			continue
		}
		if e.info.IsGhost {
			return nil, &GhostCommitError{Hash: e.info.Hash}
		}
		for _, p := range e.info.Parents {
			pe, err := q.get(ctx, e.resolver, p)
			if err != nil {
				return nil, err
			}
			if pe.queued {
				continue
			}
			if pe.info.IsGhost {
				heights, ok := e.resolver.(AncestryIndex)
				if !ok {
					return nil, &GhostCommitError{Hash: p}
				}
				height, err := heights.GhostHeight(ctx, p, e.info)
				if err != nil {
					return nil, err
				}
				pe.info = &CommitInfo{Hash: p, Height: height, IsGhost: true}
			}
			pe.queued = true
			heap.Push(q, pe)
		}
	}
	return mergeBases, nil
}

// mergeBaseQueue pops the tallest commit first. A parent is always shorter than its children, so every flag a commit
// can receive is set before it is popped.
type mergeBaseQueue struct {
	*pq
}

func (q mergeBaseQueue) Less(i, j int) bool {
	ei, ej := q.pending[i], q.pending[j]
	if ei.info.Height != ej.info.Height {
		return ei.info.Height > ej.info.Height
	}
	return ei.info.Hash.Less(ej.info.Hash)
}
