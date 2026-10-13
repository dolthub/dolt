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

	"github.com/dolthub/dolt/go/store/hash"
)

const (
	reachableFromLeft uint8 = 1 << iota
	reachableFromRight
	ancestorOfMergeBase
)

// GetMergeBases returns every best common ancestor of |left| and |right|: each common ancestor that is not an ancestor
// of another common ancestor, the same set as `git merge-base --all`. The result is empty when there is no common
// ancestor. |left| is resolved through |leftResolver| and |right| through |rightResolver|; every other commit is
// resolved through the resolver of the commit it was first reached from. Ghost commits are boundaries.
func GetMergeBases(ctx context.Context, leftResolver HashResolver, left hash.Hash, rightResolver HashResolver, right hash.Hash) ([]hash.Hash, error) {
	q := mergeBaseQueue{newPQ()}
	unresolved := 0
	markReachable := func(r HashResolver, h hash.Hash, flags uint8) error {
		e, err := q.get(ctx, r, h)
		if err != nil {
			return err
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

	if err := markReachable(leftResolver, left, reachableFromLeft); err != nil {
		return nil, err
	}
	if err := markReachable(rightResolver, right, reachableFromRight); err != nil {
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
		for _, p := range e.info.Parents {
			if err := markReachable(e.resolver, p, e.flags); err != nil {
				return nil, err
			}
		}
	}
	return mergeBases, nil
}

// mergeBaseQueue pops the tallest commit first. A parent is always shorter than its children, so every flag a commit
// can receive is set before it is popped. Ghosts have no height and are popped last, after all of their children.
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
