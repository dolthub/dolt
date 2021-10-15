// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package prolly

import (
	"context"

	"github.com/dolthub/dolt/go/store/d"
)

// nodeCursor explores a tree of node items.
type nodeCursor struct {
	nd  node
	idx int

	nrw    NodeReadWriter
	parent *nodeCursor
}

func newNodeCursor(nd node, idx int, nrw NodeReadWriter, parent *nodeCursor) *nodeCursor {
	if idx >= nd.count() || idx < 0 {
		panic("node cursor out of bounds")
	}
	return &nodeCursor{parent: parent, nd: nd, idx: idx}
}

func (cur *nodeCursor) valid() bool {
	return cur.idx >= 0 && cur.idx < cur.nd.count()
}

// current returns the item at the current cursor position
func (cur *nodeCursor) current() nodeItem {
	d.PanicIfFalse(cur.valid())
	return cur.nd.getItem(cur.idx)
}

func (cur *nodeCursor) skipToNodeStart() {
	cur.idx = 0
}

func (cur *nodeCursor) skipToNodeEnd() {
	cur.idx = cur.lastIdx()
}

func (cur *nodeCursor) atNodeStart() bool {
	return cur.idx == 0
}

func (cur *nodeCursor) atNodeEnd() bool {
	return cur.idx == cur.lastIdx()
}

func (cur *nodeCursor) lastIdx() int {
	return cur.nd.count() - 1
}

func (cur *nodeCursor) compare(other *nodeCursor) int {
	if cur.parent != nil {
		p := cur.parent.compare(other.parent)
		if p != 0 {
			return p
		}
	}
	d.PanicIfFalse(cur.nd.count() == other.nd.count())
	return cur.idx - other.idx
}

func (cur *nodeCursor) advance(ctx context.Context) (bool, error) {
	return cur.advanceMaybeAllowPastEnd(ctx, true)
}

func (cur *nodeCursor) advanceMaybeAllowPastEnd(ctx context.Context, allowPastEnd bool) (bool, error) {
	if cur.idx < cur.nd.count()-1 {
		cur.idx++
		return true, nil
	}

	if cur.idx == cur.nd.count() {
		return false, nil
	}

	if cur.parent != nil {
		ok, err := cur.parent.advanceMaybeAllowPastEnd(ctx, false)

		if err != nil {
			return false, err
		}

		if ok {
			// at end of current leaf chunk and there are more
			err := cur.fetchNode(ctx)
			if err != nil {
				return false, err
			}

			cur.skipToNodeStart()
			return true, nil
		}
	}

	if allowPastEnd {
		cur.idx++
	}

	return false, nil
}

func (cur *nodeCursor) retreat(ctx context.Context) (bool, error) {
	return cur.retreatMaybeAllowBeforeStart(ctx, true)
}

func (cur *nodeCursor) retreatMaybeAllowBeforeStart(ctx context.Context, allowBeforeStart bool) (bool, error) {
	if cur.idx > 0 {
		cur.idx--
		return true, nil
	}

	if cur.idx == -1 {
		return false, nil
	}

	d.PanicIfFalse(0 == cur.idx)

	if cur.parent != nil {
		ok, err := cur.parent.retreatMaybeAllowBeforeStart(ctx, false)

		if err != nil {
			return false, err
		}

		if ok {
			err := cur.fetchNode(ctx)
			if err != nil {
				return false, err
			}

			cur.skipToNodeEnd()
			return true, nil
		}
	}

	if allowBeforeStart {
		cur.idx--
	}

	return false, nil
}

// todo(andy): refactor advance/retreat
//func (cur *nodeCursor) advance(ctx context.Context) (bool, error) {
//	if cur.advanceInNode() {
//		return true, nil
//	}
//
//	if cur.parent == nil {
//		return false, nil
//	}
//
//	ok, err := cur.parent.advance(ctx)
//	if !ok || err != nil {
//		return ok, err
//	}
//
//	err = cur.fetchNode(ctx)
//	if err != nil {
//		return false, err
//	}
//	cur.skipToNodeStart()
//
//	return true, nil
//}
//
//func (cur *nodeCursor) advanceInNode() bool {
//	if cur.atNodeEnd() {
//		return false
//	}
//	cur.idx++
//	return true
//}
//
//func (cur *nodeCursor) retreat(ctx context.Context) (bool, error) {
//	if cur.retreatInNode() {
//		return true, nil
//	}
//
//	if cur.parent == nil {
//		return false, nil
//	}
//
//	ok, err := cur.parent.retreat(ctx)
//	if !ok || err != nil {
//		return ok, err
//	}
//
//	err = cur.fetchNode(ctx)
//	if err != nil {
//		return false, err
//	}
//	cur.skipToNodeEnd()
//
//	return true, nil
//}
//
//func (cur *nodeCursor) retreatInNode() bool {
//	if cur.atNodeStart() {
//		return false
//	}
//	cur.idx--
//	return true
//}

// fetchNode loads the node that the cursor index points to.
// It's called whenever the cursor advances/retreats to a different chunk.
func (cur *nodeCursor) fetchNode(ctx context.Context) (err error) {
	d.PanicIfFalse(cur.parent != nil)
	cur.nd, err = fetchRef(ctx, cur.nrw, cur.parent.current())
	cur.idx = -1 // caller must set
	return err
}
